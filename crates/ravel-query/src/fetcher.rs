//! SegmentFetcher: footer-first suffix reads, identity verification, matcher
//! pruning, and coalesced byte-range page fetches over one segment
//! (docs/query-engine.md "Flow", docs/segment-format.md reader protocol).

use bytes::Bytes;
use futures::future::join_all;
use ravel_cache::{Cache, CacheKey, SingleFlightError};
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_object_store::{Etag, GetOutcome, GetRange, ObjectStoreBackend, StoreError, Version};
use ravel_promql::{LabelMatcher, matches_series};
use ravel_segment::{
    ExpectedIdentity, Footer, FooterOutcome, HistogramValue, ReaderLimits, RunEntry, SeriesEntry,
    SeriesEntryV4, ValPageKind, ValueKind, check_identity, decode_catalog_v4, decode_catalog_v5,
    decode_catalog_v5_chunked, decode_run_histogram_pages, decode_run_pages_soa, open_from_suffix,
    plan_ranges_v4,
};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{LabelSet, Sample, SeriesId, TenantHash};
use tracing::Instrument;

/// Section kinds from docs/segment-format.md (not exported by
/// `ravel-segment`). LABEL_DICT + SERIES_IDS + SERIES_META are the catalog
/// sections a below-threshold v5 object carries; SERIES_META's absence (it is
/// replaced by SERIES_IDX + SERIES_META_CHUNKS) marks a sparse object at or
/// above the 4096-series threshold.
const SECTION_LABEL_DICT: u32 = 1;
const SECTION_SERIES_IDS: u32 = 5;
const SECTION_SERIES_META: u32 = 6;
const SECTION_SERIES_IDX: u32 = 8;
const SECTION_SERIES_META_CHUNKS: u32 = 9;

/// Store-sourced GET cost, either of a single `guarded_get` call or accumulated
/// across the coalesced GETs of one `ensure_ranges` call. It scopes ADR-0044
/// decision 5's per-span `s3_requests`/`s3_bytes` to bytes that actually
/// crossed the network, so the `segment_open`/`page_fetch` spans never count
/// cache-served bytes as S3 traffic.
///
/// `guarded_get` routes cache-eligible ranges through `cached_get`, which on a
/// hit returns bytes with no store round trip at all
/// (`accounting.record_cache_hit`, never an `AccountedOp::Get`). A cache hit
/// therefore contributes `{0, 0}` here. A store GET -- the uncached path, a
/// cache miss's leader, or a single-flight follower riding another caller's
/// in-flight GET -- contributes `{1, bytes_len}`, matching `log_fetcher.rs`'s
/// `fetch_accounted_with_tenant`: a follower still attributes one logical GET,
/// which bounds this call's own attribution and never under-counts the query
/// total, which is what the span is for.
///
/// A local per-call value is used rather than a before/after `QueryAccounting`
/// delta because `engine.rs` runs one segment future per `buffer_unordered`
/// slot against the same accounting handle, so between one phase's before- and
/// after-snapshot a sibling segment's GETs would bump the shared counters and
/// the delta would capture their bytes too. `requests` is the number of
/// store-sourced GETs (cache hits excluded); `bytes` is the sum of the bytes
/// those store GETs returned.
#[derive(Default, Clone, Copy)]
struct GetCost {
    requests: u64,
    bytes: u64,
}

/// Absolute `(offset, len)` of a section by kind, from the footer.
fn section_range(footer: &Footer, kind: u32) -> Option<(u64, u64)> {
    footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .map(|s| (s.offset, s.len))
}

/// Default suffix length fetched on the first GET of a segment object.
pub const DEFAULT_SUFFIX_LEN: u64 = 64 * 1024;
/// Default maximum gap between two planned byte ranges that still get
/// coalesced into a single GET.
pub const DEFAULT_COALESCE_GAP: u64 = 64 * 1024;
/// Default size at or below which the first GET fetches the whole object in
/// one request instead of a footer suffix. The commit record
/// carries the exact object size, so a small object is read whole up front:
/// its footer, catalog sections, and page bytes then all resolve from that
/// one buffer with no second probe. Above the threshold the footer-suffix
/// path is kept, which reads only the tail and coalesces the page GETs it
/// actually needs. 512 KiB comfortably covers a typical ~30 KiB L0 flush and
/// the small end of the L1 part distribution while never whole-object-reading
/// a large compacted part just to reach its footer.
pub const DEFAULT_WHOLE_OBJECT_THRESHOLD: u64 = 512 * 1024;
/// Default bound on the number of byte-range GETs a single segment fetch
/// keeps in flight at once. The fetcher clones share one
/// semaphore, so this also bounds the total in-flight GETs across every
/// concurrent segment fetch in a query, not just within one segment.
pub const DEFAULT_MAX_CONCURRENT_GETS: usize = 16;

/// Object-size floor for taking the sparse catalog-probe path in
/// [`SegmentFetcher::decode_selected`] instead of the whole-object fallback.
/// A sparse (>=4096-series) v5 object lays its catalog sections
/// (LABEL_DICT, SERIES_IDS, SERIES_META_CHUNKS, SERIES_IDX) contiguously at the
/// front, ahead of the TS/VAL/HIST page sections. Fetching only that catalog
/// prefix -- one contiguous range GET -- skips the page bytes a whole-object
/// GET pulls in, then the matched series' pages are fetched selectively
/// afterward, exactly as the below-threshold path already does.
///
/// The crossover this constant guards is a round-trip-vs-bytes tradeoff, not a
/// pure win: below the floor the whole object is small enough that a single GET
/// beats one catalog GET plus the extra selective page-range GETs (each a fresh
/// round trip: ~15-80 ms on S3, ~1-5 ms on loopback MinIO). Those
/// measurements are per-request latency and do NOT meter this specific
/// within-segment crossover, so this floor is
/// set conservatively rather than fit to a measured point (see
/// docs/query-engine.md). 256 KiB is above the four fixed 64 KiB suffix/gap
/// probes yet far below any real compacted sparse L1 part, so production sparse
/// objects take the probe path while a degenerate tiny sparse object keeps the
/// cheaper single GET.
pub const SPARSE_PROBE_MIN_OBJECT_SIZE: u64 = 256 * 1024;

/// Errors fetching and decoding one segment. Every variant is a hard error:
/// the caller never receives partial or silently-wrong data for a segment
/// that failed to fetch or decode.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("object store error reading segment {key}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("corrupt segment {key}: {source}")]
    Corrupt {
        key: String,
        #[source]
        source: ravel_segment::SegmentError,
    },
    #[error("etag changed between reads of segment {key}: store returned inconsistent data")]
    EtagChanged { key: String },
}

/// The error channel for a cache-routed GET's `get_or_fetch` closure
/// (`cached_get`, below). A store failure and an etag change on a cache
/// miss are both real failures but mean different things downstream: a
/// store error is opaque backend trouble, while an etag change means the
/// live GET caught the object being replaced mid-open, which is a
/// snapshot-invalidation signal (ADR-0046 decision 4's amendment) and must
/// surface as `FetchError::EtagChanged`, not get folded into `Store`.
/// `Cache<E>`/`SingleFlightError<E>` require `E: Clone`, hence the `Arc`
/// around `StoreError`, same as the pre-existing single-variant channel.
#[derive(Debug, Clone)]
pub enum CacheFetchError {
    Store(std::sync::Arc<StoreError>),
    EtagChanged { key: String },
}

/// Lets a `get_or_fetch` closure use `?` directly on a `store.get(..)` call
/// (as `log_fetcher.rs`'s does) without an explicit `.map_err`, the same way
/// the blanket `From<T> for Arc<T>` let the pre-existing `Cache<Arc<StoreError>>`
/// channel do.
impl From<StoreError> for CacheFetchError {
    fn from(err: StoreError) -> Self {
        CacheFetchError::Store(std::sync::Arc::new(err))
    }
}

/// The full four-element dedup key of ONE sample (docs/catalog-and-mvcc.md,
/// ADR-0010 §5): `(created_unix_ns, writer_epoch, writer_seq, in_page_index)`,
/// greatest wins.
///
/// A run whose samples all came from the same write shares the first three
/// elements and derives the fourth from array position, which is what every
/// object in existence carries and what the run-wide provenance fields on
/// [`FetchedSeries`]/[`FetchedSeriesSoa`]/[`FetchedHistogramSeries`] express.
/// This type exists for the shape those fields cannot express: a run that
/// merged several inputs' samples, where each sample keeps the provenance of
/// the write it came from and array position no longer reconstructs the fourth
/// element (ADR-0092 "Why 11.46 is not the target", decision 1). Nothing
/// produces that shape yet; issue #315 makes L1 compaction emit it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SamplePriority {
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    pub in_page_index: u32,
}

impl SamplePriority {
    /// The key as the plain tuple the merge's comparison uses. Field order
    /// here IS the comparison order, so the tuple is a reordering-free
    /// projection.
    pub fn as_tuple(&self) -> (i64, u64, u64, u32) {
        (
            self.created_unix_ns,
            self.writer_epoch,
            self.writer_seq,
            self.in_page_index,
        )
    }
}

/// One matched series' decoded samples plus the provenance fields needed for
/// cross-segment duplicate-sample resolution (docs/catalog-and-mvcc.md).
#[derive(Debug, Clone)]
pub struct FetchedSeries {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    /// On-disk order, including any duplicate timestamps within this
    /// segment; index in this vec is the "in-page index" tiebreak.
    pub samples: Vec<Sample>,
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    /// Per-sample dedup keys, parallel to `samples`, for a run that merged
    /// several writes' samples. `None` (the default and, today, the only shape
    /// any object produces) means the run-wide fields above plus array
    /// position give every sample's key. When `Some`, its length must equal
    /// `samples.len()`; the merge rejects a disagreement as
    /// `QueryError::PrioritySampleCountMismatch` rather than truncating.
    pub per_sample_priorities: Option<Vec<SamplePriority>>,
}

/// SoA counterpart to `FetchedSeries`: timestamps and values as separate
/// vecs, ready for zero-copy Arrow
/// buffer adoption in `ravel-sql`. Same provenance fields, same
/// per-segment on-disk order and in-page-index tiebreak (index into
/// `timestamps`/`values`) as `FetchedSeries`.
#[derive(Debug, Clone)]
pub struct FetchedSeriesSoa {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    /// Per-sample dedup keys, parallel to `timestamps`/`values`; see
    /// [`FetchedSeries::per_sample_priorities`]. `None` for every run the
    /// fetcher emits today.
    pub per_sample_priorities: Option<Vec<SamplePriority>>,
}

/// Histogram counterpart to `FetchedSeriesSoa`: the decoded
/// native-histogram samples of one matched histogram-kind series, as parallel
/// timestamp/value vecs, with the same provenance fields and per-segment
/// on-disk order (index into `timestamps`/`values` is the in-page-index
/// tiebreak) the scalar SoA carries. `values` holds `ravel_segment`'s storage
/// histogram model; the read path converts each to the evaluator's float model
/// downstream (docs/query-engine.md, `merge_histogram_soa_runs`).
#[derive(Debug, Clone)]
pub struct FetchedHistogramSeries {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub timestamps: Vec<i64>,
    pub values: Vec<HistogramValue>,
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    /// Per-sample dedup keys, parallel to `timestamps`/`values`; see
    /// [`FetchedSeries::per_sample_priorities`]. `None` for every run the
    /// fetcher emits today.
    pub per_sample_priorities: Option<Vec<SamplePriority>>,
}

/// Page-kind counters accumulated over one `fetch_soa` call, for downstream
/// consumers to read. Currently tracks VAL_RAW_F64 pages only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FetchStats {
    pub raw_f64_pages: u64,
    pub raw_f64_bytes: u64,
}

impl FetchStats {
    fn record_val_page(&mut self, kind: ValPageKind, bytes: usize) {
        if kind == ValPageKind::RawF64 {
            self.raw_f64_pages += 1;
            self.raw_f64_bytes += bytes as u64;
        }
    }
}

/// Byte ranges already fetched from a segment object, keyed by absolute
/// offset, so later planned ranges that fall inside an already-fetched
/// buffer (typically the initial suffix, for small segments) need no
/// additional GET.
#[derive(Default)]
struct FetchedRegions {
    buffers: Vec<(u64, Bytes)>,
}

impl FetchedRegions {
    fn insert(&mut self, start: u64, bytes: Bytes) {
        self.buffers.push((start, bytes));
    }

    fn covers(&self, start: u64, end: u64) -> bool {
        self.buffers
            .iter()
            .any(|(s, b)| *s <= start && end <= s.saturating_add(b.len() as u64))
    }

    fn slice(&self, offset: u64, len: u64) -> Option<Bytes> {
        let end = offset.checked_add(len)?;
        self.buffers.iter().find_map(|(s, b)| {
            if *s <= offset && end <= s.saturating_add(b.len() as u64) {
                let start_rel = usize::try_from(offset - s).ok()?;
                let end_rel = usize::try_from(end - s).ok()?;
                // Refcounted slice of the already-fetched buffer, not a
                // copy:
                // `b` is `Bytes`, so `slice` shares the backing allocation.
                // `end_rel <= b.len()` holds by the range check above.
                Some(b.slice(start_rel..end_rel))
            } else {
                None
            }
        })
    }
}

/// The four sparse-catalog section ranges (`(offset, len)` each) the
/// catalog-probe path fetches instead of the whole object: LABEL_DICT,
/// SERIES_IDS, SERIES_IDX, and SERIES_META_CHUNKS. They lie contiguously at the
/// object front (ahead of the TS/VAL/HIST page sections), so `ensure_ranges`
/// coalesces them into as few GETs as their layout allows.
struct SparseCatalogRanges {
    label_dict: (u64, u64),
    series_ids: (u64, u64),
    series_idx: (u64, u64),
    meta_chunks: (u64, u64),
}

/// Merges (start, end) ranges into ordered, non-overlapping groups, joining
/// consecutive ranges whose gap is at most `max_gap` into a single group
/// (docs/query-engine.md "coalesce adjacent byte ranges").
fn coalesce_ranges(mut ranges: Vec<(u64, u64)>, max_gap: u64) -> Vec<(u64, u64)> {
    ranges.sort_by_key(|r| r.0);
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = out.last_mut()
            && start <= last.1.saturating_add(max_gap)
        {
            last.1 = last.1.max(end);
            continue;
        }
        out.push((start, end));
    }
    out
}

/// Fetches and decodes one segment at a time: suffix-GET the footer, verify
/// identity, prune series by matchers, plan and coalesce page ranges, decode
/// selected pages. See docs/query-engine.md "Flow" for the full contract.
#[derive(Clone)]
pub struct SegmentFetcher {
    store: std::sync::Arc<dyn ObjectStoreBackend>,
    suffix_len: u64,
    coalesce_gap: u64,
    /// Object size at or below which the first GET reads the whole object
    /// instead of a footer suffix.
    whole_object_threshold: u64,
    limits: ReaderLimits,
    /// Bounds the byte-range GETs kept in flight. Shared across `clone`s (it
    /// is an `Arc`), so every concurrent segment fetch in one query draws
    /// from the same permit pool rather than each opening its own unbounded
    /// fan-out. It is only ever held around a single leaf
    /// `store.get`, never across a scope that acquires another permit, so no
    /// fetch can deadlock waiting on permits it already holds.
    get_semaphore: std::sync::Arc<tokio::sync::Semaphore>,
    /// ADR-0046's read cache, consulted by `guarded_get` for every
    /// byte-range (and whole-object) GET. `None` -- the default from
    /// `new` -- reproduces exactly the pre-cache behavior: every GET goes
    /// to the store. Shared across clones, and safe to share across
    /// tenants: the cache key is derived per call from the caller-supplied
    /// `tenant_hash`, never fixed on the fetcher itself.
    cache: Option<std::sync::Arc<Cache<CacheFetchError>>>,
}

impl SegmentFetcher {
    pub fn new(store: std::sync::Arc<dyn ObjectStoreBackend>) -> Self {
        SegmentFetcher {
            store,
            suffix_len: DEFAULT_SUFFIX_LEN,
            coalesce_gap: DEFAULT_COALESCE_GAP,
            whole_object_threshold: DEFAULT_WHOLE_OBJECT_THRESHOLD,
            limits: ReaderLimits::default(),
            get_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_MAX_CONCURRENT_GETS,
            )),
            cache: None,
        }
    }

    /// Wires ADR-0046's read cache into every GET `guarded_get` issues
    /// (decision 1). A `SegmentFetcher` built with plain `new` and never
    /// given a cache behaves exactly as it did before this cache existed.
    #[must_use]
    pub fn with_cache(mut self, cache: std::sync::Arc<Cache<CacheFetchError>>) -> Self {
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

    /// Sets the whole-object fetch threshold. An object whose
    /// commit-record size is at or below `n` is read whole on its first GET;
    /// a larger one keeps the footer-suffix path. `0` disables the
    /// whole-object path entirely (every object takes the suffix path),
    /// which is what the multi-GET suffix tests use to exercise the footer
    /// `NeedRange` chase on a small object.
    #[must_use]
    pub fn with_whole_object_threshold(mut self, n: u64) -> Self {
        self.whole_object_threshold = n;
        self
    }

    /// Sets the in-flight byte-range GET bound. Shared across
    /// this fetcher's clones.
    #[must_use]
    pub fn with_max_concurrent_gets(mut self, n: usize) -> Self {
        self.get_semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(n.max(1)));
        self
    }

    /// One store GET, bounded by the shared in-flight semaphore. The permit
    /// is released the moment the GET resolves; callers must
    /// never hold the returned future's permit across another
    /// `guarded_get`/`ensure_ranges` call, or a query whose in-flight GETs
    /// already fill the pool could wait on itself.
    ///
    /// Only a successful GET is recorded, matching `QueryAccounting`'s
    /// "completed store request" wording; a failed GET propagates its error
    /// unrecorded.
    async fn store_get(
        &self,
        key: &str,
        range: GetRange,
        accounting: &QueryAccounting,
    ) -> Result<GetOutcome, StoreError> {
        let _permit =
            self.get_semaphore.acquire().await.map_err(|_| {
                StoreError::Transient("fetch concurrency semaphore closed".to_string())
            })?;
        let got = self.store.get(key, range).await?;
        accounting.record_s3_request(AccountedOp::Get);
        accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
        Ok(got)
    }

    /// This is the funnel every ranged GET in this file passes through
    /// (ADR-0044 "2. Accounting is recorded at existing funnels only", and
    /// ADR-0046 decision 1). A GET that goes to the store records one
    /// `AccountedOp::Get` request and its transferred bytes (via
    /// `store_get`); a cache hit records a cache hit and its served bytes
    /// instead (ADR-0046 decision 4's test needs the two distinguishable),
    /// and never also records an `AccountedOp::Get`, since no store round
    /// trip happened.
    ///
    /// A `GetRange::Range` or `GetRange::Full` request is cache-eligible
    /// when a cache is configured: the cache key is built here, from the
    /// same `seg_ref`/`tenant_hash`/`range` values driving this call and
    /// nowhere else, so a payload can never be admitted under a key that
    /// does not describe it (ADR-0046 decision 4's amendment). `Full` maps
    /// to the range `(0, seg_ref.object_size)`; the whole object is a valid
    /// sub-range of itself, and its `total_size` is trivially its own
    /// length, unlike `GetRange::Suffix`. A suffix GET always bypasses the
    /// cache: `Cache`'s value is bare `Bytes`, with nowhere to carry the
    /// object's total size a suffix hit would need to fabricate, and total
    /// size is not otherwise recoverable from a suffix's own byte length.
    ///
    /// A cache-routed GET's `expected_etag` check runs, or does not, by
    /// whether the call is a hit or a miss -- these are not the same case.
    /// On a **hit**, no check runs: the check exists to catch two live store
    /// reads of a supposedly immutable object returning different bytes, a
    /// cache entry is addressed by content hash rather than by a live read,
    /// and there is nothing to compare it against. On a **miss**,
    /// `cached_get` performs a real live store GET exactly like the
    /// uncached path, and that GET's etag is checked against
    /// `expected_etag` exactly as it would be uncached: skipping it there
    /// would silently disable the check for bytes that came straight from
    /// the store in this same call, purely because a cache happened to be
    /// attached. See `cached_get` for where that check runs.
    ///
    /// Returns the `GetOutcome` alongside the [`GetCost`] this single call
    /// contributes to its caller's per-span S3 counts: `{1, bytes_len}` when
    /// the bytes came from the store (this uncached path, or a cache
    /// miss/follower inside `cached_get`), `{0, 0}` on a cache hit. The caller
    /// folds that cost in rather than re-deriving hit-vs-miss, so the
    /// store-vs-cache branch lives once, at the seam that already knows the
    /// answer (`store_get` for the store round trip, `cached_get`'s explicit
    /// `cache.get` for the hit check).
    async fn guarded_get(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        range: GetRange,
        expected_etag: Option<&Etag>,
        accounting: &QueryAccounting,
    ) -> Result<(GetOutcome, GetCost), FetchError> {
        let key = seg_ref.data_object_key.as_str();

        let cacheable_range = match range {
            GetRange::Range(start, end) => Some((start, end)),
            GetRange::Full => Some((0, seg_ref.object_size)),
            GetRange::Suffix(_) => None,
        };
        if let (Some((start, end)), Some(cache)) = (cacheable_range, &self.cache) {
            return self
                .cached_get(
                    cache,
                    seg_ref,
                    tenant_hash,
                    range,
                    start,
                    end,
                    expected_etag,
                    accounting,
                )
                .await;
        }

        // No cache, or a suffix GET that always bypasses it: this is an
        // unconditional store round trip, so it always contributes one GET.
        let got = self
            .store_get(key, range, accounting)
            .await
            .map_err(|source| FetchError::Store {
                key: key.to_string(),
                source,
            })?;
        if let Some(expected) = expected_etag
            && &got.etag != expected
        {
            return Err(FetchError::EtagChanged {
                key: key.to_string(),
            });
        }
        let cost = GetCost {
            requests: 1,
            bytes: got.data.len() as u64,
        };
        Ok((got, cost))
    }

    /// The cache-routed half of `guarded_get`. `range` is the exact
    /// `GetRange` the store call uses on a miss (so a `Full` request still
    /// issues one whole-object GET rather than a synthesized
    /// `Range(0, object_size)`); `(start, end)` is `range` restated as the
    /// absolute byte bounds the `CacheKey` and the fabricated hit
    /// `GetOutcome` use.
    ///
    /// Hit/miss is decided by an explicit `cache.get` before touching
    /// `get_or_fetch`, rather than inferring it from whether `get_or_fetch`
    /// ran its fetch closure: a single-flight follower's closure never
    /// runs either, but a follower is not a "hit" in the accounting sense
    /// used here (a store round trip still happened, just not this
    /// caller's own) -- it is what ADR-0046 amendment's documented gap
    /// (see the crate-level test module for the resulting corruption-gate
    /// reach) turns on. Only bytes resident *before* this call asked
    /// count as a hit.
    ///
    /// On a miss, the store GET inside the `get_or_fetch` closure is
    /// checked against `expected_etag` exactly like the uncached path in
    /// `guarded_get` does -- see that function's doc comment for why a hit
    /// skips this and a miss must not. `CacheFetchError` carries the two
    /// outcomes (`Store` vs `EtagChanged`) separately through
    /// `get_or_fetch`'s single error channel so this can still report
    /// `FetchError::EtagChanged` distinctly rather than folding it into a
    /// generic store error.
    #[allow(clippy::too_many_arguments)]
    async fn cached_get(
        &self,
        cache: &std::sync::Arc<Cache<CacheFetchError>>,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        range: GetRange,
        start: u64,
        end: u64,
        expected_etag: Option<&Etag>,
        accounting: &QueryAccounting,
    ) -> Result<(GetOutcome, GetCost), FetchError> {
        let key = seg_ref.data_object_key.as_str();
        let cache_key = CacheKey::new(tenant_hash.0, seg_ref.content_hash, start, end - start);

        if let Some(bytes) = cache.get(&cache_key) {
            accounting.record_cache_hit();
            accounting.add_cache_bytes(bytes.len() as u64);
            // Bytes were resident in the cache before this call asked, so no
            // store round trip happened and this call adds nothing to its
            // span's S3 counts (ADR-0044 decision 5).
            return Ok((
                placeholder_outcome(bytes, seg_ref.object_size),
                GetCost::default(),
            ));
        }
        accounting.record_cache_miss();

        let bytes = cache
            .get_or_fetch(cache_key, || async move {
                let got = self
                    .store_get(key, range, accounting)
                    .await
                    .map_err(|source| CacheFetchError::Store(std::sync::Arc::new(source)))?;
                if let Some(expected) = expected_etag
                    && &got.etag != expected
                {
                    return Err(CacheFetchError::EtagChanged {
                        key: key.to_string(),
                    });
                }
                Ok(got.data)
            })
            .await
            .map_err(|err| match err {
                SingleFlightError::Upstream(CacheFetchError::Store(source)) => FetchError::Store {
                    key: key.to_string(),
                    source: clone_store_error(&source),
                },
                SingleFlightError::Upstream(CacheFetchError::EtagChanged { key }) => {
                    FetchError::EtagChanged { key }
                }
                SingleFlightError::LeaderLost => FetchError::Store {
                    key: key.to_string(),
                    source: StoreError::Transient(
                        "cache single-flight leader lost before producing a result".to_string(),
                    ),
                },
            })?;
        // A miss issued one store GET for these bytes (leader), or rode another
        // caller's in-flight GET (follower). Either way attribute one logical
        // GET, as `log_fetcher.rs`'s `fetch_accounted_with_tenant` does: it does
        // not try to distinguish the rare follower, since recording one GET
        // bounds this call's own attribution and never under-counts the query
        // total. `record_cache_hit` above (the only zero-cost path) is the sole
        // case that came from cache, so this arm always crossed the network from
        // this caller's point of view.
        let cost = GetCost {
            requests: 1,
            bytes: bytes.len() as u64,
        };
        Ok((placeholder_outcome(bytes, seg_ref.object_size), cost))
    }

    async fn ensure_ranges(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        suffix_etag: &Etag,
        needed: &[(u64, u64)],
        regions: &mut FetchedRegions,
        accounting: &QueryAccounting,
    ) -> Result<GetCost, FetchError> {
        let mut reused_bytes: u64 = 0;
        let missing: Vec<(u64, u64)> = needed
            .iter()
            .copied()
            .filter(|(start, end)| {
                if regions.covers(*start, *end) {
                    reused_bytes = reused_bytes.saturating_add(end - start);
                    false
                } else {
                    true
                }
            })
            .collect();
        if reused_bytes > 0 {
            accounting.add_bytes_reused(reused_bytes);
        }
        if missing.is_empty() {
            return Ok(GetCost::default());
        }
        // Fetch the coalesced ranges concurrently rather than one await at a
        // time: a multi-page selection over a large segment
        // used to pay one round trip per coalesced range in series. The
        // shared semaphore inside `guarded_get` bounds the actual in-flight
        // GETs; `join_all` preserves input order, so the resulting
        // `regions` insert order is identical to the old sequential loop.
        let gets = join_all(coalesce_ranges(missing, self.coalesce_gap).into_iter().map(
            |(start, end)| async move {
                let (got, got_cost) = self
                    .guarded_get(
                        seg_ref,
                        tenant_hash,
                        GetRange::Range(start, end),
                        Some(suffix_etag),
                        accounting,
                    )
                    .await?;
                Ok::<(u64, Bytes, GetCost), FetchError>((start, got.data, got_cost))
            },
        ))
        .await;
        // Each coalesced GET reports its own store-vs-cache cost, so a range
        // served from a warm cache adds nothing here while a store GET adds one
        // request and its bytes (ADR-0044 decision 5). Summing the per-call
        // costs never sweeps in a cache hit's served bytes.
        let mut cost = GetCost::default();
        for got in gets {
            let (start, data, got_cost) = got?;
            cost.requests = cost.requests.saturating_add(got_cost.requests);
            cost.bytes = cost.bytes.saturating_add(got_cost.bytes);
            regions.insert(start, data);
        }
        Ok(cost)
    }

    /// Opens a segment: suffix-GET, chase `NeedRange` for the footer if
    /// necessary, and verify identity. Returns the object's `total_size`
    /// alongside the footer; ADR-0027 leaves v5 the only version, so
    /// `open_from_suffix` has already rejected anything else and there is no
    /// per-version dispatch left for callers to do.
    ///
    /// Identity verification is level-aware.
    /// An L0 ref verifies the
    /// footer's writer identity against the commit record (ADR-0010 §7). An
    /// L1 part ref has no writer identity of its own, so the footer's
    /// tenant/shard/ingest_hour/input_set_hash/part_index (and `level == 1`)
    /// are verified against the compaction record's fields the ref carries.
    async fn open_segment(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        accounting: &QueryAccounting,
    ) -> Result<(Footer, u64, Etag, FetchedRegions), FetchError> {
        // Created and recorded on directly, never through
        // `tracing::Span::current()` (ADR-0044 decision 5): this span is
        // debug-level, so at an INFO production level it is disabled, and
        // `current()` would resolve to the nearest *enabled* ancestor -- the
        // request-level span, which declares the same `s3_requests`/
        // `s3_bytes` field names -- silently overwriting that span's
        // whole-request total with just this segment's own cost. Recording on
        // a disabled span handle is a no-op, which is correct.
        let span = tracing::debug_span!(
            "segment_open",
            tenant_hash = %tenant_hash.to_hex(),
            object_size = seg_ref.object_size,
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
        );
        async {
            // This segment's own GET cost, accumulated from the `guarded_get`
            // calls this invocation makes itself rather than diffed from the
            // shared `QueryAccounting`: concurrent sibling segments share that
            // handle, so a delta would fold their GETs into this span (ADR-0044
            // decision 5).
            let mut span_requests: u64 = 0;
            let mut span_bytes: u64 = 0;
            let key = &seg_ref.data_object_key;
            // Size-aware first GET: the commit record already
            // carries the exact object size, so a small object is read whole in
            // one request (its footer, catalog, and pages then all come from that
            // buffer, never a second probe), while a large one keeps the
            // footer-suffix read that touches only the tail. `whole_object_threshold
            // == 0` disables the whole-object path.
            let first_range =
                if seg_ref.object_size != 0 && seg_ref.object_size <= self.whole_object_threshold {
                    GetRange::Full
                } else {
                    GetRange::Suffix(self.suffix_len)
                };
            let (first, first_cost) = self
                .guarded_get(seg_ref, tenant_hash, first_range, None, accounting)
                .await?;
            // Only this GET's store-sourced cost: zero on a warm cache hit (the
            // whole-object `Full` first GET is cache-eligible), one GET otherwise.
            span_requests = span_requests.saturating_add(first_cost.requests);
            span_bytes = span_bytes.saturating_add(first_cost.bytes);
            let total_size = first.total_size;
            let suffix_etag = first.etag.clone();
            let mut regions = FetchedRegions::default();
            let first_start = total_size.saturating_sub(first.data.len() as u64);
            regions.insert(first_start, first.data.clone());

            let footer = match open_from_suffix(&first.data, total_size, self.limits)
                .map_err(|source| corrupt(key, source))?
            {
                FooterOutcome::Ready(loc) => loc.footer,
                FooterOutcome::NeedRange { offset, len } => {
                    let (got, got_cost) = self
                        .guarded_get(
                            seg_ref,
                            tenant_hash,
                            GetRange::Range(offset, offset + len),
                            Some(&suffix_etag),
                            accounting,
                        )
                        .await?;
                    // Same store-vs-cache scoping as the first GET: a warm cache
                    // serves this footer-chase range for zero S3 cost.
                    span_requests = span_requests.saturating_add(got_cost.requests);
                    span_bytes = span_bytes.saturating_add(got_cost.bytes);
                    regions.insert(offset, got.data.clone());
                    match open_from_suffix(&got.data, total_size, self.limits)
                        .map_err(|source| corrupt(key, source))?
                    {
                        FooterOutcome::Ready(loc) => loc.footer,
                        FooterOutcome::NeedRange { .. } => {
                            return Err(corrupt(key, ravel_segment::SegmentError::Truncated));
                        }
                    }
                }
            };

            match &seg_ref.level {
                SegmentLevel::L0 => {
                    let expected = expected_identity(tenant_hash, seg_ref);
                    check_identity(&footer, &expected).map_err(|source| corrupt(key, source))?;
                }
                SegmentLevel::L1 {
                    input_set_hash,
                    part_index,
                } => {
                    verify_l1_identity(&footer, tenant_hash, seg_ref, input_set_hash, *part_index)
                        .map_err(|source| corrupt(key, source))?;
                }
            }
            accounting.add_segments_opened(1);
            // This segment's own GET cost (suffix read plus any footer-chase or
            // whole-object read above), scoped to the span, not the query total.
            span.record("s3_requests", span_requests);
            span.record("s3_bytes", span_bytes);
            Ok((footer, total_size, suffix_etag, regions))
        }
        .instrument(span.clone())
        .await
    }

    /// Decodes the catalog and returns the run-major series matching
    /// `matchers`, fetching only the catalog sections when it can.
    ///
    /// - **Below the sparse threshold** a v5 object carries the whole
    ///   SERIES_META (kind 6): fetch LABEL_DICT + SERIES_IDS + SERIES_META and
    ///   decode the run-major catalog, so a label-pruned read fetches no page
    ///   bytes it will not return.
    /// - **At or above the threshold** (SERIES_META absent) the catalog is the
    ///   chunked SERIES_META_CHUNKS plus the SERIES_IDX directory. When the
    ///   object qualifies for the sparse catalog-probe path, fetch
    ///   just its four catalog sections (LABEL_DICT, SERIES_IDS, SERIES_IDX, and
    ///   SERIES_META_CHUNKS, contiguous at the object front) and decode via
    ///   [`decode_catalog_v5_chunked`], skipping the TS/VAL/HIST page bytes the
    ///   whole-object GET would pull in.
    ///   [`Self::sparse_probe_qualifies`] gives the crossover.
    /// - **Otherwise** (a non-qualifying sparse object) fall back to the
    ///   whole-object decode, unchanged.
    ///
    /// Page bytes are fetched afterwards by the caller from `regions`; the run
    /// page ranges are absolute-in-object on every path, so that step is
    /// identical regardless of which catalog path ran here.
    #[allow(clippy::too_many_arguments)]
    async fn decode_selected(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        footer: &Footer,
        total_size: u64,
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
    ) -> Result<Vec<SeriesEntryV4>, FetchError> {
        // See `open_segment`'s comment: recorded on this handle directly,
        // never through `tracing::Span::current()`.
        let span = tracing::debug_span!(
            "catalog_decode",
            matcher_count = matchers.len(),
            total_size,
            series_matched = tracing::field::Empty,
        );
        async {
            let key = seg_ref.data_object_key.as_str();
            let entries = if let Some((sm_off, sm_len)) = section_range(footer, SECTION_SERIES_META)
            {
                let (ld_off, ld_len) =
                    section_range(footer, SECTION_LABEL_DICT).ok_or_else(|| {
                        corrupt(
                            key,
                            ravel_segment::SegmentError::MissingSection("LABEL_DICT"),
                        )
                    })?;
                let (si_off, si_len) =
                    section_range(footer, SECTION_SERIES_IDS).ok_or_else(|| {
                        corrupt(
                            key,
                            ravel_segment::SegmentError::MissingSection("SERIES_IDS"),
                        )
                    })?;
                self.ensure_ranges(
                    seg_ref,
                    tenant_hash,
                    suffix_etag,
                    &[
                        (ld_off, ld_off + ld_len),
                        (si_off, si_off + si_len),
                        (sm_off, sm_off + sm_len),
                    ],
                    regions,
                    accounting,
                )
                .await?;
                let dict = regions
                    .slice(ld_off, ld_len)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                let ids = regions
                    .slice(si_off, si_len)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                let meta = regions
                    .slice(sm_off, sm_len)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                decode_catalog_v4(footer, &dict, &ids, &meta, self.limits)
                    .map_err(|source| corrupt(key, source))?
            } else if let Some(sparse) = self.sparse_probe_qualifies(footer, total_size, matchers) {
                self.decode_sparse_catalog(
                    seg_ref,
                    tenant_hash,
                    footer,
                    suffix_etag,
                    regions,
                    &sparse,
                    accounting,
                )
                .await?
            } else {
                self.ensure_ranges(
                    seg_ref,
                    tenant_hash,
                    suffix_etag,
                    &[(0, total_size)],
                    regions,
                    accounting,
                )
                .await?;
                let object = regions
                    .slice(0, total_size)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                decode_catalog_v5(footer, &object, self.limits)
                    .map_err(|source| corrupt(key, source))?
            };
            let matched: Vec<SeriesEntryV4> = entries
                .into_iter()
                .filter(|e| matches_series(matchers, &e.entry.labels))
                .collect();
            accounting.add_series_matched(matched.len() as u64);
            // The catalog decode fetches only catalog sections, not page bytes, so
            // series matched is the meaningful count here rather than decompressed
            // page bytes (ADR-0044 decision 5: "record decompressed_bytes if
            // applicable, or series_matched"). Recorded from this call's own count.
            span.record("series_matched", matched.len() as u64);
            Ok(matched)
        }
        .instrument(span.clone())
        .await
    }

    /// Decides whether a sparse (SERIES_META-absent) v5 object takes the
    /// catalog-probe path, returning the four catalog section ranges to fetch
    /// when it does. `None` means fall back to the whole-object decode.
    ///
    /// Qualifies when all hold:
    /// - the object carries both sparse sections (SERIES_IDX +
    ///   SERIES_META_CHUNKS); a malformed footer missing either falls back,
    ///   where `decode_catalog_v5` surfaces the same typed error it always did;
    /// - `matchers` is non-empty. An empty matcher matches every series, so the
    ///   query wants all pages anyway; a single whole-object GET beats one
    ///   catalog GET plus a page GET spanning the whole object;
    /// - the object is at least [`SPARSE_PROBE_MIN_OBJECT_SIZE`], so the page
    ///   bytes skipped outweigh the extra selective page-range round trips.
    fn sparse_probe_qualifies(
        &self,
        footer: &Footer,
        total_size: u64,
        matchers: &[LabelMatcher],
    ) -> Option<SparseCatalogRanges> {
        if matchers.is_empty() || total_size < SPARSE_PROBE_MIN_OBJECT_SIZE {
            return None;
        }
        let (ld_off, ld_len) = section_range(footer, SECTION_LABEL_DICT)?;
        let (si_off, si_len) = section_range(footer, SECTION_SERIES_IDS)?;
        let (idx_off, idx_len) = section_range(footer, SECTION_SERIES_IDX)?;
        let (ch_off, ch_len) = section_range(footer, SECTION_SERIES_META_CHUNKS)?;
        Some(SparseCatalogRanges {
            label_dict: (ld_off, ld_len),
            series_ids: (si_off, si_len),
            series_idx: (idx_off, idx_len),
            meta_chunks: (ch_off, ch_len),
        })
    }

    /// Fetches the four sparse catalog sections named by `ranges` (coalesced
    /// into as few GETs as their layout allows) and decodes the chunked catalog
    /// from them, without the page sections. Each section is crc-verified by
    /// [`decode_catalog_v5_chunked`], so a mis-ranged or corrupt fetch is a
    /// typed error, never wrong data.
    #[allow(clippy::too_many_arguments)]
    async fn decode_sparse_catalog(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        footer: &Footer,
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
        ranges: &SparseCatalogRanges,
        accounting: &QueryAccounting,
    ) -> Result<Vec<SeriesEntryV4>, FetchError> {
        let key = seg_ref.data_object_key.as_str();
        let needed = [
            (
                ranges.label_dict.0,
                ranges.label_dict.0 + ranges.label_dict.1,
            ),
            (
                ranges.series_ids.0,
                ranges.series_ids.0 + ranges.series_ids.1,
            ),
            (
                ranges.series_idx.0,
                ranges.series_idx.0 + ranges.series_idx.1,
            ),
            (
                ranges.meta_chunks.0,
                ranges.meta_chunks.0 + ranges.meta_chunks.1,
            ),
        ];
        self.ensure_ranges(
            seg_ref,
            tenant_hash,
            suffix_etag,
            &needed,
            regions,
            accounting,
        )
        .await?;
        let dict = regions
            .slice(ranges.label_dict.0, ranges.label_dict.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let ids = regions
            .slice(ranges.series_ids.0, ranges.series_ids.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let idx = regions
            .slice(ranges.series_idx.0, ranges.series_idx.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let chunks = regions
            .slice(ranges.meta_chunks.0, ranges.meta_chunks.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        decode_catalog_v5_chunked(footer, &dict, &ids, &idx, &chunks, self.limits)
            .map_err(|source| corrupt(key, source))
    }

    /// Coalesced page ranges for the scalar runs of `selected` (histogram
    /// runs carry no scalar samples and are skipped), fetched into `regions`.
    /// Returns the plan for every run of every scalar series.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_scalar_pages(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        footer: &Footer,
        scalar: &[&SeriesEntryV4],
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
        accounting: &QueryAccounting,
    ) -> Result<Vec<ravel_segment::PlannedRunRange>, FetchError> {
        // See `open_segment`'s comment: recorded on this handle directly,
        // never through `tracing::Span::current()`.
        let span = tracing::debug_span!(
            "page_fetch",
            page_kind = "scalar",
            series_count = scalar.len(),
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
        );
        async {
            let key = seg_ref.data_object_key.as_str();
            let planned = plan_ranges_v4(footer, scalar).map_err(|source| corrupt(key, source))?;
            let page_ranges: Vec<(u64, u64)> = planned
                .iter()
                .flat_map(|p| {
                    [
                        (p.ts_range.0, p.ts_range.0 + p.ts_range.1),
                        (p.val_range.0, p.val_range.0 + p.val_range.1),
                    ]
                })
                .collect();
            let cost = self
                .ensure_ranges(
                    seg_ref,
                    tenant_hash,
                    suffix_etag,
                    &page_ranges,
                    regions,
                    accounting,
                )
                .await?;
            // This call's own coalesced page-range GET cost, from the GETs
            // `ensure_ranges` issued for this invocation, scoped to the span.
            span.record("s3_requests", cost.requests);
            span.record("s3_bytes", cost.bytes);
            Ok(planned)
        }
        .instrument(span.clone())
        .await
    }

    /// Histogram counterpart to [`fetch_scalar_pages`](Self::fetch_scalar_pages)
    ///: coalesced TS/HIST page ranges for the histogram runs of
    /// `histogram`, fetched into `regions`. `plan_ranges_v4` fills each
    /// histogram run's `hist_range` (and leaves `val_range` a `(0, 0)`
    /// sentinel), so this fetches the TS and HIST byte ranges the histogram
    /// decode path reads.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_histogram_pages(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        footer: &Footer,
        histogram: &[&SeriesEntryV4],
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
        accounting: &QueryAccounting,
    ) -> Result<Vec<ravel_segment::PlannedRunRange>, FetchError> {
        // See `open_segment`'s comment: recorded on this handle directly,
        // never through `tracing::Span::current()`.
        let span = tracing::debug_span!(
            "page_fetch",
            page_kind = "histogram",
            series_count = histogram.len(),
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
        );
        async {
            let key = seg_ref.data_object_key.as_str();
            let planned =
                plan_ranges_v4(footer, histogram).map_err(|source| corrupt(key, source))?;
            let page_ranges: Vec<(u64, u64)> = planned
                .iter()
                .flat_map(|p| {
                    [
                        (p.ts_range.0, p.ts_range.0 + p.ts_range.1),
                        (p.hist_range.0, p.hist_range.0 + p.hist_range.1),
                    ]
                })
                .collect();
            let cost = self
                .ensure_ranges(
                    seg_ref,
                    tenant_hash,
                    suffix_etag,
                    &page_ranges,
                    regions,
                    accounting,
                )
                .await?;
            // This call's own coalesced page-range GET cost, from the GETs
            // `ensure_ranges` issued for this invocation, scoped to the span.
            span.record("s3_requests", cost.requests);
            span.record("s3_bytes", cost.bytes);
            Ok(planned)
        }
        .instrument(span.clone())
        .await
    }

    /// Decodes one scalar run's TS/VAL pages into `timestamps`/`values`
    /// (appending), returning the VAL page kind for stats. Shared by the L0
    /// and L1 sample paths; reuses the caller's decompression `scratch`.
    #[allow(clippy::too_many_arguments)]
    fn decode_run(
        &self,
        key: &str,
        series_id: &SeriesId,
        run: &RunEntry,
        plan: &ravel_segment::PlannedRunRange,
        regions: &FetchedRegions,
        scratch: &mut Vec<u8>,
        timestamps: &mut Vec<i64>,
        values: &mut Vec<f64>,
        accounting: &QueryAccounting,
    ) -> Result<(ValPageKind, u64), FetchError> {
        let ts_bytes = regions
            .slice(plan.ts_range.0, plan.ts_range.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let val_bytes = regions
            .slice(plan.val_range.0, plan.val_range.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let before = timestamps.len();
        let kind = decode_run_pages_soa(
            series_id,
            run,
            &ts_bytes,
            &val_bytes,
            self.limits,
            scratch,
            timestamps,
            values,
        )
        .map_err(|source| corrupt(key, source))?;
        // Typed-output footprint (i64 ts + f64 value per sample), not the
        // intermediate decompression buffer size: `ravel-segment` does not
        // expose the latter without an out-of-scope change (docs/query-engine.md
        // "Cost accounting").
        let added = (timestamps.len() - before) as u64;
        // Query-level accounting still needs this increment to the shared
        // handle; the decode span's own count is summed locally by the caller
        // from the returned value, not diffed off this shared handle (ADR-0044
        // decision 5).
        let decompressed = added * 16;
        accounting.add_decompressed_bytes(decompressed);
        Ok((kind, decompressed))
    }

    /// Histogram counterpart to [`decode_run`](Self::decode_run):
    /// decodes one histogram run's TS/HIST pages into `timestamps`/`values`
    /// (appending). `decode_run_histogram_pages` yields combined
    /// [`ravel_segment::HistogramSample`]s (ts + value), which this splits into
    /// the parallel vecs the SoA shape carries, preserving on-disk order.
    #[allow(clippy::too_many_arguments)]
    fn decode_histogram_run(
        &self,
        key: &str,
        series_id: &SeriesId,
        run: &RunEntry,
        plan: &ravel_segment::PlannedRunRange,
        regions: &FetchedRegions,
        timestamps: &mut Vec<i64>,
        values: &mut Vec<HistogramValue>,
        accounting: &QueryAccounting,
    ) -> Result<u64, FetchError> {
        let ts_bytes = regions
            .slice(plan.ts_range.0, plan.ts_range.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let hist_bytes = regions
            .slice(plan.hist_range.0, plan.hist_range.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let samples =
            decode_run_histogram_pages(series_id, run, &ts_bytes, &hist_bytes, self.limits)
                .map_err(|source| corrupt(key, source))?;
        let mut added_bytes: u64 = 0;
        for sample in samples {
            added_bytes = added_bytes.saturating_add(8 + histogram_value_footprint(&sample.value));
            timestamps.push(sample.ts_ns);
            values.push(sample.value);
        }
        accounting.add_decompressed_bytes(added_bytes);
        Ok(added_bytes)
    }

    /// Core of `fetch`/`fetch_soa`: decodes every matched scalar series into
    /// one [`RunDecode`] per emitted unit, with resolved provenance keyed on
    /// the segment level:
    ///
    /// - **L0**: one unit per series, all runs concatenated in on-disk order,
    ///   stamped with the segment's commit-record provenance
    ///   (`seg_ref.created_unix_ns`/`writer_epoch`/`writer_seq`). An L0 flush
    ///   frames exactly one run per series, so this is one unit per series.
    /// - **L1**: one unit per (series, run), each stamped with that run's own
    ///   provenance copied from the input's commit record at compaction
    ///   (`RunEntry::created_unix_ns`/`writer_epoch`/`writer_seq`). This makes
    ///   a query over the L1 part see the same `(series, ts, provenance)`
    ///   tuples, in the same dedup total order, as a query over the L0 inputs
    ///   it was built from.
    ///
    /// Histogram-valued series are skipped here: a scalar SoA cannot hold
    /// them. They are fetched by the mirror-image
    /// [`fetch_histogram_runs`](Self::fetch_histogram_runs) instead.
    async fn fetch_runs(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
        count_stats: bool,
        accounting: &QueryAccounting,
    ) -> Result<(Vec<RunDecode>, FetchStats), FetchError> {
        let key = &seg_ref.data_object_key;
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref, accounting).await?;
        let selected = self
            .decode_selected(
                seg_ref,
                tenant_hash,
                &footer,
                total_size,
                &suffix_etag,
                &mut regions,
                matchers,
                accounting,
            )
            .await?;
        let scalar: Vec<&SeriesEntryV4> = selected
            .iter()
            .filter(|e| e.entry.value_kind == ValueKind::Scalar)
            .collect();
        if scalar.is_empty() {
            return Ok((Vec::new(), FetchStats::default()));
        }
        let planned = self
            .fetch_scalar_pages(
                seg_ref,
                tenant_hash,
                &footer,
                &scalar,
                &suffix_etag,
                &mut regions,
                accounting,
            )
            .await?;
        self.build_scalar_decodes(
            key,
            seg_ref,
            &scalar,
            &planned,
            &regions,
            count_stats,
            accounting,
        )
    }

    /// Decodes the already-fetched scalar page bytes of `scalar` into one
    /// [`RunDecode`] per emitted unit (see [`fetch_runs`](Self::fetch_runs)
    /// for the per-level emission contract). Split out so the scalar-only
    /// [`fetch_runs`](Self::fetch_runs) and the combined
    /// [`fetch_runs_and_histograms`](Self::fetch_runs_and_histograms) share
    /// one decode body rather than drifting apart.
    #[allow(clippy::too_many_arguments)]
    fn build_scalar_decodes(
        &self,
        key: &str,
        seg_ref: &SegmentRef,
        scalar: &[&SeriesEntryV4],
        planned: &[ravel_segment::PlannedRunRange],
        regions: &FetchedRegions,
        count_stats: bool,
        accounting: &QueryAccounting,
    ) -> Result<(Vec<RunDecode>, FetchStats), FetchError> {
        // See `open_segment`'s comment: recorded on this handle directly,
        // never through `tracing::Span::current()`. Synchronous function, so
        // an entered guard (not `.instrument()`) covers the whole body; no
        // `.await` point can invalidate it.
        let span = tracing::debug_span!(
            "decode",
            page_kind = "scalar",
            series_count = scalar.len(),
            decompressed_bytes = tracing::field::Empty,
        );
        let _guard = span.enter();
        // Summed locally from each `decode_run`'s own output rather than
        // diffed off the shared `QueryAccounting`: sibling segments decode
        // concurrently on other threads against the same handle, so a delta
        // would fold their decompressed bytes into this span (ADR-0044
        // decision 5).
        let mut span_decompressed: u64 = 0;
        let mut stats = FetchStats::default();
        let mut scratch = Vec::new();
        let mut out = Vec::with_capacity(scalar.len());
        for entry in scalar {
            match &seg_ref.level {
                SegmentLevel::L0 => {
                    // One unit per series: concatenate every run's samples in
                    // on-disk order, segment-level provenance. `decode_run`
                    // appends (its page decoders append), so the shared
                    // `timestamps`/`values` accumulate across runs rather than
                    // each run clobbering the last. An L0 flush frames exactly
                    // one run per series today, so this is normally a single
                    // pass; the concatenation is what keeps a multi-run L0
                    // (were one ever produced) correct instead of silently
                    // dropping all but the final run.
                    let mut timestamps = Vec::new();
                    let mut values = Vec::new();
                    for (run_index, run) in entry.runs.iter().enumerate() {
                        let plan = find_run_plan(planned, &entry.entry.series_id, run_index)
                            .ok_or_else(|| {
                                corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds)
                            })?;
                        let (kind, decoded) = self.decode_run(
                            key,
                            &entry.entry.series_id,
                            run,
                            plan,
                            regions,
                            &mut scratch,
                            &mut timestamps,
                            &mut values,
                            accounting,
                        )?;
                        span_decompressed = span_decompressed.saturating_add(decoded);
                        if count_stats {
                            stats.record_val_page(kind, plan.val_range.1 as usize);
                        }
                    }
                    out.push(RunDecode {
                        series_id: entry.entry.series_id,
                        labels: entry.entry.labels.clone(),
                        timestamps,
                        values,
                        created_unix_ns: seg_ref.created_unix_ns,
                        writer_epoch: seg_ref.writer_epoch,
                        writer_seq: seg_ref.writer_seq,
                    });
                }
                SegmentLevel::L1 { .. } => {
                    // One unit per (series, run): each run keeps its own
                    // provenance so cross-input duplicate samples resolve
                    // under the same total order as the pre-compaction L0s.
                    for (run_index, run) in entry.runs.iter().enumerate() {
                        let plan = find_run_plan(planned, &entry.entry.series_id, run_index)
                            .ok_or_else(|| {
                                corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds)
                            })?;
                        let mut timestamps = Vec::new();
                        let mut values = Vec::new();
                        let (kind, decoded) = self.decode_run(
                            key,
                            &entry.entry.series_id,
                            run,
                            plan,
                            regions,
                            &mut scratch,
                            &mut timestamps,
                            &mut values,
                            accounting,
                        )?;
                        span_decompressed = span_decompressed.saturating_add(decoded);
                        if count_stats {
                            stats.record_val_page(kind, plan.val_range.1 as usize);
                        }
                        out.push(RunDecode {
                            series_id: entry.entry.series_id,
                            labels: entry.entry.labels.clone(),
                            timestamps,
                            values,
                            created_unix_ns: run.created_unix_ns,
                            writer_epoch: run.writer_epoch,
                            writer_seq: run.writer_seq,
                        });
                    }
                }
            }
        }
        // This decode pass's own decompressed-byte output, scoped to the span.
        span.record("decompressed_bytes", span_decompressed);
        Ok((out, stats))
    }

    /// Histogram counterpart to [`fetch_runs`](Self::fetch_runs):
    /// decodes every matched histogram-kind series into one
    /// [`RunHistogramDecode`] per emitted unit, with the same per-level
    /// provenance resolution as the scalar path (L0: one unit per series, runs
    /// concatenated, segment provenance; L1: one unit per (series, run), each
    /// run's own provenance). These are the histogram-kind entries the scalar
    /// path's `ValueKind::Scalar` filter drops.
    async fn fetch_histogram_runs(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
    ) -> Result<Vec<RunHistogramDecode>, FetchError> {
        let key = &seg_ref.data_object_key;
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref, accounting).await?;
        let selected = self
            .decode_selected(
                seg_ref,
                tenant_hash,
                &footer,
                total_size,
                &suffix_etag,
                &mut regions,
                matchers,
                accounting,
            )
            .await?;
        let histogram: Vec<&SeriesEntryV4> = selected
            .iter()
            .filter(|e| e.entry.value_kind == ValueKind::Histogram)
            .collect();
        if histogram.is_empty() {
            return Ok(Vec::new());
        }
        let planned = self
            .fetch_histogram_pages(
                seg_ref,
                tenant_hash,
                &footer,
                &histogram,
                &suffix_etag,
                &mut regions,
                accounting,
            )
            .await?;
        self.build_histogram_decodes(key, seg_ref, &histogram, &planned, &regions, accounting)
    }

    /// Histogram counterpart to
    /// [`build_scalar_decodes`](Self::build_scalar_decodes): decodes the
    /// already-fetched histogram page bytes of `histogram` into one
    /// [`RunHistogramDecode`] per emitted unit. Shared by the histogram-only
    /// [`fetch_histogram_runs`](Self::fetch_histogram_runs) and the combined
    /// [`fetch_runs_and_histograms`](Self::fetch_runs_and_histograms).
    fn build_histogram_decodes(
        &self,
        key: &str,
        seg_ref: &SegmentRef,
        histogram: &[&SeriesEntryV4],
        planned: &[ravel_segment::PlannedRunRange],
        regions: &FetchedRegions,
        accounting: &QueryAccounting,
    ) -> Result<Vec<RunHistogramDecode>, FetchError> {
        // See `open_segment`'s comment: recorded on this handle directly,
        // never through `tracing::Span::current()`. Synchronous function, so
        // an entered guard (not `.instrument()`) covers the whole body; no
        // `.await` point can invalidate it.
        let span = tracing::debug_span!(
            "decode",
            page_kind = "histogram",
            series_count = histogram.len(),
            decompressed_bytes = tracing::field::Empty,
        );
        let _guard = span.enter();
        // Summed locally from each `decode_histogram_run`'s own output rather
        // than diffed off the shared `QueryAccounting`, for the same
        // cross-thread reason as the scalar path (ADR-0044 decision 5).
        let mut span_decompressed: u64 = 0;
        let mut out = Vec::with_capacity(histogram.len());
        for entry in histogram {
            match &seg_ref.level {
                SegmentLevel::L0 => {
                    // One unit per series: concatenate every run's samples in
                    // on-disk order, segment-level provenance.
                    let mut timestamps = Vec::new();
                    let mut values = Vec::new();
                    for (run_index, run) in entry.runs.iter().enumerate() {
                        let plan = find_run_plan(planned, &entry.entry.series_id, run_index)
                            .ok_or_else(|| {
                                corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds)
                            })?;
                        let decoded = self.decode_histogram_run(
                            key,
                            &entry.entry.series_id,
                            run,
                            plan,
                            regions,
                            &mut timestamps,
                            &mut values,
                            accounting,
                        )?;
                        span_decompressed = span_decompressed.saturating_add(decoded);
                    }
                    out.push(RunHistogramDecode {
                        series_id: entry.entry.series_id,
                        labels: entry.entry.labels.clone(),
                        timestamps,
                        values,
                        created_unix_ns: seg_ref.created_unix_ns,
                        writer_epoch: seg_ref.writer_epoch,
                        writer_seq: seg_ref.writer_seq,
                    });
                }
                SegmentLevel::L1 { .. } => {
                    // One unit per (series, run): each run keeps its own
                    // provenance so cross-input duplicate samples resolve under
                    // the same total order as the pre-compaction L0s.
                    for (run_index, run) in entry.runs.iter().enumerate() {
                        let plan = find_run_plan(planned, &entry.entry.series_id, run_index)
                            .ok_or_else(|| {
                                corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds)
                            })?;
                        let mut timestamps = Vec::new();
                        let mut values = Vec::new();
                        let decoded = self.decode_histogram_run(
                            key,
                            &entry.entry.series_id,
                            run,
                            plan,
                            regions,
                            &mut timestamps,
                            &mut values,
                            accounting,
                        )?;
                        span_decompressed = span_decompressed.saturating_add(decoded);
                        out.push(RunHistogramDecode {
                            series_id: entry.entry.series_id,
                            labels: entry.entry.labels.clone(),
                            timestamps,
                            values,
                            created_unix_ns: run.created_unix_ns,
                            writer_epoch: run.writer_epoch,
                            writer_seq: run.writer_seq,
                        });
                    }
                }
            }
        }
        // This decode pass's own decompressed-byte output, scoped to the span.
        span.record("decompressed_bytes", span_decompressed);
        Ok(out)
    }

    /// Opens the segment once and decodes both its matched scalar and matched
    /// histogram series in a single pass. The scalar-only
    /// [`fetch_runs`](Self::fetch_runs) and histogram-only
    /// [`fetch_histogram_runs`](Self::fetch_histogram_runs) each re-run
    /// `open_segment` (a footer GET) and `decode_selected` (catalog decode)
    /// from scratch; a PromQL/SQL prefetch needs both kinds off every segment,
    /// so calling the two in sequence opened and decoded each segment twice.
    /// This shares the one `open_segment` + `decode_selected`, then fetches
    /// and decodes the scalar and histogram pages off the same `regions`,
    /// returning byte-for-byte the same units the two separate calls did.
    async fn fetch_runs_and_histograms(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
        count_stats: bool,
        accounting: &QueryAccounting,
    ) -> Result<(Vec<RunDecode>, FetchStats, Vec<RunHistogramDecode>), FetchError> {
        let key = &seg_ref.data_object_key;
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref, accounting).await?;
        let selected = self
            .decode_selected(
                seg_ref,
                tenant_hash,
                &footer,
                total_size,
                &suffix_etag,
                &mut regions,
                matchers,
                accounting,
            )
            .await?;
        let scalar: Vec<&SeriesEntryV4> = selected
            .iter()
            .filter(|e| e.entry.value_kind == ValueKind::Scalar)
            .collect();
        let histogram: Vec<&SeriesEntryV4> = selected
            .iter()
            .filter(|e| e.entry.value_kind == ValueKind::Histogram)
            .collect();

        let scalar_planned = if scalar.is_empty() {
            Vec::new()
        } else {
            self.fetch_scalar_pages(
                seg_ref,
                tenant_hash,
                &footer,
                &scalar,
                &suffix_etag,
                &mut regions,
                accounting,
            )
            .await?
        };
        let histogram_planned = if histogram.is_empty() {
            Vec::new()
        } else {
            self.fetch_histogram_pages(
                seg_ref,
                tenant_hash,
                &footer,
                &histogram,
                &suffix_etag,
                &mut regions,
                accounting,
            )
            .await?
        };

        let (scalar_out, stats) = if scalar.is_empty() {
            (Vec::new(), FetchStats::default())
        } else {
            self.build_scalar_decodes(
                key,
                seg_ref,
                &scalar,
                &scalar_planned,
                &regions,
                count_stats,
                accounting,
            )?
        };
        let histogram_out = if histogram.is_empty() {
            Vec::new()
        } else {
            self.build_histogram_decodes(
                key,
                seg_ref,
                &histogram,
                &histogram_planned,
                &regions,
                accounting,
            )?
        };
        Ok((scalar_out, stats, histogram_out))
    }

    /// Returns the series (labels only, no samples) in this segment matching
    /// `matchers`. Used by the labels/label-values/series HTTP endpoints,
    /// which never need page data. Returns the folded per-series
    /// [`SeriesEntry`] view (labels + identity); an L1 part's multi-run
    /// series fold to the same per-series shape.
    pub async fn fetch_series(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SeriesEntry>, FetchError> {
        self.fetch_series_accounted(tenant_hash, seg_ref, matchers, &QueryAccounting::new())
            .await
    }

    /// Accounted counterpart of [`fetch_series`](Self::fetch_series): same
    /// behavior, plus every store GET and matched series is recorded against
    /// `accounting` (ADR-0044). `engine.rs` calls this; `fetch_series` stays
    /// the unaccounted entry point so `ravel-sql`'s direct calls need no
    /// signature change.
    pub async fn fetch_series_accounted(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
    ) -> Result<Vec<SeriesEntry>, FetchError> {
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref, accounting).await?;
        let selected = self
            .decode_selected(
                seg_ref,
                tenant_hash,
                &footer,
                total_size,
                &suffix_etag,
                &mut regions,
                matchers,
                accounting,
            )
            .await?;
        Ok(selected.into_iter().map(|e| e.entry).collect())
    }

    /// Fetches and decodes the scalar samples of every series in this segment
    /// matching `matchers`. Histogram-kind series carry no scalar samples and
    /// are skipped: the scalar query path (PromQL/SQL) does not consume them.
    /// For an L1 part this emits one [`FetchedSeries`] per (series, run) with
    /// the run's provenance; for an L0 segment one per series with the
    /// segment's provenance (see [`fetch_runs`](Self::fetch_runs)).
    pub async fn fetch(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<FetchedSeries>, FetchError> {
        self.fetch_accounted(tenant_hash, seg_ref, matchers, &QueryAccounting::new())
            .await
    }

    /// Accounted counterpart of [`fetch`](Self::fetch); see
    /// [`fetch_series_accounted`](Self::fetch_series_accounted) for why both
    /// forms exist.
    pub async fn fetch_accounted(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
    ) -> Result<Vec<FetchedSeries>, FetchError> {
        let (runs, _stats) = self
            .fetch_runs(tenant_hash, seg_ref, matchers, false, accounting)
            .await?;
        Ok(runs.into_iter().map(RunDecode::into_aos).collect())
    }

    /// SoA counterpart to `fetch`: decodes the same selected scalar series but returns timestamps
    /// and values as separate vecs per emitted unit, plus page-kind stats.
    /// Same per-level emission shape and provenance as `fetch`
    /// (see [`fetch_runs`](Self::fetch_runs)). Reuses one decompression
    /// scratch buffer across every run in the segment.
    pub async fn fetch_soa(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<FetchedSeriesSoa>, FetchStats), FetchError> {
        self.fetch_soa_accounted(tenant_hash, seg_ref, matchers, &QueryAccounting::new())
            .await
    }

    /// Accounted counterpart of [`fetch_soa`](Self::fetch_soa); see
    /// [`fetch_series_accounted`](Self::fetch_series_accounted) for why both
    /// forms exist.
    pub async fn fetch_soa_accounted(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
    ) -> Result<(Vec<FetchedSeriesSoa>, FetchStats), FetchError> {
        let (runs, stats) = self
            .fetch_runs(tenant_hash, seg_ref, matchers, true, accounting)
            .await?;
        Ok((runs.into_iter().map(RunDecode::into_soa).collect(), stats))
    }

    /// Histogram counterpart to [`fetch_soa`](Self::fetch_soa): fetches
    /// and decodes the native-histogram samples of every histogram-kind series
    /// in this segment matching `matchers`, as SoA
    /// [`FetchedHistogramSeries`]. Scalar-kind series carry no histogram
    /// samples and are skipped (the mirror image of `fetch_soa` skipping
    /// histogram-kind series). Same per-level emission shape and provenance as
    /// `fetch_soa` (see [`fetch_histogram_runs`](Self::fetch_histogram_runs)).
    pub async fn fetch_histograms(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<FetchedHistogramSeries>, FetchError> {
        self.fetch_histograms_accounted(tenant_hash, seg_ref, matchers, &QueryAccounting::new())
            .await
    }

    /// Accounted counterpart of [`fetch_histograms`](Self::fetch_histograms);
    /// see [`fetch_series_accounted`](Self::fetch_series_accounted) for why
    /// both forms exist.
    pub async fn fetch_histograms_accounted(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
    ) -> Result<Vec<FetchedHistogramSeries>, FetchError> {
        let runs = self
            .fetch_histogram_runs(tenant_hash, seg_ref, matchers, accounting)
            .await?;
        Ok(runs
            .into_iter()
            .map(RunHistogramDecode::into_fetched)
            .collect())
    }

    /// Single-open counterpart to calling [`fetch_soa`](Self::fetch_soa) and
    /// [`fetch_histograms`](Self::fetch_histograms) back to back on the same
    /// segment: opens the segment once, decodes its catalog
    /// once, and returns both the scalar SoA series (with page-kind stats) and
    /// the native-histogram series. The scalar and histogram results are
    /// identical to the two separate calls; only the segment open and catalog
    /// decode are shared instead of paid twice.
    pub async fn fetch_soa_and_histograms(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<
        (
            Vec<FetchedSeriesSoa>,
            FetchStats,
            Vec<FetchedHistogramSeries>,
        ),
        FetchError,
    > {
        self.fetch_soa_and_histograms_accounted(
            tenant_hash,
            seg_ref,
            matchers,
            &QueryAccounting::new(),
        )
        .await
    }

    /// Accounted counterpart of
    /// [`fetch_soa_and_histograms`](Self::fetch_soa_and_histograms); see
    /// [`fetch_series_accounted`](Self::fetch_series_accounted) for why both
    /// forms exist.
    pub async fn fetch_soa_and_histograms_accounted(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
    ) -> Result<
        (
            Vec<FetchedSeriesSoa>,
            FetchStats,
            Vec<FetchedHistogramSeries>,
        ),
        FetchError,
    > {
        let (runs, stats, hist_runs) = self
            .fetch_runs_and_histograms(tenant_hash, seg_ref, matchers, true, accounting)
            .await?;
        Ok((
            runs.into_iter().map(RunDecode::into_soa).collect(),
            stats,
            hist_runs
                .into_iter()
                .map(RunHistogramDecode::into_fetched)
                .collect(),
        ))
    }
}

/// One decoded, provenance-resolved emission unit, convertible to either the
/// AoS or SoA fetched shape. For L0 this is one series (runs concatenated);
/// for L1 this is one (series, run).
struct RunDecode {
    series_id: SeriesId,
    labels: LabelSet,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
}

impl RunDecode {
    // `per_sample_priorities: None` on both conversions is the level-keyed
    // emission contract above, not a stub: an emission unit is one series' runs
    // from one L0, or one L1 run, and every sample in it shares the run-wide
    // provenance the unit carries. A per-sample column only becomes possible
    // once a run merges several writes' samples (ADR-0092 decision 1, issue
    // #315).
    fn into_soa(self) -> FetchedSeriesSoa {
        FetchedSeriesSoa {
            series_id: self.series_id,
            labels: self.labels,
            timestamps: self.timestamps,
            values: self.values,
            created_unix_ns: self.created_unix_ns,
            writer_epoch: self.writer_epoch,
            writer_seq: self.writer_seq,
            per_sample_priorities: None,
        }
    }

    fn into_aos(self) -> FetchedSeries {
        let samples = self
            .timestamps
            .into_iter()
            .zip(self.values)
            .map(|(ts_ns, value)| Sample { ts_ns, value })
            .collect();
        FetchedSeries {
            series_id: self.series_id,
            labels: self.labels,
            samples,
            created_unix_ns: self.created_unix_ns,
            writer_epoch: self.writer_epoch,
            writer_seq: self.writer_seq,
            per_sample_priorities: None,
        }
    }
}

/// Histogram counterpart to [`RunDecode`]: one decoded,
/// provenance-resolved histogram emission unit. For L0 this is one series
/// (runs concatenated); for L1 this is one (series, run).
struct RunHistogramDecode {
    series_id: SeriesId,
    labels: LabelSet,
    timestamps: Vec<i64>,
    values: Vec<HistogramValue>,
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
}

impl RunHistogramDecode {
    // Run-wide provenance only, for the same reason as [`RunDecode::into_soa`].
    fn into_fetched(self) -> FetchedHistogramSeries {
        FetchedHistogramSeries {
            series_id: self.series_id,
            labels: self.labels,
            timestamps: self.timestamps,
            values: self.values,
            created_unix_ns: self.created_unix_ns,
            writer_epoch: self.writer_epoch,
            writer_seq: self.writer_seq,
            per_sample_priorities: None,
        }
    }
}

/// Verify an L1 part's v5 footer against the compaction record's identity
/// fields the [`SegmentRef`] carries (readers verify
/// tenant/shard/ingest_hour/input_set_hash/part_index against the record,
/// the L1 analog of ADR-0010 §7). A part has no writer
/// identity, so these five fields plus `level == 1` are the identity.
/// ADR-0027 leaves v5 the only version, so there is no format-version field
/// to check here beyond what `open_from_suffix` already enforced.
fn verify_l1_identity(
    footer: &Footer,
    tenant_hash: TenantHash,
    seg_ref: &SegmentRef,
    input_set_hash: &[u8; 32],
    part_index: u32,
) -> Result<(), ravel_segment::SegmentError> {
    if footer.tenant_hash.as_slice() != tenant_hash.0.as_slice() {
        return Err(ravel_segment::SegmentError::IdentityMismatch("tenant_hash"));
    }
    if footer.shard != seg_ref.shard {
        return Err(ravel_segment::SegmentError::IdentityMismatch("shard"));
    }
    if footer.ingest_hour_bucket != seg_ref.ingest_hour_bucket {
        return Err(ravel_segment::SegmentError::IdentityMismatch(
            "ingest_hour_bucket",
        ));
    }
    if footer.input_set_hash.as_slice() != input_set_hash.as_slice() {
        return Err(ravel_segment::SegmentError::IdentityMismatch(
            "input_set_hash",
        ));
    }
    if footer.part_index != part_index {
        return Err(ravel_segment::SegmentError::IdentityMismatch("part_index"));
    }
    if footer.level != 1 {
        return Err(ravel_segment::SegmentError::IdentityMismatch("level"));
    }
    Ok(())
}

/// Looks up the planned byte ranges for one run of one series.
fn find_run_plan<'a>(
    planned: &'a [ravel_segment::PlannedRunRange],
    series_id: &SeriesId,
    run_index: usize,
) -> Option<&'a ravel_segment::PlannedRunRange> {
    planned
        .iter()
        .find(|p| &p.series_id == series_id && p.run_index == run_index)
}

fn expected_identity(tenant_hash: TenantHash, seg_ref: &SegmentRef) -> ExpectedIdentity {
    ExpectedIdentity {
        tenant_hash: tenant_hash.0,
        shard: seg_ref.shard,
        writer_id: seg_ref.writer_id.to_string(),
        writer_epoch: seg_ref.writer_epoch,
        writer_seq: seg_ref.writer_seq,
    }
}

fn corrupt(key: &str, source: ravel_segment::SegmentError) -> FetchError {
    FetchError::Corrupt {
        key: key.to_string(),
        source,
    }
}

/// Builds the `GetOutcome` a cache-routed `guarded_get` returns, whether the
/// bytes came from a cache hit or from the store call inside
/// `Cache::get_or_fetch`. `etag`/`version` are placeholders: nothing
/// downstream of a cache-routed get compares them (see `guarded_get`'s doc
/// comment on why the etag check is skipped for that path), so there is no
/// live value to put there.
fn placeholder_outcome(data: Bytes, total_size: u64) -> GetOutcome {
    GetOutcome {
        data,
        etag: Etag(String::new()),
        version: Version(String::new()),
        total_size,
    }
}

/// Reconstructs an owned `StoreError` from a shared `&StoreError` after a
/// cache single-flight resolves (`Cache<E>` requires `E: Clone`, which
/// `StoreError` itself is not; the cache is built over `Arc<StoreError>`
/// instead, and this un-shares it for the one caller that needs a plain
/// `StoreError` to build a `FetchError::Store`). Exhaustive over every
/// `StoreError` variant on purpose: a new variant must fail to compile here
/// rather than silently falling back to a generic one.
pub(crate) fn clone_store_error(err: &StoreError) -> StoreError {
    match err {
        StoreError::NotFound => StoreError::NotFound,
        StoreError::AlreadyExists => StoreError::AlreadyExists,
        StoreError::PreconditionFailed => StoreError::PreconditionFailed,
        StoreError::AccessDenied(msg) => StoreError::AccessDenied(msg.clone()),
        StoreError::Throttled { retry_after_ms } => StoreError::Throttled {
            retry_after_ms: *retry_after_ms,
        },
        StoreError::Timeout => StoreError::Timeout,
        StoreError::Corrupted(msg) => StoreError::Corrupted(msg.clone()),
        StoreError::InvalidRange(msg) => StoreError::InvalidRange(msg.clone()),
        StoreError::Transient(msg) => StoreError::Transient(msg.clone()),
        StoreError::Permanent(msg) => StoreError::Permanent(msg.clone()),
    }
}

/// Exact typed-output byte footprint of one decoded `HistogramValue`, for
/// `decompressed_bytes` accounting (docs/query-engine.md "Cost accounting").
/// Mirrors the struct's own field layout rather than `size_of`, since
/// `size_of` would not count the heap bytes of its `Vec`/`Option<Vec>` fields.
fn histogram_value_footprint(v: &HistogramValue) -> u64 {
    let spans_bytes = |spans: &[ravel_segment::HistogramSpan]| (spans.len() as u64) * 8;
    let counts_bytes = match &v.counts {
        ravel_segment::HistogramCounts::Int {
            positive, negative, ..
        } => 16 + (positive.len() as u64) * 8 + (negative.len() as u64) * 8,
        ravel_segment::HistogramCounts::Float {
            positive, negative, ..
        } => 16 + (positive.len() as u64) * 8 + (negative.len() as u64) * 8,
    };
    4 // scale: i32
        + 8 // zero_threshold: f64
        + 8 // sum: Option<f64>, upper bound on the discriminant + payload
        + v.custom_values.as_ref().map_or(0, |c| (c.len() as u64) * 8)
        + spans_bytes(&v.positive_spans)
        + spans_bytes(&v.negative_spans)
        + counts_bytes
        + 1 // reset_hint: fieldless enum
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use ravel_catalog::SegmentRef;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence};
    use ravel_object_store::memory::MemoryStore;
    use ravel_segment::{
        HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds, ResetHint,
        SegmentIdentity, SegmentWriter, SeriesInput, SeriesInputV3, SeriesValues,
    };
    use ravel_types::{Label, LabelSet};
    use uuid::Uuid;

    use super::*;

    fn labels(metric: &str) -> LabelSet {
        LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels")
    }

    fn series(metric: &str, samples: &[(i64, f64)]) -> SeriesInput {
        let label_set = labels(metric);
        let tenant_id = ravel_types::TenantId::new("t".to_string());
        let series_id =
            ravel_types::SeriesId::compute(&tenant_id, metric, &label_set).expect("series id");
        SeriesInput {
            series_id,
            labels: label_set,
            samples: samples
                .iter()
                .map(|(ts_ns, value)| ravel_types::Sample {
                    ts_ns: *ts_ns,
                    value: *value,
                })
                .collect(),
        }
    }

    /// Writes a real RSEG segment with two series: one whose values Gorilla
    /// compresses well (identical values -> VAL_GORILLA) and one whose
    /// values are maximally incompressible (two samples with disjoint bit
    /// patterns -> VAL_RAW_F64, since the writer falls back to raw once the
    /// Gorilla encoding is not smaller than 8 bytes/sample). Puts the bytes
    /// directly on a `MemoryStore` and returns a matching `SegmentRef`.
    async fn write_test_segment() -> (Arc<MemoryStore>, TenantHash, SegmentRef) {
        let tenant_hash = TenantHash([7u8; 16]);
        let writer_id = Uuid::from_u128(1);
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        };
        const NS: i64 = 1_000_000_000;
        let smooth = series(
            "smooth_metric",
            &[(1_000 * NS, 1.0), (1_001 * NS, 1.0), (1_002 * NS, 1.0)],
        );
        let chaotic = series(
            "chaotic_metric",
            &[(1_000 * NS, 0.0), (1_001 * NS, f64::from_bits(u64::MAX))],
        );
        let written =
            SegmentWriter::write(vec![smooth, chaotic], identity, bounds).expect("write segment");

        let store = Arc::new(MemoryStore::new());
        let key = "test/segment.rseg";
        store
            .put(key, written.bytes.clone(), PutOptions::default())
            .await
            .expect("put segment object");

        let seg_ref = SegmentRef {
            data_object_key: key.to_string(),
            object_size: written.bytes.len() as u64,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            ingest_hour_bucket: 0,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            shard: 0,
            content_hash: written.summary.blake3,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 42,
            level: ravel_catalog::SegmentLevel::L0,
        };
        (store, tenant_hash, seg_ref)
    }

    #[tokio::test]
    async fn fetch_soa_matches_fetch_and_counts_raw_f64_pages() {
        let (store, tenant_hash, seg_ref) = write_test_segment().await;
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let fetcher = SegmentFetcher::new(backend);

        let mut aos = fetcher
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch");
        let (mut soa, stats) = fetcher
            .fetch_soa(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch_soa");

        aos.sort_by_key(|s| s.series_id.0);
        soa.sort_by_key(|s| s.series_id.0);

        assert_eq!(aos.len(), 2);
        assert_eq!(soa.len(), 2);
        for (a, s) in aos.iter().zip(soa.iter()) {
            assert_eq!(a.series_id, s.series_id);
            assert_eq!(a.labels, s.labels);
            assert_eq!(a.created_unix_ns, s.created_unix_ns);
            assert_eq!(a.writer_epoch, s.writer_epoch);
            assert_eq!(a.writer_seq, s.writer_seq);
            assert_eq!(a.samples.len(), s.timestamps.len());
            assert_eq!(s.timestamps.len(), s.values.len());
            for (sample, (ts, val)) in a
                .samples
                .iter()
                .zip(s.timestamps.iter().zip(s.values.iter()))
            {
                assert_eq!(sample.ts_ns, *ts);
                assert_eq!(sample.value.to_bits(), val.to_bits());
            }
        }

        // "chaotic_metric" (2 maximally-differing samples) must have forced
        // VAL_RAW_F64; "smooth_metric" (identical values) must have stayed
        // VAL_GORILLA. Exactly one raw page, exactly one page's worth of
        // raw-f64 bytes (6-byte header + 2 * 8-byte values).
        assert_eq!(stats.raw_f64_pages, 1);
        assert_eq!(stats.raw_f64_bytes, 6 + 2 * 8);
    }

    /// ADR-0044: `guarded_get`'s counters must match what the store itself
    /// recorded (cross-checked against `InstrumentedStore`, the object-store
    /// metrics oracle), and `bytes_reused` must be non-zero for a segment
    /// with more than one page served from an already-fetched region.
    #[tokio::test]
    async fn guarded_get_records_requests_bytes_and_reuse() {
        let (store, tenant_hash, seg_ref) = write_test_segment().await;
        let bytes = store
            .get(&seg_ref.data_object_key, GetRange::Full)
            .await
            .expect("read back test segment")
            .data;
        let (fetcher, metrics) = metered_fetcher(&seg_ref.data_object_key, bytes).await;

        let accounting = QueryAccounting::new();
        let (soa, _stats) = fetcher
            .fetch_soa_accounted(tenant_hash, &seg_ref, &[], &accounting)
            .await
            .expect("fetch_soa_accounted");
        assert_eq!(soa.len(), 2);

        let snapshot = accounting.snapshot();
        let store_get = metrics.snapshot().get;

        assert_eq!(
            snapshot.s3_requests(AccountedOp::Get),
            store_get.calls,
            "guarded_get's request count must match the store's own call count"
        );
        assert_eq!(
            snapshot.s3_bytes(AccountedOp::Get),
            store_get.bytes,
            "guarded_get's byte count must match the store's own bytes-returned count"
        );
        assert!(store_get.calls >= 1, "fetch must issue at least one GET");
        assert!(
            snapshot.bytes_reused > 0,
            "two series sharing one small whole-object fetch must reuse bytes \
             across pages instead of re-fetching them"
        );
        assert_eq!(snapshot.segments_opened, 1);
        assert_eq!(snapshot.series_matched, 2);
    }

    /// ADR-0044: accounting must be pure observation. The accounted and
    /// unaccounted entry points must fetch identical bytes and decode
    /// identical samples -- including the NaN bit pattern in
    /// "chaotic_metric" -- with counting the only added work.
    #[tokio::test]
    async fn fetch_soa_accounted_matches_unaccounted_fetch_bit_for_bit() {
        let (store, tenant_hash, seg_ref) = write_test_segment().await;
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let fetcher = SegmentFetcher::new(backend);

        let (mut plain, plain_stats) = fetcher
            .fetch_soa(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch_soa");
        let accounting = QueryAccounting::new();
        let (mut accounted, accounted_stats) = fetcher
            .fetch_soa_accounted(tenant_hash, &seg_ref, &[], &accounting)
            .await
            .expect("fetch_soa_accounted");

        plain.sort_by_key(|s| s.series_id.0);
        accounted.sort_by_key(|s| s.series_id.0);

        assert_eq!(plain_stats, accounted_stats);
        assert_eq!(plain.len(), accounted.len());
        for (p, a) in plain.iter().zip(accounted.iter()) {
            assert_eq!(p.series_id, a.series_id);
            assert_eq!(p.labels, a.labels);
            assert_eq!(p.timestamps, a.timestamps);
            assert_eq!(p.values.len(), a.values.len());
            for (pv, av) in p.values.iter().zip(a.values.iter()) {
                assert_eq!(
                    pv.to_bits(),
                    av.to_bits(),
                    "sample bit patterns must match exactly, NaN included"
                );
            }
        }

        let snapshot = accounting.snapshot();
        assert!(
            snapshot.segments_opened > 0,
            "the accounted path must have actually recorded counters"
        );
    }

    /// A simple int-counts native histogram varying only by `count`/`sum`, so
    /// two samples in one series are structurally distinct and their round-trip
    /// is unambiguous.
    fn hist_value(count: u64, sum: f64) -> HistogramValue {
        HistogramValue {
            scale: 2,
            zero_threshold: 1e-9,
            sum: Some(sum),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: 0,
                count,
                positive: vec![count],
                negative: vec![],
            },
            reset_hint: ResetHint::Unknown,
        }
    }

    fn hist_series(metric: &str, samples: Vec<HistogramSample>) -> SeriesInputV3 {
        let label_set = labels(metric);
        let tenant_id = ravel_types::TenantId::new("t".to_string());
        let series_id =
            ravel_types::SeriesId::compute(&tenant_id, metric, &label_set).expect("series id");
        SeriesInputV3 {
            series_id,
            labels: label_set,
            values: SeriesValues::Histogram(samples),
        }
    }

    fn scalar_series_v3(metric: &str, samples: &[(i64, f64)]) -> SeriesInputV3 {
        let label_set = labels(metric);
        let tenant_id = ravel_types::TenantId::new("t".to_string());
        let series_id =
            ravel_types::SeriesId::compute(&tenant_id, metric, &label_set).expect("series id");
        SeriesInputV3 {
            series_id,
            labels: label_set,
            values: SeriesValues::Scalar(
                samples
                    .iter()
                    .map(|(ts_ns, value)| ravel_types::Sample {
                        ts_ns: *ts_ns,
                        value: *value,
                    })
                    .collect(),
            ),
        }
    }

    /// Writes a real HIST_PAGES segment (via `write_histograms`) carrying one
    /// histogram-kind series and one scalar-kind series, puts it on a
    /// `MemoryStore`, and returns a matching L0 `SegmentRef`.
    async fn write_histogram_test_segment() -> (Arc<MemoryStore>, TenantHash, SegmentRef) {
        let tenant_hash = TenantHash([9u8; 16]);
        let writer_id = Uuid::from_u128(2);
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        };
        const NS: i64 = 1_000_000_000;
        let hist = hist_series(
            "hist_metric",
            vec![
                HistogramSample {
                    ts_ns: 1_000 * NS,
                    value: hist_value(3, 6.0),
                },
                HistogramSample {
                    ts_ns: 1_001 * NS,
                    value: hist_value(5, 11.0),
                },
            ],
        );
        let scalar = scalar_series_v3("scalar_metric", &[(1_000 * NS, 1.0), (1_001 * NS, 2.0)]);
        let written = SegmentWriter::write_histograms(vec![hist, scalar], identity, bounds)
            .expect("write histogram segment");

        let store = Arc::new(MemoryStore::new());
        let key = "test/hist-segment.rseg";
        store
            .put(key, written.bytes.clone(), PutOptions::default())
            .await
            .expect("put segment object");

        let seg_ref = SegmentRef {
            data_object_key: key.to_string(),
            object_size: written.bytes.len() as u64,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            ingest_hour_bucket: 0,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            shard: 0,
            content_hash: written.summary.blake3,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 77,
            level: ravel_catalog::SegmentLevel::L0,
        };
        (store, tenant_hash, seg_ref)
    }

    #[tokio::test]
    async fn fetch_histograms_round_trips_hist_pages_and_skips_scalar() {
        let (store, tenant_hash, seg_ref) = write_histogram_test_segment().await;
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let fetcher = SegmentFetcher::new(backend);

        // The histogram path returns only the histogram-kind series, with its
        // samples decoded from HIST_PAGES back to the exact input values.
        let hist = fetcher
            .fetch_histograms(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch_histograms");
        assert_eq!(hist.len(), 1, "only the histogram-kind series is returned");
        let series = &hist[0];
        assert_eq!(series.labels, labels("hist_metric"));
        assert_eq!(series.created_unix_ns, seg_ref.created_unix_ns);
        const NS: i64 = 1_000_000_000;
        assert_eq!(series.timestamps, vec![1_000 * NS, 1_001 * NS]);
        assert_eq!(series.values, vec![hist_value(3, 6.0), hist_value(5, 11.0)]);

        // The scalar path is the mirror image: it returns only the scalar-kind
        // series and never the histogram one.
        let (scalar, _stats) = fetcher
            .fetch_soa(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch_soa");
        assert_eq!(scalar.len(), 1, "only the scalar-kind series is returned");
        assert_eq!(scalar[0].labels, labels("scalar_metric"));
    }

    /// Writes a real sparse (>= 4096-series) v5 RSEG object. Above the
    /// threshold `SegmentWriter::write` emits the SERIES_IDX + chunked
    /// SERIES_META sections (ADR-0026), so `SERIES_META` (kind 6) is absent and
    /// the fetcher sees a sparse object. `samples_per` scalar samples per series
    /// keep the TS/VAL page sections large relative to the catalog, so the
    /// catalog-probe path's byte win is unambiguous. Returns the object bytes
    /// and a matching L0 `SegmentRef`.
    async fn write_sparse_test_segment(
        n_series: usize,
        samples_per: usize,
    ) -> (Bytes, TenantHash, SegmentRef) {
        let tenant_hash = TenantHash([5u8; 16]);
        let writer_id = Uuid::from_u128(3);
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        };
        const NS: i64 = 1_000_000_000;
        let mut inputs = Vec::with_capacity(n_series);
        for i in 0..n_series {
            let metric = format!("sparse_metric_{i}");
            let samples: Vec<(i64, f64)> = (0..samples_per)
                .map(|j| {
                    (
                        (1_000 + j as i64) * NS,
                        (i as f64) * 7.0 + (j as f64) * 13.0 + 0.5,
                    )
                })
                .collect();
            inputs.push(series(&metric, &samples));
        }
        let written = SegmentWriter::write(inputs, identity, bounds).expect("write sparse segment");

        let seg_ref = SegmentRef {
            data_object_key: "test/sparse-segment.rseg".to_string(),
            object_size: written.bytes.len() as u64,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            ingest_hour_bucket: 0,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            shard: 0,
            content_hash: written.summary.blake3,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 99,
            level: ravel_catalog::SegmentLevel::L0,
        };
        (written.bytes, tenant_hash, seg_ref)
    }

    /// Puts `bytes` on a fresh `MemoryStore` wrapped in an `InstrumentedStore`,
    /// returning a fetcher over it plus the shared metrics handle. The metrics
    /// count exactly the GET calls and bytes the fetcher issued.
    async fn metered_fetcher(
        key: &str,
        bytes: Bytes,
    ) -> (SegmentFetcher, Arc<ravel_object_store::StoreMetrics>) {
        let memory = MemoryStore::new();
        memory
            .put(key, bytes, PutOptions::default())
            .await
            .expect("put sparse object");
        let instrumented = Arc::new(ravel_object_store::InstrumentedStore::new(memory));
        let metrics = instrumented.metrics();
        let backend: Arc<dyn ObjectStoreBackend> = instrumented;
        (SegmentFetcher::new(backend), metrics)
    }

    /// A matcher-pruned read of a sparse (>= 4096-series) v5 segment
    /// fetches only the catalog sections (LABEL_DICT + SERIES_IDS + SERIES_IDX +
    /// SERIES_META_CHUNKS) plus the matched series' pages, instead of a
    /// whole-object GET. The probe read moves far fewer bytes than the object
    /// size, and returns exactly the matched series' samples.
    #[tokio::test]
    async fn sparse_segment_uses_catalog_probe_path_not_whole_object() {
        let (bytes, tenant_hash, seg_ref) = write_sparse_test_segment(4096, 8).await;
        let object_size = seg_ref.object_size;
        assert!(
            object_size > SPARSE_PROBE_MIN_OBJECT_SIZE,
            "sparse test object ({object_size} B) must exceed the probe-path floor"
        );

        // Matcher pins one metric, so only its series' pages should be fetched.
        let matchers = [LabelMatcher::equal("__name__", "sparse_metric_2000")];
        let (fetcher, metrics) = metered_fetcher(&seg_ref.data_object_key, bytes.clone()).await;
        // The whole-object path reads an object whole on its first GET when its size is at
        // or below `whole_object_threshold` (default 512 KiB); this test's
        // sparse object is smaller than that, so without disabling it here the
        // first GET alone would already cover the whole object and the
        // catalog-probe path below would correctly find nothing left to fetch
        // -- exercising the whole-object optimization instead of the one this test is
        // for. Disable it so the first GET stays a footer suffix and this test
        // isolates the catalog-probe path on its own.
        let fetcher = fetcher.with_whole_object_threshold(0);
        let (soa, _stats) = fetcher
            .fetch_soa(tenant_hash, &seg_ref, &matchers)
            .await
            .expect("fetch_soa probe path");

        assert_eq!(soa.len(), 1, "exactly the matched series is returned");
        assert_eq!(soa[0].labels, labels("sparse_metric_2000"));
        assert_eq!(soa[0].timestamps.len(), 8);
        assert_eq!(soa[0].values.len(), 8);

        let probe = metrics.snapshot().get;
        // The probe path never GETs the whole object: its total bytes stay well
        // under the object size (catalog sections + one series' pages + the
        // 64 KiB footer suffix), where the whole-object fallback would move at
        // least `object_size` bytes in one GET.
        assert!(
            probe.bytes < object_size,
            "probe path moved {} B, must be under the {object_size} B object",
            probe.bytes
        );
        assert!(
            probe.bytes < object_size / 2,
            "probe path ({} B) should be far under half the object ({object_size} B)",
            probe.bytes
        );
    }

    /// An object that does not qualify for the probe path keeps the
    /// unchanged whole-object fallback. An empty matcher matches every series,
    /// so the fetcher takes the whole-object GET (one GET covering the object)
    /// rather than the catalog-probe path. Proven by the metered GET bytes
    /// reaching at least the object size.
    #[tokio::test]
    async fn sparse_segment_empty_matcher_keeps_whole_object_fallback() {
        let (bytes, tenant_hash, seg_ref) = write_sparse_test_segment(4096, 8).await;
        let object_size = seg_ref.object_size;

        let (fetcher, metrics) = metered_fetcher(&seg_ref.data_object_key, bytes.clone()).await;
        let (soa, _stats) = fetcher
            .fetch_soa(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch_soa whole-object fallback");

        assert_eq!(soa.len(), 4096, "an empty matcher returns every series");

        let get = metrics.snapshot().get;
        // The whole-object fallback fetches the entire object in one GET, so the
        // metered bytes include at least `object_size` (plus the earlier 64 KiB
        // footer suffix). The probe path would have stayed strictly under it.
        assert!(
            get.bytes >= object_size,
            "whole-object fallback moved {} B, must be at least the {object_size} B object",
            get.bytes
        );
    }

    // --- coalesce_ranges / FetchedRegions unit coverage.
    // docs/query-engine.md "coalesce adjacent byte ranges": merge within the
    // gap, split beyond it, never join unrelated regions, never overflow. The
    // fetcher's whole multi-GET plan reduces to these two helpers, so they are
    // pinned directly here (the end-to-end path is exercised in
    // tests/fetch_multi_get.rs).

    #[test]
    fn coalesce_merges_within_gap_and_splits_beyond() {
        // Unsorted input: (12,20) is within gap 5 of (0,10) -> one group
        // spanning (0,20); (1000,1010) is far beyond the gap -> its own group.
        // Proves both the merge and the "no unrelated join" direction.
        let out = coalesce_ranges(vec![(1000, 1010), (0, 10), (12, 20)], 5);
        assert_eq!(out, vec![(0, 20), (1000, 1010)]);
    }

    #[test]
    fn coalesce_merges_overlapping_and_keeps_max_end() {
        // Overlap (5 <= 10) merges; the wider range's end wins even when the
        // ranges are supplied shorter-last.
        let out = coalesce_ranges(vec![(0, 10), (5, 8), (8, 30)], 0);
        assert_eq!(out, vec![(0, 30)]);
    }

    #[test]
    fn coalesce_zero_gap_only_joins_touching_ranges() {
        // gap 0: exactly-adjacent ranges (end == next start) join; a 1-byte
        // hole splits them. This is the with_coalesce_gap(0) GET set: two GETs.
        let ranges = vec![(0, 10), (10, 20), (21, 30)];
        assert_eq!(coalesce_ranges(ranges.clone(), 0), vec![(0, 20), (21, 30)]);
        // Widening the gap to bridge the 1-byte hole collapses the same three
        // ranges to a single GET: coalescing reduces the GET set, never grows
        // it, and never joins ranges further apart than the gap.
        assert_eq!(coalesce_ranges(ranges, 1), vec![(0, 30)]);
    }

    #[test]
    fn coalesce_is_overflow_safe_near_u64_max() {
        // saturating_add on the gap must not panic (debug) or wrap (release)
        // when last.end + max_gap would exceed u64::MAX.
        let out = coalesce_ranges(vec![(0, 5), (u64::MAX - 2, u64::MAX)], u64::MAX);
        assert_eq!(out, vec![(0, u64::MAX)]);
        let out = coalesce_ranges(vec![(u64::MAX - 3, u64::MAX)], u64::MAX);
        assert_eq!(out, vec![(u64::MAX - 3, u64::MAX)]);
    }

    #[test]
    fn fetched_regions_slice_is_zero_copy_within_one_buffer() {
        let mut regions = FetchedRegions::default();
        let buf = Bytes::from(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let base = buf.as_ptr() as usize;
        // Buffer lives at absolute offset 100, spanning [100, 110).
        regions.insert(100, buf);

        assert!(regions.covers(102, 108));
        assert!(
            !regions.covers(95, 105),
            "must not claim to cover a left overhang"
        );
        assert!(
            !regions.covers(105, 115),
            "must not claim to cover a right overhang"
        );

        let s = regions.slice(103, 4).expect("sub-range is covered");
        assert_eq!(&s[..], &[3u8, 4, 5, 6]);
        // Zero-copy (ADR-0013, fetcher.rs FetchedRegions::slice comment): the
        // returned Bytes points into the original allocation at base + 3, not
        // a fresh copy. Compared as integers to avoid `unsafe` pointer math
        // (unsafe is denied workspace-wide).
        assert_eq!(s.as_ptr() as usize, base + 3);
        // Out-of-range slice is rejected, never a copy of the wrong bytes.
        assert!(regions.slice(108, 4).is_none());
    }

    /// A `FaultStore` whose only rule is a long run of pass-throughs on the
    /// object key, so `sequence_progress(0)` faithfully counts the GETs a
    /// fetch issued (the same GET-counting trick as tests/fetch_multi_get.rs).
    async fn counting_store(bytes: Bytes, key: &str) -> Arc<FaultStore<MemoryStore>> {
        let inner = MemoryStore::new();
        inner
            .put(key, bytes, PutOptions::default())
            .await
            .expect("put segment object");
        let mut seq = Sequence::new(Op::Get).with_key_contains(key);
        for _ in 0..64 {
            seq = seq.then_passthrough();
        }
        Arc::new(FaultStore::new(
            inner,
            FaultPlan::empty().with_sequence(seq),
        ))
    }

    /// Opening a segment once with `fetch_soa_and_histograms`
    /// returns byte-for-byte the same scalar and histogram series the two
    /// separate `fetch_soa` + `fetch_histograms` passes did, while issuing
    /// strictly fewer GETs (one segment open instead of two).
    #[tokio::test]
    async fn fetch_soa_and_histograms_matches_separate_passes_in_one_open() {
        let (mem, tenant_hash, seg_ref) = write_histogram_test_segment().await;
        let bytes = mem
            .get(&seg_ref.data_object_key, GetRange::Full)
            .await
            .expect("get bytes")
            .data;

        let ref_store = counting_store(bytes.clone(), &seg_ref.data_object_key).await;
        let ref_backend: Arc<dyn ObjectStoreBackend> = ref_store.clone();
        let ref_fetcher = SegmentFetcher::new(ref_backend);
        let (soa_ref, _s) = ref_fetcher
            .fetch_soa(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch_soa");
        let hist_ref = ref_fetcher
            .fetch_histograms(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch_histograms");
        let two_pass_gets = ref_store.sequence_progress(0);

        let comb_store = counting_store(bytes, &seg_ref.data_object_key).await;
        let comb_backend: Arc<dyn ObjectStoreBackend> = comb_store.clone();
        let (mut soa, _s2, mut hist) = SegmentFetcher::new(comb_backend)
            .fetch_soa_and_histograms(tenant_hash, &seg_ref, &[])
            .await
            .expect("combined fetch");
        let combined_gets = comb_store.sequence_progress(0);

        let mut soa_ref = soa_ref;
        soa.sort_by_key(|s| s.series_id.0);
        soa_ref.sort_by_key(|s| s.series_id.0);
        assert_eq!(soa.len(), soa_ref.len());
        for (x, y) in soa.iter().zip(soa_ref.iter()) {
            assert_eq!(x.series_id, y.series_id);
            assert_eq!(x.labels, y.labels);
            assert_eq!(x.timestamps, y.timestamps);
            let xv: Vec<u64> = x.values.iter().map(|v| v.to_bits()).collect();
            let yv: Vec<u64> = y.values.iter().map(|v| v.to_bits()).collect();
            assert_eq!(xv, yv);
        }

        let mut hist_ref = hist_ref;
        hist.sort_by_key(|s| s.series_id.0);
        hist_ref.sort_by_key(|s| s.series_id.0);
        assert_eq!(hist.len(), hist_ref.len());
        for (x, y) in hist.iter().zip(hist_ref.iter()) {
            assert_eq!(x.series_id, y.series_id);
            assert_eq!(x.labels, y.labels);
            assert_eq!(x.timestamps, y.timestamps);
            assert_eq!(x.values, y.values);
        }

        assert_eq!(
            two_pass_gets, 2,
            "the two separate passes open the segment twice"
        );
        assert_eq!(
            combined_gets, 1,
            "the merged pass opens the small segment exactly once"
        );
    }

    /// Below the whole-object threshold the first GET reads the
    /// entire object (one request, no footer probe) regardless of `suffix_len`;
    /// above it the footer-suffix path runs and a tiny suffix forces the
    /// `NeedRange` chase into more than one GET. Both decode identical data.
    #[tokio::test]
    async fn size_aware_first_get_reads_whole_small_object_else_footer_suffix() {
        let (mem, tenant_hash, seg_ref) = write_test_segment().await;
        let bytes = mem
            .get(&seg_ref.data_object_key, GetRange::Full)
            .await
            .expect("get bytes")
            .data;
        let size = seg_ref.object_size;

        // Below threshold: one whole-object GET even with a 16-byte suffix.
        let whole_store = counting_store(bytes.clone(), &seg_ref.data_object_key).await;
        let whole_backend: Arc<dyn ObjectStoreBackend> = whole_store.clone();
        let whole = SegmentFetcher::new(whole_backend)
            .with_whole_object_threshold(size + 1)
            .with_suffix_len(16)
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("whole-object fetch");
        assert_eq!(
            whole_store.sequence_progress(0),
            1,
            "below threshold the fetch reads the whole object in one GET"
        );

        // Above threshold (whole-object disabled) with a tiny suffix: the
        // footer NeedRange chase issues at least a second GET.
        let suffix_store = counting_store(bytes, &seg_ref.data_object_key).await;
        let suffix_backend: Arc<dyn ObjectStoreBackend> = suffix_store.clone();
        let suffixed = SegmentFetcher::new(suffix_backend)
            .with_whole_object_threshold(0)
            .with_suffix_len(16)
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("footer-suffix fetch");
        assert!(
            suffix_store.sequence_progress(0) >= 2,
            "above threshold the footer-suffix path issues a second probe, got {}",
            suffix_store.sequence_progress(0)
        );

        let mut a = whole;
        let mut b = suffixed;
        a.sort_by_key(|s| s.series_id.0);
        b.sort_by_key(|s| s.series_id.0);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.series_id, y.series_id);
            assert_eq!(x.samples.len(), y.samples.len());
            for (sx, sy) in x.samples.iter().zip(y.samples.iter()) {
                assert_eq!(sx.ts_ns, sy.ts_ns);
                assert_eq!(sx.value.to_bits(), sy.value.to_bits());
            }
        }
    }

    #[test]
    fn fetched_regions_does_not_cover_a_range_straddling_two_buffers() {
        // A sub-range that spans a buffer boundary is reported uncovered, so
        // the fetcher refetches it rather than stitching bytes from two GETs.
        let mut regions = FetchedRegions::default();
        regions.insert(0, Bytes::from(vec![0u8; 10]));
        regions.insert(10, Bytes::from(vec![1u8; 10]));
        assert!(regions.covers(2, 8));
        assert!(regions.covers(12, 18));
        assert!(!regions.covers(8, 12), "a straddling range is not covered");
        assert!(regions.slice(8, 4).is_none());
    }

    /// A `Layer` that records every `record()` call's field names, keyed by
    /// which span it landed on (by name), so a test can assert a field never
    /// lands on the wrong span.
    #[derive(Clone, Default)]
    struct RecordCollector {
        hits: std::sync::Arc<std::sync::Mutex<Vec<(String, &'static str)>>>,
    }

    impl<S> tracing_subscriber::Layer<S> for RecordCollector
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct FieldNames(Vec<&'static str>);
            impl tracing::field::Visit for FieldNames {
                fn record_u64(&mut self, field: &tracing::field::Field, _value: u64) {
                    self.0.push(field.name());
                }
                fn record_debug(
                    &mut self,
                    _field: &tracing::field::Field,
                    _value: &dyn std::fmt::Debug,
                ) {
                }
            }
            let mut names = FieldNames(Vec::new());
            values.record(&mut names);
            let span_name = ctx
                .span(id)
                .map(|s| s.name().to_string())
                .unwrap_or_default();
            if let Ok(mut hits) = self.hits.lock() {
                for name in names.0 {
                    hits.push((span_name.clone(), name));
                }
            }
        }
    }

    /// Discriminating regression test for the checkpoint-review bug: the
    /// phase functions (`open_segment`, `decode_selected`,
    /// `fetch_scalar_pages`, `build_scalar_decodes`, ...) used to record their
    /// per-phase `s3_requests`/`s3_bytes`/`series_matched`/
    /// `decompressed_bytes` fields via `tracing::Span::current()`. Every phase
    /// span is `debug`-level; at INFO (the production default) they are
    /// disabled, and entering a disabled span is a no-op that never changes
    /// what `Span::current()` returns. `Span::current()` would then resolve to
    /// the nearest *enabled* ancestor -- in production, the `sql_query`/
    /// `analytics_query` request-level span, which declares the identical
    /// field names -- and a phase's `record()` call would land there instead,
    /// visible to any subscriber that observes intermediate `on_record`
    /// events (a live/streaming trace exporter, not just a backend that reads
    /// the span once it closes).
    ///
    /// This installs an INFO-only filter (so every phase span here is
    /// disabled, matching production) with a "request_span" standing in for
    /// `sql_query`/`analytics_query`, entered around the fetch exactly as
    /// `services/ravel-server/src/sql.rs` enters its own request span. It
    /// asserts no `s3_requests`/`s3_bytes` field is ever recorded onto
    /// anything: the fix's `debug_span!`/`.instrument()` (or, for the sync
    /// decode functions, `span.enter()`) handles mean every phase's `record()`
    /// call targets its own (disabled, so silently dropped) span, never the
    /// ambient one. Before the fix, this test fails: the collector observes
    /// `s3_requests`/`s3_bytes` recorded on `"request_span"`.
    #[tokio::test]
    async fn phase_spans_never_record_onto_the_ambient_span_when_disabled() {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let collector = RecordCollector::default();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::INFO)
            .with(collector.clone());
        let _guard = subscriber.set_default();

        let (store, tenant_hash, seg_ref) = write_test_segment().await;
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let fetcher = SegmentFetcher::new(backend);

        // Stands in for `sql_query`/`analytics_query`: INFO level, so
        // enabled, declaring the same field names the phase spans do.
        let request_span = tracing::info_span!(
            "request_span",
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
        );
        async {
            fetcher
                .fetch_soa(tenant_hash, &seg_ref, &[])
                .await
                .expect("fetch_soa succeeds")
        }
        .instrument(request_span.clone())
        .await;

        let hits = collector.hits.lock().expect("lock").clone();
        let leaked: Vec<_> = hits
            .iter()
            .filter(|(_, field)| *field == "s3_requests" || *field == "s3_bytes")
            .collect();
        assert!(
            leaked.is_empty(),
            "a disabled phase span's s3_requests/s3_bytes record() call must \
             never land on any other span (production's request-level span \
             declares the same field names): got {leaked:?}"
        );
    }
}
