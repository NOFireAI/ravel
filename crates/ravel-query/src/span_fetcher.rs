//! `SpanSegmentFetcher`: a fetch-and-scan abstraction over one RSPAN span
//! segment (crate `ravel-rspan`, docs/span-segment-format.md; ADR-0041 phase 5,
//! ADR-0054 bloom pruning).
//!
//! This is the span-signal sibling of [`crate::log_fetcher::LogSegmentFetcher`].
//! It was promoted here from `ravel-sql` (`spans_fetcher.rs`) for the
//! distributed Spans fan-out (#285, ADR-0071 "log and span distributed fan-out"
//! amendment): the fragment worker lives in `ravel-query`, and `ravel-sql`
//! depends on `ravel-query`, so the worker could not use a fetcher that lived
//! in `ravel-sql`. The move preserves the fetcher's behavior byte-for-byte; the
//! `spans` SQL table (`ravel-sql`) now reaches it through a re-export
//! (`ravel_query::SpanSegmentFetcher`), so its scan path is unchanged.
//!
//! # Why this drives the block scan itself (ADR-0054)
//!
//! v1 delegated the whole per-object scan to [`RspanReader::scan`], which prunes
//! blocks by trace_id range and time interval and re-evaluates survivors
//! exactly. v3 adds a per-block BLOOM over `service.name`/`name` tokens
//! ([`SkipIndex::candidate_blocks_with_bloom`]), but the reader's
//! `scan` still only takes a [`SpanQuery`] (ts window + trace_id) and cannot
//! carry a bloom predicate. `ravel-rspan` deliberately exposes the primitives
//! for a caller to assemble a bloom-backed scan itself -- [`RspanReader::bloom`]
//! and [`RspanReader::skip_index`], `candidate_blocks_with_bloom`,
//! [`ravel_rspan::block::read_block`], and
//! [`ravel_rspan::block::DecodedBlock::service_name`] -- so this fetcher
//! assembles that scan here on top of the public format surface rather than
//! adding a bloom-aware `scan` to the reader.
//! The block-slicing bounds check mirrors the reader's own: a
//! `(block_offset, block_len)` decoded from SKIP_IDX is checked against the
//! BLOCKS section's own length, never merely the whole object, so a corrupt
//! offset can never return foreign bytes.
//!
//! Pruning stays widen-only (ADR-0013): the bloom's negative probe is a proof
//! the token is absent, so a skipped block truly held no matching row; a bloom
//! false positive only costs a wasted block decode, never a wrong or missing
//! row. The `spans` SQL provider's `supports_filters_pushdown` is `Inexact`, so
//! DataFusion re-applies the original `service_name`/`name` predicate above the
//! scan regardless.
//!
//! Object-level relevance (does the segment's ts span overlap the window?) is
//! still decided from the catalog summary before any GET.

use std::sync::Arc;

use crate::fetcher::{ReadCache, gate_not_run};
use crate::phase_accounting::{PhaseAccounting, QueryPhase};
use crate::reserved_bytes::attach_reservation;
use bytes::Bytes;
use ravel_cache::{CacheKey, ReadOutcome, SingleFlightError};
use ravel_catalog::SegmentRef;
use ravel_cpu_gate::{CpuGateError, JobSize, ReadGate, ReadSite};
use ravel_object_store::get_latency::InGetPhase;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_rspan::block::{DEFAULT_MAX_UNCOMP, DecodedBlock, read_block, read_block_projected};
use ravel_rspan::footer::{SectionDesc, kind};
use ravel_rspan::record::{
    COL_END_TS, COL_EVENT_ATTRS_BLOB, COL_EVENT_COUNT, COL_EVENT_NAME, COL_EVENT_TS, COL_START_TS,
    COL_TRACE_ID,
};
use ravel_rspan::skip_index::BlockEntry;
use ravel_rspan::varint::get_uvarint;
use ravel_rspan::{
    BloomPredicate, BloomSection, RspanConfig, RspanRangeReader, RspanReader, ScanStats, SpanQuery,
    SpanRecord, SpanSegError, SuffixOutcome, TraceBlockSpan, TraceKeyProbe, TraceKeyStep,
    decode_section, open, open_from_suffix,
};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};

/// One scanned span: the rebuilt record plus its `service_name` read straight
/// from the v3 dictionary-encoded `COL_SERVICE_NAME` column (ADR-0054), rather
/// than looked up by linear scan of the record's merged `attrs` map at build
/// time. `None` when the span carried no `service.name`. The record's `attrs`
/// still carry `service.name` (the reader re-inserts it), so the public
/// `spans.attrs` column is unchanged; this field is the direct read that backs
/// the dedicated `service_name` column.
///
/// `PartialEq`/`Eq` are derived (all fields are `Eq`: `SpanRecord` is, and
/// `service_name` is `Option<String>`), so the distributed fan-out's
/// differential tests can compare a decoded span multiset for exact equality.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanRow {
    pub record: SpanRecord,
    pub service_name: Option<String>,
}

/// The rows matching one fetch, plus the reader's own scan pruning counters.
/// [`ScanStats`] is what proves a prune fired: a bloom-backed or trace-keyed
/// scan reports strictly fewer `blocks_scanned` than a bare window scan over the
/// same object.
#[derive(Clone, Debug)]
pub struct SpanFetchOutput {
    pub records: Vec<SpanRow>,
    pub stats: ScanStats,
    /// Column pages this fetch's decode decompressed, summed over the blocks it
    /// scanned. The row exit decodes every page of every block it scans, so this
    /// is the whole page count of the scanned blocks and
    /// [`pages_skipped`](Self::pages_skipped) is 0. `ScanStats` carries no page
    /// counters (only block ones), so these two ride here, from the same
    /// per-block decode counts the page-byte accounting folds.
    pub pages_decoded: usize,
    /// Column pages this fetch's decode walked past because the projection
    /// excluded them. Always 0 for the row exit, which requests every column;
    /// it is a measured 0, not an absent counter, so a caller publishing it can
    /// state the row path skipped nothing rather than leaving a metric unwritten
    /// (#669).
    pub pages_skipped: usize,
    /// What the object's KEY_IDX probe pruned (ADR-2707 decision 5, tier 1).
    pub key_index: KeyIndexPruning,
}

/// What one object's tier-1 KEY_IDX probe pruned (ADR-2707 decision 5). Both
/// figures are zero when no probe ran (no `trace_id`, a caller-supplied block
/// set, no skip-index candidate) or when the probe fell back to SKIP_IDX alone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeyIndexPruning {
    /// 1 when the probe proved the trace absent, so no block of the object was
    /// read; else 0.
    pub segments_pruned: u64,
    /// SKIP_IDX candidate blocks the probe's block set removed, so they were
    /// neither read nor decoded.
    pub blocks_pruned: u64,
}

/// One candidate block resolved to its absolute, bounds-checked
/// `(start, end, crc32c)` byte range within the whole object, ready for the
/// cursor to re-slice without holding a borrow of the reader.
type CandidateBlock = (usize, usize, u32);

/// One candidate block located in a scan's resident buffers:
/// `(buffer, start, end, crc32c)`, with `start..end` relative to that buffer.
type BufferedBlock = (usize, usize, usize, u32);

/// The object tail the ranged trace path GETs first, before it knows where the
/// footer and sections start. Sized to cover the footer, SKIP_IDX and BLOOM of
/// a typical object in one request; a section the probe does not reach costs
/// one more ranged GET of exactly that section.
pub const DEFAULT_TRACE_PROBE_BYTES: u64 = 64 << 10;

/// Which read a span fetch issues for one object. A query carrying a
/// `trace_id` equality takes [`RangedTrace`](Self::RangedTrace): a tail probe
/// for the footer and sections, then one ranged GET per contiguous block run
/// holding the trace. Every other query reads the whole object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanReadPath {
    WholeObject,
    RangedTrace,
}

impl SpanReadPath {
    /// The path [`SpanSegmentFetcher`] takes for `query`.
    #[must_use]
    pub fn for_query(query: &SpanQuery) -> Self {
        if query.trace_id.is_some() {
            SpanReadPath::RangedTrace
        } else {
            SpanReadPath::WholeObject
        }
    }

    /// The label for the read path, exposed for the explain output; wiring
    /// it into `SpansScanExec`'s explain is #2832.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SpanReadPath::WholeObject => "whole-object",
            SpanReadPath::RangedTrace => "ranged-trace",
        }
    }
}

impl std::fmt::Display for SpanReadPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One decoded block handed out by the columnar exit
/// ([`SpanSegmentFetcher::fetch_accounted_columnar`], ADR-0110 decision 2),
/// plus the indices of its rows that survived the query's ts window and
/// optional `trace_id` equality.
///
/// The block was decoded under the caller's projection (unioned with the
/// predicate columns [`COL_TRACE_ID`]/[`COL_START_TS`]/[`COL_END_TS`] so the
/// surviving rows can be evaluated), so pages outside that set were never
/// decompressed. Read its columns through [`DecodedBlock::view`] and gather them
/// over `rows`; a column the projection excluded answers
/// [`ravel_rspan::SpanSegError::ColumnNotRequested`] rather than a silent column
/// of nulls.
///
/// `rows` is ascending and is exactly the set the row exit's `SpanRow` results
/// cover for the same block, computed by the one shared predicate
/// ([`surviving_rows`]).
pub struct ColumnarBlock {
    pub block: DecodedBlock,
    pub rows: Vec<usize>,
}

/// Shape only: [`DecodedBlock`] is not `Debug` (its columns are deliberately not
/// formattable), and formatting the surviving rows' cells would defeat a view
/// that exists to avoid materializing them.
impl std::fmt::Debug for ColumnarBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarBlock")
            .field("record_count", &self.block.record_count())
            .field("pages_decoded", &self.block.pages_decoded())
            .field("pages_skipped", &self.block.pages_skipped())
            .field("surviving_rows", &self.rows.len())
            .finish()
    }
}

/// Errors fetching and decoding one RSPAN segment. Every variant is a hard
/// error: the caller never receives partial or silently-wrong data. Mirrors
/// [`crate::LogFetchError`] so `ravel-sql`'s `SqlError` can redact it the same
/// way (the `Display` embeds the object key, logged server-side only).
#[derive(Debug, thiserror::Error)]
pub enum SpanFetchError {
    #[error("object store error reading span segment {key}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("corrupt span segment {key}: {source}")]
    Corrupt {
        key: String,
        #[source]
        source: SpanSegError,
    },
    #[error("span segment {key} belongs to a different tenant than the query")]
    TenantMismatch { key: String },
    /// Mirrors [`crate::FetchError::UnsupportedVersion`]: the trailer declares
    /// a version outside this build's reader window and the object's bytes
    /// match the content hash its catalog entry recorded.
    #[error("{} (span segment {key})", crate::fetcher::unsupported_version_message(*version))]
    UnsupportedVersion { key: String, version: u32 },
    /// The fetch-layer memory budget (ADR-1170 decision 2) refused the
    /// reservation for the bytes this GET would materialize. Carries only the
    /// budget's figures and the class of the refusal (`cause`), never a key
    /// or tenant value. Mirrors [`crate::FetchError::FetchMemoryExhausted`],
    /// `Display` included.
    #[error(
        "fetch {}",
        crate::fetcher::memory_exhausted(.requested, .reserved, .limit, .cause)
    )]
    FetchMemoryExhausted {
        requested: u64,
        reserved: u64,
        limit: u64,
        cause: ravel_memory::ExhaustionCause,
    },
}

/// Fetches and scans one RSPAN span segment at a time. Constructed with the
/// same [`ObjectStoreBackend`] trait object the log/metric fetchers take.
#[derive(Clone)]
pub struct SpanSegmentFetcher {
    store: Arc<dyn ObjectStoreBackend>,
    cfg: RspanConfig,
    /// ADR-0046's read cache, consulted by
    /// [`whole_object_bytes`](Self::whole_object_bytes) on behalf of
    /// [`fetch_accounted`](Self::fetch_accounted) and
    /// [`fetch_accounted_columnar`](Self::fetch_accounted_columnar) -- the only
    /// funnels here that carry the `tenant_hash` a cache key needs.
    /// [`fetch`](Self::fetch) is unaccounted and tenant-blind (test/block-stat
    /// spy only) and never consults it, unchanged by its presence. Mirrors
    /// [`LogSegmentFetcher::cache`](crate::log_fetcher::LogSegmentFetcher);
    /// either tier configuration (see [`ReadCache`]) works, and production
    /// builds the RAM or RAM-over-disk variant the same way the metric and log
    /// fetchers do.
    cache: Option<ReadCache>,
    /// Bounds in-flight object-store GETs (ADR-1195). This is this fetcher's
    /// FIRST-EVER concurrency bound: before ADR-1195, `SpanSegmentFetcher` held
    /// no semaphore at all and its GET concurrency was limited only by its
    /// caller. `new` gives it a private limiter at
    /// [`crate::fetcher::DEFAULT_MAX_CONCURRENT_GETS`]; [`Self::with_get_limiter`]
    /// wires it to the one process-shared limiter every query-side fetcher can
    /// hold instead.
    get_limiter: Arc<crate::GetLimiter>,
    /// The process-wide fetch memory budget (ADR-1170 decision 2). Each
    /// whole-object GET reserves the object's size against it before the GET,
    /// and the reservation travels with the returned bytes. Default unlimited
    /// (never refuses); [`Self::with_memory_budget`] wires the shared one,
    /// mirroring [`Self::with_get_limiter`].
    memory_budget: Arc<ravel_memory::MemoryBudget>,
    /// The read CPU gate the row exits' block decodes run on (ADR-1702
    /// decision 4). `None`, the default, decodes inline.
    read_gate: Option<Arc<ReadGate>>,
    /// Length of the tail probe the ranged trace path reads first
    /// ([`DEFAULT_TRACE_PROBE_BYTES`] unless overridden).
    trace_probe_bytes: u64,
}

impl SpanSegmentFetcher {
    pub fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        SpanSegmentFetcher {
            store,
            cfg: RspanConfig::default(),
            cache: None,
            get_limiter: Arc::new(crate::GetLimiter::new_unchecked(
                crate::fetcher::DEFAULT_MAX_CONCURRENT_GETS,
            )),
            memory_budget: Arc::new(ravel_memory::MemoryBudget::unlimited()),
            read_gate: None,
            trace_probe_bytes: DEFAULT_TRACE_PROBE_BYTES,
        }
    }

    /// Overrides the tail probe length of the ranged trace path. Values below
    /// the 64-byte floor are raised to it, so the probe always covers the
    /// trailer.
    #[must_use]
    pub fn with_trace_probe_bytes(mut self, n: u64) -> Self {
        self.trace_probe_bytes = n.max(64);
        self
    }

    /// Runs the block decodes of [`fetch`](Self::fetch) and
    /// [`fetch_accounted`](Self::fetch_accounted), and of a columnar scan
    /// drained through [`SpanColumnarScan::next_block_on_gate`], on `gate`
    /// (ADR-1702 decision 4). Each block, with all its pages, is one job.
    #[must_use]
    pub fn with_read_gate(mut self, gate: Arc<ReadGate>) -> Self {
        self.read_gate = Some(gate);
        self
    }

    /// Wires this fetcher to a caller-owned [`ravel_memory::MemoryBudget`]
    /// (ADR-1170 decision 2), so its whole-object GETs reserve against the same
    /// budget as every other fetcher (and, via
    /// [`crate::QueryEngine::with_memory_budget`], every other engine) holding
    /// the same `Arc`. Mirrors [`Self::with_get_limiter`].
    #[must_use]
    pub fn with_memory_budget(mut self, budget: Arc<ravel_memory::MemoryBudget>) -> Self {
        self.memory_budget = budget;
        self
    }

    /// This fetcher's current memory budget, for a test to `Arc::ptr_eq`.
    #[cfg(test)]
    pub(crate) fn memory_budget_for_test(&self) -> &Arc<ravel_memory::MemoryBudget> {
        &self.memory_budget
    }

    /// Reserves `n` bytes against this fetcher's budget before a whole-object
    /// GET, mapping a refusal to [`SpanFetchError::FetchMemoryExhausted`]. The
    /// guard is owned for the fetched buffer's lifetime (ADR-1170 decision 2).
    fn reserve_fetch(&self, n: u64) -> Result<ravel_memory::Reservation, SpanFetchError> {
        self.memory_budget
            .reserve(n)
            .map_err(|e| SpanFetchError::FetchMemoryExhausted {
                requested: e.requested,
                reserved: e.reserved,
                limit: e.limit,
                cause: e.cause,
            })
    }

    /// Sets the in-flight GET bound by building a new private limiter. Shared
    /// across this fetcher's clones; not shared with any other fetcher unless
    /// [`Self::with_get_limiter`] is used instead.
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

    /// This fetcher's `GetLimiter` permit count (ADR-1195): the process-wide
    /// GET concurrency bound, shared with every other fetcher and engine that
    /// took the same `Arc` via [`Self::with_get_limiter`]. Mirrors
    /// [`SegmentFetcher::get_limiter_permits`](crate::fetcher::SegmentFetcher::get_limiter_permits).
    #[must_use]
    pub fn get_limiter_permits(&self) -> usize {
        self.get_limiter.permits()
    }

    /// Overrides the [`RspanConfig`] used for section-size caps when decoding.
    #[must_use]
    pub fn with_config(mut self, cfg: RspanConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// Wires ADR-0046's read cache into
    /// [`fetch_accounted`](Self::fetch_accounted) and
    /// [`fetch_accounted_columnar`](Self::fetch_accounted_columnar), mirroring
    /// [`LogSegmentFetcher::with_cache`](crate::log_fetcher::LogSegmentFetcher::with_cache).
    /// Accepts either tier configuration through [`ReadCache`] (an
    /// `Arc<Cache<..>>` or an `Arc<TieredCache<..>>`, both convert via
    /// [`From`]).
    #[must_use]
    pub fn with_cache(mut self, cache: impl Into<ReadCache>) -> Self {
        self.cache = Some(cache.into());
        self
    }

    /// Whether this fetcher was built with [`with_cache`](Self::with_cache).
    /// Exposed for tests and callers that need to know without a probe read
    /// (mirrors [`LogSegmentFetcher::has_cache`](crate::log_fetcher::LogSegmentFetcher::has_cache)).
    #[must_use]
    pub fn has_cache(&self) -> bool {
        self.cache.is_some()
    }

    /// Per-object relevance from the catalog summary alone, with no object
    /// read: true iff the segment's event-ts span
    /// (`min_event_ts_ns..=max_event_ts_ns`) overlaps the inclusive query
    /// window. A `false` return lets [`fetch`] skip the object without a GET.
    ///
    /// A span object's summary ts span is `[min_start_ts, max_end_ts]` (the
    /// footer's whole-object interval), so this is the same interval-overlap
    /// test the block-level skip index applies, just at object granularity.
    ///
    /// [`fetch`]: Self::fetch
    #[must_use]
    pub fn ts_range_relevant(seg_ref: &SegmentRef, ts_min_ns: i64, ts_max_ns: i64) -> bool {
        seg_ref.min_event_ts_ns <= ts_max_ns && ts_min_ns <= seg_ref.max_event_ts_ns
    }

    /// Fetches, prunes, and scans one segment for spans matching `query` and
    /// `predicates`.
    ///
    /// The ts-range relevance pre-check runs first, from the catalog summary
    /// only: an object whose span cannot overlap the window returns `Ok(None)`
    /// with no GET. A query carrying a `trace_id` then takes the ranged read
    /// ([`SpanReadPath::RangedTrace`], described on
    /// [`fetch_accounted_in_blocks`](Self::fetch_accounted_in_blocks)) and
    /// never issues a [`GetRange::Full`]. Every other query fetches the whole
    /// object once ([`GetRange::Full`]) and scans it block by block:
    ///
    /// - candidate blocks are chosen by the skip index's trace_id/ts prune,
    ///   the `duration_ns`/`status_mask` skip-index prune when a duration or
    ///   status filter was pushed, and, when `predicates` is
    ///   non-empty, the bloom-backed
    ///   [`SkipIndex::candidate_blocks_with_bloom`] prune (a block whose bloom
    ///   proves a predicate's token absent is dropped before decode);
    /// - each surviving block is crc-verified and decoded, its rows
    ///   re-evaluated exactly against the ts window and (when set) trace_id, and
    ///   its `service_name` read straight from the v3 dictionary column.
    ///
    /// `duration_ns` and `status_mask` are the widen-only skip-index prune
    /// shapes the `spans` SQL pushdown extracts (`SpansPushdown::duration_window`
    /// and `status_mask`): `duration_ns` is an inclusive `[min, max]` window a
    /// block's `[min_duration_ns, max_duration_ns]` must overlap, `status_mask`
    /// a bitmask a block's `status_mask` must share a bit with. Both `None`
    /// means unconstrained on that axis. They can only skip blocks that cannot
    /// hold a matching row, so results are unchanged; only the read set shrinks.
    ///
    /// Every returned row satisfies `query` exactly. The bloom prune is
    /// widen-only (ADR-0013): a bloom false positive costs one wasted block
    /// decode, never a wrong or missing row, and DataFusion re-applies the
    /// `service_name`/`name` predicate above the scan. `stats` reports how much
    /// the skip index and bloom pruned.
    ///
    /// [`SkipIndex::candidate_blocks_with_bloom`]:
    /// ravel_rspan::SkipIndex::candidate_blocks_with_bloom
    pub async fn fetch(
        &self,
        seg_ref: &SegmentRef,
        query: &SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
    ) -> Result<Option<SpanFetchOutput>, SpanFetchError> {
        if !Self::ts_range_relevant(seg_ref, query.ts_min, query.ts_max) {
            return Ok(None);
        }
        if SpanReadPath::for_query(query) == SpanReadPath::RangedTrace {
            // Tenant-blind and uncached, like the whole-object read below.
            let scan = self
                .ranged_trace_scan(
                    seg_ref,
                    None,
                    *query,
                    duration_ns,
                    status_mask,
                    predicates,
                    None,
                    BlockProjection::All,
                    &PhaseAccounting::new(),
                )
                .await?;
            return Ok(Some(drain_rows(scan).await?));
        }

        let key = &seg_ref.data_object_key;
        // Reserve the whole object's bytes before the GET (ADR-1170 decision 2):
        // a refusal fails typed with zero GETs. Held to the end of this call,
        // covering the decode below; released when the fully-decoded rows are
        // returned.
        let _reservation = self.reserve_fetch(seg_ref.object_size)?;
        // The permit covers the GET only; decode below runs without it, as on
        // every other funnel.
        let got = async {
            let _permit = self
                .get_limiter
                .acquire()
                .await
                .map_err(|_| SpanFetchError::Store {
                    key: key.to_string(),
                    source: StoreError::Transient(
                        "GetLimiter semaphore closed unexpectedly".to_string(),
                    ),
                })?;
            self.store
                .get(key, GetRange::Full)
                .in_get_phase(QueryPhase::Scan.into())
                .await
                .map_err(|source| SpanFetchError::Store {
                    key: key.to_string(),
                    source,
                })
        }
        .await?;

        // Unaccounted entry point: the page-byte fold lands in a throwaway
        // handle, exactly as the logs side routes `fetch` through
        // `fetch_accounted(&QueryAccounting::new())`. The row output is
        // unchanged.
        let scan = self
            .open_scan(
                got.data.clone(),
                key.to_string(),
                *query,
                duration_ns,
                status_mask,
                predicates,
                BlockProjection::All,
                QueryAccounting::new(),
            )
            .map_err(|err| settle_version_refusal(seg_ref, &got.data, err))?;
        Ok(Some(drain_rows(scan).await?))
    }

    /// Accounted, tenant-checked counterpart of [`fetch`](Self::fetch):
    /// identical prune-and-scan behavior, plus two things (ADR-0044). The
    /// object read is recorded against `accounting` at this funnel -- the
    /// funnel the span scan did not account before -- exactly as
    /// `LogSegmentFetcher::fetch_accounted` records the log read (cache hit or
    /// store GET, via [`whole_object_bytes`](Self::whole_object_bytes)); and
    /// the fetched object's footer `tenant_hash` is verified against
    /// `tenant_hash` before it is decoded, failing closed with
    /// [`SpanFetchError::TenantMismatch`].
    ///
    /// `tenant_hash` mirrors the logs scan chain, which threads a
    /// `TenantHash` from `LogsTableProvider` through `LogsScanExec` into
    /// `LogSegmentFetcher::fetch_accounted_with_tenant`. Here it does double
    /// duty: it keys the ADR-0046 read cache (when [`with_cache`](Self::with_cache)
    /// configured one), exactly as the logs/metrics funnels key theirs, and it
    /// guards against decoding an object that belongs to another tenant. The
    /// read is recorded before the guard runs: the request was really issued
    /// regardless of whose object came back.
    ///
    /// This is the funnel a production `spans` query takes once a caller is
    /// wired in (ADR-0045 decision 5, phase 2); the unaccounted, tenant-blind
    /// [`fetch`](Self::fetch) stays for unit tests and block-stat spies.
    ///
    /// A query carrying a `trace_id` takes the ranged trace path
    /// ([`SpanReadPath::RangedTrace`]): see
    /// [`fetch_accounted_in_blocks`](Self::fetch_accounted_in_blocks), which
    /// this calls with no block set.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_accounted(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
        accounting: &QueryAccounting,
    ) -> Result<Option<SpanFetchOutput>, SpanFetchError> {
        self.fetch_accounted_in_blocks(
            seg_ref,
            tenant_hash,
            query,
            duration_ns,
            status_mask,
            predicates,
            None,
            &PhaseAccounting::pooled_over(accounting),
        )
        .await
    }

    /// [`fetch_accounted`](Self::fetch_accounted) with an optional set of
    /// block ordinals already known to hold `query.trace_id` (ADR-2707
    /// decision 7, a key-index entry's block set).
    ///
    /// With a `trace_id`, the object is never read through `GetRange::Full`.
    /// One ranged GET of the object's tail ([`DEFAULT_TRACE_PROBE_BYTES`];
    /// an object no larger than that is read in one ranged GET of its full
    /// length) yields the footer and, when the probe reaches them, SKIP_IDX
    /// and BLOOM; a section it does not reach is one ranged GET of exactly
    /// that section. The block run comes from
    /// [`RspanRangeReader::trace_block_span`] when `blocks` is `None`, or,
    /// otherwise, from one [`RspanRangeReader::block_run_span`] per
    /// contiguous run of the intersection of `blocks` with this object's
    /// skip-index candidates, and each run is one ranged GET (served from the
    /// probe when the probe already holds it). The `None` run is read whole,
    /// pruned blocks inside it included, because it is bounded by one trace's
    /// blocks and one GET beats two for the common adjacent case; a caller's
    /// set may span the object, so it is split on the intersection instead.
    /// Only the run's blocks that
    /// pass the same skip-index and bloom prune the whole-object path applies
    /// are decoded, through the same per-block decode that accumulates page
    /// bytes. On a well-formed object, with `blocks` `None` the rows equal the
    /// whole-object path's, and so do `stats` except that `blocks_after_skip`
    /// leaves out the blocks the KEY_IDX probe below pruned; with a block set
    /// the decoded set is the intersection, so rows and `blocks_after_skip`
    /// are those of the set. The wire bytes and request count are what
    /// differ from the whole-object path. One check differs too: a lookup
    /// with no pushed predicate does not read BLOOM, so a missing or corrupt
    /// BLOOM section that the whole-object path refuses at open (its
    /// `RspanReader::new` reads BLOOM unconditionally) goes unnoticed here;
    /// the bloom only ever widens a candidate set, so no row is wrong, and
    /// any other query on the object still reports the corruption. A failed
    /// ranged GET is a typed [`SpanFetchError::Store`], never a fallback to a
    /// whole-object read.
    ///
    /// With `blocks` `None` and at least one candidate, the object's KEY_IDX
    /// section is probed first (ADR-2707 decision 5): its `[0, prefix_len)`
    /// and the one bucket the trace mixes to, the bucket skipped when the
    /// directory says it is empty, each sliced from the tail probe when the
    /// probe holds it and otherwise one ranged GET on `accounting.key_index()`.
    /// An absent trace reads no block and counts the object in
    /// [`SpanFetchOutput::key_index`]; a present one keeps the candidates the
    /// index names, each adjacent run read from its first kept candidate to
    /// its last. A corrupt or declining section is logged with the object key
    /// and prunes nothing, so the rows are SKIP_IDX's; a failed probe GET is a
    /// [`SpanFetchError::Store`] like any other.
    ///
    /// With a read cache, every ranged read is keyed
    /// `(tenant_hash, content_hash, offset, len)`, so a repeated lookup is
    /// served from the cache with no GET. Each read is recorded on the phase
    /// that issued it, as one request plus the wire bytes it returned, or as
    /// a cache hit or miss: the tail probe, the footer chase and the
    /// SKIP_IDX/BLOOM section reads on `accounting.probe()`, the phase the
    /// RLOG data read charges the same GET kinds to (`ReadPhases::SCAN`), each
    /// block-run GET on `accounting.scan()`. A run sliced out of the probe
    /// issues no GET of its own. A query without a `trace_id` reads the whole object on
    /// `accounting.scan()`. [`fetch_accounted`](Self::fetch_accounted) passes
    /// [`PhaseAccounting::pooled_over`] its one handle, so every phase lands
    /// on that handle.
    ///
    /// `blocks` without a `trace_id` is a caller error, reported as
    /// [`SpanFetchError::Corrupt`] before any GET; a block ordinal at or past
    /// this object's block count is `Corrupt` before any run is read, whether
    /// or not the object has a candidate block; an empty set reads nothing.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_accounted_in_blocks(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
        blocks: Option<&[u32]>,
        accounting: &PhaseAccounting,
    ) -> Result<Option<SpanFetchOutput>, SpanFetchError> {
        let scan = self
            .open_accounted_scan(
                seg_ref,
                tenant_hash,
                query,
                duration_ns,
                status_mask,
                predicates,
                blocks,
                BlockProjection::All,
                accounting,
            )
            .await?;
        match scan {
            Some(scan) => Ok(Some(drain_rows(scan).await?)),
            None => Ok(None),
        }
    }

    /// Columnar sibling of [`fetch_accounted`](Self::fetch_accounted) (ADR-0110
    /// decision 2): identical fetch, tenant guard, candidate selection, bloom
    /// probe, and ts/`trace_id` filtering, but instead of a `Vec<SpanRow>` it
    /// returns a [`SpanColumnarScan`] the caller drains one [`ColumnarBlock`] at
    /// a time. Each block is decoded under `projected_columns` (unioned with the
    /// predicate columns [`COL_TRACE_ID`]/[`COL_START_TS`]/[`COL_END_TS`] so the
    /// surviving rows can be evaluated), so pages for columns outside that set
    /// are never decompressed.
    ///
    /// The row and columnar exits run over the same primitive
    /// ([`open_scan`](Self::open_scan)), so a block's surviving rows are
    /// identical across the two; the only difference a caller observes is the
    /// decoded shape and, in `accounting`, `page_bytes_decoded`.
    ///
    /// Accounting (ADR-0107 decision 4, contract unchanged): the whole-object
    /// GET is recorded here exactly as the row exit records it, so
    /// `page_bytes_fetched` is identical to the row exit's for the same query;
    /// `page_bytes_decoded` is lower whenever `projected_columns` excludes a
    /// page the block carries (an attribute or event page). The fold is done by
    /// [`SpanColumnarScan`], once, on exhaustion or drop, so a scan abandoned
    /// after a satisfied `LIMIT` still accounts the blocks it decoded.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_accounted_columnar(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
        projected_columns: &[u32],
        accounting: &QueryAccounting,
    ) -> Result<Option<SpanColumnarScan>, SpanFetchError> {
        self.fetch_accounted_columnar_in_blocks(
            seg_ref,
            tenant_hash,
            query,
            duration_ns,
            status_mask,
            predicates,
            projected_columns,
            None,
            &PhaseAccounting::pooled_over(accounting),
        )
        .await
    }

    /// Columnar sibling of
    /// [`fetch_accounted_in_blocks`](Self::fetch_accounted_in_blocks): the same
    /// ranged trace read for a `trace_id` query, with or without a known block
    /// set, handing out a [`SpanColumnarScan`].
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_accounted_columnar_in_blocks(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
        projected_columns: &[u32],
        blocks: Option<&[u32]>,
        accounting: &PhaseAccounting,
    ) -> Result<Option<SpanColumnarScan>, SpanFetchError> {
        self.open_accounted_scan(
            seg_ref,
            tenant_hash,
            query,
            duration_ns,
            status_mask,
            predicates,
            blocks,
            BlockProjection::columnar(projected_columns),
            accounting,
        )
        .await
    }

    /// The accounted, tenant-checked funnel both exits share: the catalog ts
    /// pre-check, then the ranged trace read for a `trace_id` query or the
    /// whole-object read otherwise.
    #[allow(clippy::too_many_arguments)]
    async fn open_accounted_scan(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
        blocks: Option<&[u32]>,
        projection: BlockProjection,
        accounting: &PhaseAccounting,
    ) -> Result<Option<SpanColumnarScan>, SpanFetchError> {
        let key = &seg_ref.data_object_key;
        if blocks.is_some() && query.trace_id.is_none() {
            return Err(corrupt(
                key,
                SpanSegError::Corrupted("a block set needs a trace_id".into()),
            ));
        }
        if !Self::ts_range_relevant(seg_ref, query.ts_min, query.ts_max) {
            return Ok(None);
        }
        if SpanReadPath::for_query(query) == SpanReadPath::RangedTrace {
            let scan = self
                .ranged_trace_scan(
                    seg_ref,
                    Some(tenant_hash),
                    *query,
                    duration_ns,
                    status_mask,
                    predicates,
                    blocks,
                    projection,
                    accounting,
                )
                .await?;
            return Ok(Some(scan));
        }

        // One whole-object read for either exit (cache hit or store GET): the
        // columnar exit skips decode, not fetch, so `page_bytes_fetched` cannot
        // diverge between the two. It fetches block data, so it is a scan read.
        let accounting = accounting.scan();
        let bytes = self
            .whole_object_bytes(seg_ref, tenant_hash, accounting)
            .in_get_phase(QueryPhase::Scan.into())
            .await?;

        // Tenant identity guard: an object whose footer names a different
        // tenant is never decoded. The footer trailer is parsed here (cheap);
        // `open_scan` re-opens it for the BLOCKS section descriptor.
        let footer = open(&bytes)
            .map_err(|source| settle_version_refusal(seg_ref, &bytes, corrupt(key, source)))?;
        if footer.tenant_hash != tenant_hash.0 {
            return Err(SpanFetchError::TenantMismatch {
                key: key.to_string(),
            });
        }

        let scan = self.open_scan(
            bytes,
            key.to_string(),
            *query,
            duration_ns,
            status_mask,
            predicates,
            projection,
            accounting.clone(),
        )?;
        Ok(Some(scan))
    }

    /// The ranged trace read behind every `trace_id` query (see
    /// [`fetch_accounted_in_blocks`](Self::fetch_accounted_in_blocks)).
    /// `tenant_hash` is `None` only for the unaccounted, tenant-blind
    /// [`fetch`](Self::fetch), which neither checks the footer tenant nor
    /// consults the cache.
    #[allow(clippy::too_many_arguments)]
    async fn ranged_trace_scan(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: Option<TenantHash>,
        query: SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
        blocks: Option<&[u32]>,
        projection: BlockProjection,
        accounting: &PhaseAccounting,
    ) -> Result<SpanColumnarScan, SpanFetchError> {
        let key = &seg_ref.data_object_key;
        // The tail probe, the footer chase and the SKIP_IDX/BLOOM sections are
        // the metadata reads of a data read, as on the RLOG path; only the block
        // runs are data.
        let metadata = accounting.probe();
        let scan = accounting.scan();
        let bad = |source: SpanSegError| corrupt(key, source);
        let trace_id = query.trace_id.ok_or_else(|| {
            bad(SpanSegError::Corrupted(
                "ranged read needs a trace_id".into(),
            ))
        })?;
        let size = seg_ref.object_size;

        let probe_len = self.trace_probe_bytes.min(size);
        let probe = Extent {
            offset: size - probe_len,
            bytes: self
                .ranged_read(seg_ref, tenant_hash, size - probe_len, probe_len, metadata)
                .in_get_phase(QueryPhase::Probe.into())
                .await?,
        };
        let footer = match open_from_suffix(&probe.bytes, size).map_err(bad)? {
            SuffixOutcome::Ready(footer) => footer,
            SuffixOutcome::NeedRange { offset, len } => {
                let tail = self
                    .ranged_read(seg_ref, tenant_hash, offset, len, metadata)
                    .in_get_phase(QueryPhase::Probe.into())
                    .await?;
                match open_from_suffix(&tail, size).map_err(bad)? {
                    SuffixOutcome::Ready(footer) => footer,
                    SuffixOutcome::NeedRange { .. } => {
                        return Err(bad(SpanSegError::Corrupted(
                            "footer range did not cover the footer".into(),
                        )));
                    }
                }
            }
        };
        if let Some(tenant_hash) = tenant_hash
            && footer.tenant_hash != tenant_hash.0
        {
            return Err(SpanFetchError::TenantMismatch {
                key: key.to_string(),
            });
        }

        let skip_desc = footer
            .section(kind::SKIP_IDX)
            .ok_or_else(|| bad(SpanSegError::Corrupted("missing SKIP_IDX section".into())))?;
        let skip_stored = self
            .section_bytes(seg_ref, tenant_hash, &probe, skip_desc, metadata)
            .in_get_phase(QueryPhase::Probe.into())
            .await?;
        let skip_raw =
            decode_section(&skip_stored, skip_desc, self.cfg.max_uncomp_section).map_err(bad)?;
        let reader = RspanRangeReader::from_sections(&footer, &skip_raw).map_err(bad)?;
        let skip = reader.skip_index();
        let blocks_total = skip.blocks.len() as u32;

        let candidate_idx = if query.ts_min > query.ts_max {
            Vec::new()
        } else if predicates.is_empty() {
            skip.candidate_blocks(
                Some(&trace_id),
                query.ts_min,
                query.ts_max,
                duration_ns,
                status_mask,
            )
        } else {
            let bloom_desc = footer
                .section(kind::BLOOM)
                .ok_or_else(|| bad(SpanSegError::Corrupted("missing BLOOM section".into())))?;
            let bloom_stored = self
                .section_bytes(seg_ref, tenant_hash, &probe, bloom_desc, metadata)
                .in_get_phase(QueryPhase::Probe.into())
                .await?;
            let bloom_raw = decode_section(&bloom_stored, bloom_desc, self.cfg.max_uncomp_section)
                .map_err(bad)?;
            let bloom = BloomSection::parse(&bloom_raw).map_err(|e| bad(SpanSegError::from(e)))?;
            skip.candidate_blocks_with_bloom(
                Some(&trace_id),
                query.ts_min,
                query.ts_max,
                duration_ns,
                status_mask,
                &bloom,
                predicates,
            )
            .map_err(bad)?
        };

        // A caller's block set is validated against this object before any
        // candidate test, so an ordinal past the block count is `Corrupt`
        // whether or not the object has a candidate block.
        if let Some(set) = blocks
            && let Some(outside) = set.iter().find(|b| **b >= blocks_total)
        {
            return Err(bad(SpanSegError::Corrupted(format!(
                "block {outside} is outside the object's {blocks_total} blocks"
            ))));
        }
        // Tier 1 (ADR-2707 decision 5): with no caller set and a candidate to
        // read, the object's own KEY_IDX names the blocks holding the trace.
        let mut key_index = KeyIndexPruning::default();
        let probed: Option<Vec<u32>> = if blocks.is_none() && !candidate_idx.is_empty() {
            match self
                .probe_key_index(
                    seg_ref,
                    tenant_hash,
                    &probe,
                    &reader,
                    &trace_id,
                    accounting.key_index(),
                )
                .await?
            {
                // An absent trace counts its object, not its blocks.
                Some(TraceKeyProbe::Absent) => {
                    key_index.segments_pruned = 1;
                    Some(Vec::new())
                }
                Some(TraceKeyProbe::Present(mut set)) => {
                    set.sort_unstable();
                    set.dedup();
                    key_index.blocks_pruned = candidates_outside(&candidate_idx, &set);
                    Some(set)
                }
                // Unreachable today: `check_parameters` rejects a section
                // that does not index `trace_id` as Id16 before `locate`.
                Some(TraceKeyProbe::Declined) => {
                    tracing::warn!(
                        object = %key,
                        "KEY_IDX declined the trace_id lookup; pruning by SKIP_IDX alone"
                    );
                    None
                }
                None => None,
            }
        } else {
            None
        };
        let runs: Vec<TraceBlockSpan> = if candidate_idx.is_empty() {
            Vec::new()
        } else {
            match (blocks, probed.as_deref()) {
                (None, None) => reader
                    .trace_block_span(&trace_id)
                    .map_err(bad)?
                    .into_iter()
                    .collect(),
                // The run is split over the intersection of the caller's set
                // with this object's candidates, so a non-candidate block
                // between two candidates never widens a GET.
                (Some(set), _) => {
                    let wanted: Vec<u32> = set
                        .iter()
                        .copied()
                        .filter(|b| candidate_idx.binary_search(&(*b as usize)).is_ok())
                        .collect();
                    contiguous_runs(&wanted)
                        .iter()
                        .map(|run| reader.block_run_span(&trace_id, run))
                        .collect::<Result<_, _>>()
                        .map_err(bad)?
                }
                // Each adjacent run of the blocks KEY_IDX names is read from
                // its first candidate to its last, a pruned block between them
                // included, the way the trace's run is read without an index.
                (None, Some(set)) => contiguous_runs(set)
                    .iter()
                    .filter_map(|run| {
                        let first = run
                            .iter()
                            .position(|b| candidate_idx.binary_search(b).is_ok())?;
                        let last = run
                            .iter()
                            .rposition(|b| candidate_idx.binary_search(b).is_ok())?;
                        Some(reader.block_run_span(&trace_id, &run[first..=last]))
                    })
                    .collect::<Result<_, _>>()
                    .map_err(bad)?,
            }
        };

        let mut buffers = Vec::new();
        let mut candidates = Vec::new();
        for run in &runs {
            let wanted: Vec<usize> = run
                .blocks()
                .iter()
                .copied()
                .filter(|b| candidate_idx.binary_search(b).is_ok())
                .collect();
            if wanted.is_empty() {
                continue;
            }
            let bytes = match probe.slice(run.start(), run.byte_len()) {
                Some(bytes) => bytes,
                None => {
                    self.ranged_read(seg_ref, tenant_hash, run.start(), run.byte_len(), scan)
                        .in_get_phase(QueryPhase::Scan.into())
                        .await?
                }
            };
            let buffer = buffers.len();
            buffers.push(bytes);
            for b in wanted {
                let loc = reader.block_loc(b).map_err(bad)?;
                let crc = skip.blocks[b].block_crc32c;
                let rel = |abs: u64| {
                    usize::try_from(abs - run.start())
                        .map_err(|_| bad(SpanSegError::Corrupted("block offset range".into())))
                };
                candidates.push((buffer, rel(loc.start())?, rel(loc.end())?, crc));
            }
        }

        let stats = ScanStats {
            blocks_total,
            blocks_after_skip: candidates.len() as u32,
            blocks_scanned: 0,
        };
        let mut out = self.new_scan(
            buffers,
            key.to_string(),
            query,
            projection,
            candidates,
            stats,
            scan.clone(),
            SpanReadPath::RangedTrace,
        );
        out.key_index = key_index;
        Ok(out)
    }

    /// The tier-1 KEY_IDX probe of one object (ADR-2707 decision 5): the
    /// section's `[0, prefix_len)`, then the one bucket the trace mixes to,
    /// skipped when the directory says that bucket is empty. Each read is
    /// sliced from the tail probe when the probe holds it, otherwise one
    /// ranged GET on `accounting` (the key-index phase).
    ///
    /// `None` is a fallback to SKIP_IDX alone: the section's prefix or bucket
    /// is corrupt, which is logged with the object key and prunes nothing. A
    /// failed GET is not a fallback; it is a typed [`SpanFetchError::Store`].
    async fn probe_key_index(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: Option<TenantHash>,
        probe: &Extent,
        reader: &RspanRangeReader,
        trace_id: &[u8; 16],
        accounting: &QueryAccounting,
    ) -> Result<Option<TraceKeyProbe>, SpanFetchError> {
        let key = &seg_ref.data_object_key;
        let fall_back = |err: SpanSegError| {
            tracing::warn!(
                object = %key,
                error = %err,
                "KEY_IDX probe failed; pruning by SKIP_IDX alone"
            );
        };
        let prefix_span = match reader.key_idx_prefix_span() {
            Ok(span) => span,
            Err(err) => {
                fall_back(err);
                return Ok(None);
            }
        };
        let prefix = self
            .key_index_read(seg_ref, tenant_hash, probe, prefix_span, accounting)
            .await?;
        let bucket = match reader.locate_trace_key(trace_id, &prefix) {
            Ok(TraceKeyStep::Done(answer)) => return Ok(Some(answer)),
            Ok(TraceKeyStep::Bucket(bucket)) => bucket,
            Err(err) => {
                fall_back(err);
                return Ok(None);
            }
        };
        let frame = self
            .key_index_read(seg_ref, tenant_hash, probe, bucket.range(), accounting)
            .await?;
        match bucket.finish(&frame) {
            Ok(answer) => Ok(Some(answer)),
            Err(err) => {
                fall_back(err);
                Ok(None)
            }
        }
    }

    /// One KEY_IDX read of the absolute `range`: sliced from the tail probe
    /// when the probe covers it, otherwise one ranged read timed and charged
    /// under [`QueryPhase::KeyIndex`].
    async fn key_index_read(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: Option<TenantHash>,
        probe: &Extent,
        range: std::ops::Range<u64>,
        accounting: &QueryAccounting,
    ) -> Result<Bytes, SpanFetchError> {
        let len = range.end.saturating_sub(range.start);
        match probe.slice(range.start, len) {
            Some(bytes) => Ok(bytes),
            None => {
                self.ranged_read(seg_ref, tenant_hash, range.start, len, accounting)
                    .in_get_phase(QueryPhase::KeyIndex.into())
                    .await
            }
        }
    }

    /// A section's stored bytes: sliced from the tail probe when the probe
    /// covers it, otherwise one ranged GET of exactly `[offset, offset + len)`.
    async fn section_bytes(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: Option<TenantHash>,
        probe: &Extent,
        desc: &SectionDesc,
        accounting: &QueryAccounting,
    ) -> Result<Bytes, SpanFetchError> {
        match probe.slice(desc.offset, desc.len) {
            Some(bytes) => Ok(bytes),
            None => {
                self.ranged_read(seg_ref, tenant_hash, desc.offset, desc.len, accounting)
                    .await
            }
        }
    }

    /// One ranged read of `[offset, offset + len)`, through the read cache
    /// keyed `(tenant_hash, content_hash, offset, len)` when one is configured
    /// and the caller is tenant-checked. A store returning a different length
    /// than asked is `Corrupt`.
    async fn ranged_read(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: Option<TenantHash>,
        offset: u64,
        len: u64,
        accounting: &QueryAccounting,
    ) -> Result<Bytes, SpanFetchError> {
        let key = &seg_ref.data_object_key;
        let end = offset
            .checked_add(len)
            .filter(|end| len > 0 && *end <= seg_ref.object_size)
            .ok_or_else(|| {
                corrupt(
                    key,
                    SpanSegError::Corrupted("ranged read outside the object".into()),
                )
            })?;
        let bytes = self
            .read_extent(
                seg_ref,
                tenant_hash,
                offset,
                len,
                GetRange::Range(offset, end),
                accounting,
            )
            .await?;
        if bytes.len() as u64 != len {
            return Err(corrupt(
                key,
                SpanSegError::Corrupted("ranged read returned the wrong length".into()),
            ));
        }
        Ok(bytes)
    }

    /// The one primitive behind both exits (ADR-0110 decision 2): resolve
    /// candidate blocks once (the skip-index trace_id/ts and duration/status
    /// prune, plus the bloom-backed prune when `predicates` are pushed), then
    /// hand a [`SpanColumnarScan`] cursor that decodes one candidate block at a
    /// time. `fetch`/`fetch_accounted` drive it with [`BlockProjection::All`]
    /// and rebuild `SpanRow`s ([`drain_rows`]); `fetch_accounted_columnar`
    /// returns the cursor for the caller to stream. Sharing this is what keeps
    /// candidate selection, bloom probing, and the ts/`trace_id` predicate
    /// byte-identical across the two exits.
    #[allow(clippy::too_many_arguments)]
    fn open_scan(
        &self,
        bytes: Bytes,
        key: String,
        query: SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
        projection: BlockProjection,
        accounting: QueryAccounting,
    ) -> Result<SpanColumnarScan, SpanFetchError> {
        let (candidates, stats) = self
            .plan_candidates(&bytes, &query, duration_ns, status_mask, predicates)
            .map_err(|source| corrupt(&key, source))?;
        let candidates = candidates
            .into_iter()
            .map(|(start, end, crc)| (0, start, end, crc))
            .collect();
        Ok(self.new_scan(
            vec![bytes],
            key,
            query,
            projection,
            candidates,
            stats,
            accounting,
            SpanReadPath::WholeObject,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn new_scan(
        &self,
        buffers: Vec<Bytes>,
        key: String,
        query: SpanQuery,
        projection: BlockProjection,
        candidates: Vec<BufferedBlock>,
        stats: ScanStats,
        accounting: QueryAccounting,
        read_path: SpanReadPath,
    ) -> SpanColumnarScan {
        SpanColumnarScan {
            buffers,
            key,
            query,
            projection,
            candidates,
            cursor: 0,
            stats,
            accounting,
            page_bytes_fetched: 0,
            page_bytes_decoded: 0,
            pages_decoded: 0,
            pages_skipped: 0,
            finished: false,
            read_gate: self.read_gate.clone(),
            lost: false,
            lost_to: None,
            read_path,
            key_index: KeyIndexPruning::default(),
        }
    }

    /// Fetches one segment's whole-object bytes, consulting the ADR-0046 read
    /// cache when [`with_cache`](Self::with_cache) configured one -- mirrors
    /// [`LogSegmentFetcher::whole_object_bytes`](crate::log_fetcher::LogSegmentFetcher::whole_object_bytes)
    /// byte-for-byte in shape. With no cache configured this issues exactly
    /// the one store GET `fetch_accounted`/`fetch_accounted_columnar` recorded
    /// before this existed, so an uncached caller's behavior and accounting
    /// are unchanged.
    ///
    /// With a cache configured, the key is `(tenant_hash, content_hash, 0,
    /// object_size)` (ADR-0046 decision 2): tenant is part of the key, so two
    /// tenants whose objects are byte-identical never share an entry, even
    /// though the content hash alone would already be unique -- defence in
    /// depth against a hash collision or a programming error serving one
    /// tenant's bytes to another. A cache hit re-runs the disk tier's crc32c
    /// check on every access (ADR-0046 decision 4); a checksum failure or any
    /// other disk-entry defect degrades to a miss and is counted by
    /// `ravel-cache`'s own `disk_errors_degraded_to_misses` counter, not a new
    /// span-specific one, and this call falls through to the one store GET a
    /// miss takes -- it never serves unverified bytes. The [`ReadOutcome`] from
    /// [`ReadCache::get_or_fetch`] decides whether this call records a cache
    /// hit, a cache miss plus the store GET it ran, or, for a late serve (a
    /// single-flight follower on either cache kind, or a RAM-only recheck
    /// serve), a cache miss with no GET, so a hit and a miss are never
    /// double-counted on the tiered tier.
    async fn whole_object_bytes(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<Bytes, SpanFetchError> {
        self.read_extent(
            seg_ref,
            Some(tenant_hash),
            0,
            seg_ref.object_size,
            GetRange::Full,
            accounting,
        )
        .await
    }

    /// One store read of the extent `[offset, offset + len)`, issued as
    /// `range`, through the read cache keyed `(tenant_hash, content_hash,
    /// offset, len)` when a cache is configured and `tenant_hash` is known.
    /// Shared by the whole-object read and the ranged trace reads, so both
    /// account a hit, a miss and a GET the same way.
    async fn read_extent(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: Option<TenantHash>,
        offset: u64,
        len: u64,
        range: GetRange,
        accounting: &QueryAccounting,
    ) -> Result<Bytes, SpanFetchError> {
        let key = &seg_ref.data_object_key;
        // Reserve the extent's bytes before the GET (ADR-1170 decision 2): a
        // refusal fails typed with zero GETs. The guard travels with the
        // returned bytes, owned for the fetched buffer's lifetime rather than
        // released when the GET completes.
        let reservation = self.reserve_fetch(len)?;

        let (Some(cache), Some(tenant_hash)) = (&self.cache, tenant_hash) else {
            let _permit = self
                .get_limiter
                .acquire()
                .await
                .map_err(|_| SpanFetchError::Store {
                    key: key.to_string(),
                    source: StoreError::Transient(
                        "GetLimiter semaphore closed unexpectedly".to_string(),
                    ),
                })?;
            let got = self
                .store
                .get(key, range)
                .await
                .map_err(|source| SpanFetchError::Store {
                    key: key.to_string(),
                    source,
                })?;
            accounting.record_s3_request(AccountedOp::Get);
            accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
            return Ok(attach_reservation(got.data, reservation));
        };

        let cache_key = CacheKey::new(tenant_hash.0, seg_ref.content_hash, offset, len);
        // An exact range names the byte count it must return; a short answer
        // is refused inside the closure so the cache never admits it (the
        // logs fetcher's `check_extent_len` placement), and a later lookup of
        // the same extent goes back to the store instead of replaying the
        // short bytes as a hit.
        let want = match range {
            GetRange::Range(start, end) => Some(end.saturating_sub(start)),
            _ => None,
        };
        // One read-through call, accounted from the returned `ReadOutcome`: a
        // single call avoids the peek-then-`get_or_fetch` double-count on the
        // tiered tier (see `ReadCache::get_or_fetch`).
        let (bytes, outcome) = cache
            .get_or_fetch(cache_key, accounting.access_intent(), || async move {
                let _permit = self.get_limiter.acquire().await.map_err(|_| {
                    StoreError::Transient("GetLimiter semaphore closed unexpectedly".to_string())
                })?;
                let got = self.store.get(key, range).await?;
                accounting.record_s3_request(AccountedOp::Get);
                accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
                if let Some(want) = want
                    && got.data.len() as u64 != want
                {
                    return Err(crate::fetcher::CacheFetchError::Corrupt {
                        key: key.to_string(),
                        message: format!("short read: got {} bytes of {want}", got.data.len()),
                    });
                }
                Ok(got.data)
            })
            .await
            // The closure verifies the returned length of an exact range and
            // nothing else, so `Corrupt` is reachable for a short read while
            // `EtagChanged` is not; both are mapped so the match stays
            // exhaustive against the shared error type.
            .map_err(|err| from_cache_error(key, err))?;

        let mut reservation = reservation;
        match outcome {
            ReadOutcome::Hit => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                // Same two-ledger overlap as the log fetcher's hit path: the
                // returned `Bytes` clones the cache entry's allocation, so the
                // cache cap and this guard both cover it until the caller drops
                // it (ADR-1170 decision 2).
                reservation.mark_handed_off();
            }
            // A miss. Only the call that ran the closure above recorded a
            // store GET; a late serve (another caller's flight, or the RAM
            // recheck) recorded none and stays this miss alone.
            ReadOutcome::Fetched | ReadOutcome::LateServe => {
                accounting.record_cache_miss();
                // Admitted to the read cache, which has its own byte ledger:
                // mark the reservation handed off so the transient overlap is
                // visible while both this buffer and the cache entry hold the
                // same bytes (ADR-1170 decision 2). Cleared when this guard
                // drops.
                reservation.mark_handed_off();
            }
        }
        Ok(attach_reservation(bytes, reservation))
    }

    /// Candidate selection over one whole-object's bytes, with no block decoded:
    /// resolves each surviving block to an absolute, bounds-checked
    /// `(start, end, crc32c)` byte range so the cursor can re-slice `bytes`
    /// without holding a borrow of the reader or skip index. Kept store-free so
    /// the block-level logic is unit-testable without an object store.
    fn plan_candidates(
        &self,
        bytes: &[u8],
        query: &SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
    ) -> Result<(Vec<CandidateBlock>, ScanStats), SpanSegError> {
        let reader = RspanReader::new(bytes, &self.cfg)?;
        let skip = reader.skip_index();
        let blocks_total = skip.blocks.len() as u32;

        // ts_min > ts_max is an empty window: no candidate, exactly the
        // short-circuit the row scan applied before.
        let candidate_idx = if query.ts_min > query.ts_max {
            Vec::new()
        } else if predicates.is_empty() {
            skip.candidate_blocks(
                query.trace_id.as_ref(),
                query.ts_min,
                query.ts_max,
                duration_ns,
                status_mask,
            )
        } else {
            let bloom = reader.bloom()?;
            skip.candidate_blocks_with_bloom(
                query.trace_id.as_ref(),
                query.ts_min,
                query.ts_max,
                duration_ns,
                status_mask,
                &bloom,
                predicates,
            )?
        };

        // The BLOCKS section's absolute offset and length, for bounds-checked
        // per-block slicing (mirrors RspanReader's own block_bytes).
        let footer = open(bytes)?;
        let blocks = footer
            .section(kind::BLOCKS)
            .ok_or_else(|| SpanSegError::Corrupted("missing BLOCKS section".into()))?;
        let (blocks_offset, blocks_len) = (blocks.offset, blocks.len);

        let mut candidates = Vec::with_capacity(candidate_idx.len());
        for &b in &candidate_idx {
            let entry = &skip.blocks[b];
            let (start, end) = abs_block_range(blocks_offset, blocks_len, entry)?;
            candidates.push((start, end, entry.block_crc32c));
        }

        let stats = ScanStats {
            blocks_total,
            blocks_after_skip: candidate_idx.len() as u32,
            blocks_scanned: 0,
        };
        Ok((candidates, stats))
    }
}

/// A fetched, candidate-pruned, not-yet-decoded columnar scan over one RSPAN
/// object (ADR-0110 decision 2), the streaming counterpart of a `Vec<SpanRow>`.
/// The object's bytes are resident and its candidate blocks are chosen, but no
/// block is decoded until [`next_block`](Self::next_block); the caller pulls one
/// [`ColumnarBlock`] at a time so peak decoded memory is one block.
///
/// # Accounting is folded once, even on early abandonment
///
/// `page_bytes_fetched`/`page_bytes_decoded` accumulate per decoded block and
/// are folded into the query's [`QueryAccounting`] exactly once, by
/// [`finish`](Self::finish), when the scan is drained to exhaustion or dropped
/// (the `Drop` impl below). A scan a caller abandons after an upstream `LIMIT`
/// is satisfied never reaches the exhaustion arm, so without the drop-time fold
/// the partial decode it already did would be missing from the query's
/// accounting; `finish` is idempotent, so the two paths never double-count. This
/// mirrors `LogSegmentScan` (crate `log_fetcher`, PR #642).
pub struct SpanColumnarScan {
    /// The resident object bytes: the whole object on the whole-object path,
    /// one buffer per fetched block run on the ranged trace path.
    buffers: Vec<Bytes>,
    /// The object key, for error attribution.
    key: String,
    query: SpanQuery,
    projection: BlockProjection,
    /// `(buffer, start, end, crc32c)` of the candidate blocks, in scan order,
    /// with `start..end` relative to `buffers[buffer]`.
    candidates: Vec<BufferedBlock>,
    /// Which read produced `buffers`.
    read_path: SpanReadPath,
    /// What the KEY_IDX probe pruned before `candidates` were chosen.
    key_index: KeyIndexPruning,
    cursor: usize,
    stats: ScanStats,
    /// This query's accounting handle, folded once at exhaustion or drop with
    /// the scan's decode-time `page_bytes_fetched`/`page_bytes_decoded` totals
    /// (ADR-0107 decision 4). A separate, additive axis from the wire bytes the
    /// fetch funnel records through `add_s3_bytes`.
    accounting: QueryAccounting,
    page_bytes_fetched: u64,
    page_bytes_decoded: u64,
    /// Column pages decoded and skipped across the blocks drained so far: the
    /// count-side siblings of the `page_bytes_*` totals above, taken from the
    /// same per-block decode. Exposed rather than folded into `accounting`
    /// because a scan's caller publishes them as PARTITION metrics
    /// (`SpansScanExec`'s `pages_decoded`/`pages_skipped`), which the
    /// query-global accounting handle cannot express.
    pages_decoded: usize,
    pages_skipped: usize,
    /// Set once the accounting fold has run, so it runs exactly once.
    finished: bool,
    /// The read CPU gate [`next_block_on_gate`](Self::next_block_on_gate)
    /// decodes on; `None` decodes inline.
    read_gate: Option<Arc<ReadGate>>,
    /// Set once a gated block decode failed or was abandoned. The cursor had
    /// already moved past that block, so every later call fails rather than
    /// hand out the next block of a partial segment.
    lost: bool,
    /// The gate failure that lost the block, so a later call reports the same
    /// class the losing call reported (a panicked decode stays a `Corrupt`,
    /// not a redacted permanent store error). `None` when the block was
    /// abandoned instead: a caller dropped the future while the job was queued
    /// or running, and no classified failure exists to repeat.
    lost_to: Option<CpuGateError>,
}

impl SpanColumnarScan {
    /// The scan's pruning counters. `blocks_scanned` grows as blocks are
    /// consumed; read it after the last [`next_block`](Self::next_block).
    pub fn stats(&self) -> ScanStats {
        self.stats
    }

    /// Which read this scan's bytes came from, exposed for the explain
    /// output (#2832 wires it).
    pub fn read_path(&self) -> SpanReadPath {
        self.read_path
    }

    /// What the object's KEY_IDX probe pruned before this scan's candidate
    /// blocks were chosen.
    pub fn key_index(&self) -> KeyIndexPruning {
        self.key_index
    }

    /// Candidate blocks not yet decoded.
    pub fn remaining_blocks(&self) -> usize {
        self.candidates.len() - self.cursor
    }

    /// Column pages the blocks drained so far decompressed. Read after the last
    /// [`next_block`](Self::next_block), or at any point for the partial total.
    pub fn pages_decoded(&self) -> usize {
        self.pages_decoded
    }

    /// Column pages the blocks drained so far walked past because this scan's
    /// projection excluded them. Always 0 when the scan requests every column
    /// (the row exit).
    pub fn pages_skipped(&self) -> usize {
        self.pages_skipped
    }

    /// Decode the next candidate block and return it with its surviving row
    /// indices, or `None` once every candidate has been decoded.
    ///
    /// `Some(block)` with an empty `rows` is normal and distinct from `None`: a
    /// candidate block can survive pruning yet hold no row inside the ts window.
    /// Only `None` ends the scan and triggers the accounting fold.
    pub fn next_block(&mut self) -> Result<Option<ColumnarBlock>, SpanFetchError> {
        let Some((block_bytes, crc)) = self.next_candidate()? else {
            return Ok(None);
        };
        let decoded = decode_block_rows(&block_bytes, crc, &self.projection, &self.query);
        self.record_block(decoded).map(Some)
    }

    /// [`next_block`](Self::next_block) with the block decode run on the
    /// fetcher's read gate (ADR-1702 decision 4): the whole block, all its
    /// pages, is one job sized by the uncompressed lengths its header lists.
    /// Without a gate this is `next_block`. Reaching exhaustion submits no
    /// job.
    pub async fn next_block_on_gate(&mut self) -> Result<Option<ColumnarBlock>, SpanFetchError> {
        let Some(gate) = self.read_gate.clone() else {
            return self.next_block();
        };
        let Some((block_bytes, crc)) = self.next_candidate()? else {
            return Ok(None);
        };
        let size = JobSize::Bytes(block_uncompressed_len(&block_bytes));
        let projection = self.projection.clone();
        let query = self.query;
        // Cleared only once the job returns: a failed job, or this future
        // dropped while the job is queued or running, leaves the scan lost.
        self.lost = true;
        let decoded = match gate
            .run(ReadSite::SpanBlock, size, move || {
                decode_block_rows(&block_bytes, crc, &projection, &query)
            })
            .await
        {
            Ok(decoded) => decoded,
            Err(err) => {
                // Keep the failure so a later call repeats its class rather
                // than degrading to a permanent store error.
                self.lost_to = Some(err);
                return Err(span_gate_failed(&self.key, err));
            }
        };
        self.lost = false;
        self.record_block(decoded).map(Some)
    }

    /// Advances to the next candidate block and returns its bytes, or `None`
    /// (after the accounting fold) once every candidate has been taken.
    fn next_candidate(&mut self) -> Result<Option<(Bytes, u32)>, SpanFetchError> {
        if self.lost {
            return Err(match self.lost_to {
                Some(err) => span_gate_failed(&self.key, err),
                None => span_scan_lost(&self.key),
            });
        }
        let Some(&(buffer, start, end, crc)) = self.candidates.get(self.cursor) else {
            self.finish();
            return Ok(None);
        };
        self.cursor += 1;
        let Some(bytes) = self
            .buffers
            .get(buffer)
            .filter(|bytes| bytes.get(start..end).is_some())
        else {
            return Err(corrupt(
                &self.key,
                SpanSegError::Corrupted("block out of bounds".into()),
            ));
        };
        Ok(Some((bytes.slice(start..end), crc)))
    }

    /// Folds one decoded block into the scan's counters and hands it out.
    fn record_block(&mut self, decoded: DecodedRows) -> Result<ColumnarBlock, SpanFetchError> {
        let (decoded, rows) = decoded.map_err(|source| corrupt(&self.key, source))?;
        self.page_bytes_fetched += decoded.page_bytes_fetched;
        self.page_bytes_decoded += decoded.page_bytes_decoded;
        self.pages_decoded += decoded.block.pages_decoded();
        self.pages_skipped += decoded.block.pages_skipped();
        self.stats.blocks_scanned += 1;
        let rows = rows.map_err(|source| corrupt(&self.key, source))?;
        Ok(ColumnarBlock {
            block: decoded.block,
            rows,
        })
    }

    /// Folds this scan's accumulated page-byte counters into the query's
    /// accounting, exactly once (ADR-0107 decision 4).
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.accounting
            .add_page_bytes_fetched(self.page_bytes_fetched);
        self.accounting
            .add_page_bytes_decoded(self.page_bytes_decoded);
    }
}

impl Drop for SpanColumnarScan {
    /// A scan abandoned before exhaustion (an upstream `LIMIT` dropping the
    /// stream is the reachable case) never hits `next_block`'s exhaustion arm,
    /// so without this the partial decode it already did would be missing from
    /// the query's accounting. `finish` is idempotent, so this is a no-op on the
    /// already-exhausted path.
    fn drop(&mut self) {
        self.finish();
    }
}

/// Which columns a block decode materializes, and thus which pages count as
/// decoded for accounting. Mirrors `rspan`'s own projection: [`All`] is the row
/// exit (every page), [`Only`] is the columnar exit's requested set unioned with
/// the predicate columns.
///
/// [`All`]: BlockProjection::All
/// [`Only`]: BlockProjection::Only
#[derive(Clone)]
enum BlockProjection {
    All,
    Only(Vec<u32>),
}

impl BlockProjection {
    /// The columnar exit's effective decode set: the caller's `projected`
    /// columns plus the predicate/ordering columns
    /// ([`COL_TRACE_ID`]/[`COL_START_TS`]/[`COL_END_TS`]), deduplicated. Adding
    /// the predicate columns is required, not optional: [`surviving_rows`] reads
    /// them through the block view, which returns
    /// [`ravel_rspan::SpanSegError::ColumnNotRequested`] if the decode did not
    /// request them. ADR-0110 decision 4 names the same union.
    fn columnar(projected: &[u32]) -> Self {
        let mut cols: Vec<u32> = projected.to_vec();
        for c in [COL_TRACE_ID, COL_START_TS, COL_END_TS] {
            if !cols.contains(&c) {
                cols.push(c);
            }
        }
        BlockProjection::Only(cols)
    }
}

/// A decoded block plus the stored page bytes its decode fetched and actually
/// decoded (ADR-0107 decision 4).
struct DecodedAccounted {
    block: DecodedBlock,
    page_bytes_fetched: u64,
    page_bytes_decoded: u64,
}

/// Decode one block under `projection` and measure its stored page bytes
/// (ADR-0107 decision 4). `page_bytes_fetched` is every page present in the
/// block (the object was fetched whole regardless of projection);
/// `page_bytes_decoded` is the pages this projection actually decoded. The two
/// are equal for [`BlockProjection::All`] and diverge once the projection
/// excludes a page the block carries.
///
/// The per-page byte split is read from the block header
/// ([`parse_page_descs`]): `rspan`'s `read_block` surfaces page *counts*
/// ([`DecodedBlock::pages_decoded`]/[`DecodedBlock::pages_skipped`]) but not
/// their stored bytes, which is what this accounting needs. The decoded/skipped
/// page counts this projection implies are cross-checked against the decode's
/// own counts: a mismatch means the header walk and the decoder disagree about
/// which pages ran, which would make the byte split a fiction, so it fails
/// closed with a typed `Corrupted` error rather than reporting a plausible wrong
/// number.
fn decode_block_accounted(
    block_bytes: &[u8],
    crc: u32,
    projection: &BlockProjection,
) -> Result<DecodedAccounted, SpanSegError> {
    let block = match projection {
        BlockProjection::All => read_block(block_bytes, crc, DEFAULT_MAX_UNCOMP)?,
        BlockProjection::Only(cols) => {
            read_block_projected(block_bytes, crc, DEFAULT_MAX_UNCOMP, cols)?
        }
    };
    // The decode validated the block's crc and structure, so this walk is over
    // known-good bytes.
    let descs = parse_page_descs(block_bytes)?;
    let page_bytes_fetched: u64 = descs.iter().map(|(_, len)| *len).sum();

    let (page_bytes_decoded, decoded_pages, skipped_pages) = match projection {
        BlockProjection::All => (page_bytes_fetched, descs.len(), 0usize),
        BlockProjection::Only(cols) => {
            let mut decoded = 0u64;
            let mut dcount = 0usize;
            let mut scount = 0usize;
            for (col, len) in &descs {
                if page_needed(cols, *col) {
                    decoded += *len;
                    dcount += 1;
                } else {
                    scount += 1;
                }
            }
            (decoded, dcount, scount)
        }
    };
    if block.pages_decoded() != decoded_pages || block.pages_skipped() != skipped_pages {
        return Err(SpanSegError::Corrupted(
            "page-byte accounting disagreed with block decode".into(),
        ));
    }
    Ok(DecodedAccounted {
        block,
        page_bytes_fetched,
        page_bytes_decoded,
    })
}

/// Whether column `col`'s page(s) are decoded under the columnar projection
/// `cols`. Mirrors `rspan`'s `ColumnProjection::page_needed`: the four event
/// columns decode as a group (all or none) whenever any event column is
/// requested; every other column decodes iff it was requested. The runtime
/// cross-check in [`decode_block_accounted`] guards this against drift from the
/// decoder's own rule.
fn page_needed(cols: &[u32], col: u32) -> bool {
    const EVENT_COLS: [u32; 4] = [
        COL_EVENT_COUNT,
        COL_EVENT_TS,
        COL_EVENT_NAME,
        COL_EVENT_ATTRS_BLOB,
    ];
    if EVENT_COLS.contains(&col) {
        cols.iter().any(|c| EVENT_COLS.contains(c))
    } else {
        cols.contains(&col)
    }
}

/// A block decode's outcome: the decoded block and, separately, its surviving
/// rows, so the scan counts a decoded block even when its row filter fails.
type DecodedRows = Result<(DecodedAccounted, Result<Vec<usize>, SpanSegError>), SpanSegError>;

/// Decodes one candidate block under `projection` and computes its surviving
/// rows: the unit a gated decode runs as one job.
fn decode_block_rows(
    block_bytes: &[u8],
    crc: u32,
    projection: &BlockProjection,
    query: &SpanQuery,
) -> DecodedRows {
    let decoded = decode_block_accounted(block_bytes, crc, projection)?;
    let rows = surviving_rows(&decoded.block, query);
    Ok((decoded, rows))
}

/// The sum of the uncompressed page lengths a block's header lists, which is
/// what its decode inflates to. The header is not yet crc-checked here, so the
/// walk allocates nothing, and a header it cannot read sizes the block at
/// `u64::MAX`: the decode that follows reports the same bytes as corrupt.
fn block_uncompressed_len(block_bytes: &[u8]) -> u64 {
    let walk = || -> Result<u64, SpanSegError> {
        let mut pos = 0usize;
        let _record_count = get_uvarint(block_bytes, &mut pos)?;
        let page_count = get_uvarint(block_bytes, &mut pos)?;
        let mut total = 0u64;
        for _ in 0..page_count {
            let _column_id = get_uvarint(block_bytes, &mut pos)?;
            pos = pos
                .checked_add(2)
                .filter(|p| *p <= block_bytes.len())
                .ok_or_else(|| {
                    SpanSegError::Corrupted("block truncated at page enc/comp".into())
                })?;
            let _len = get_uvarint(block_bytes, &mut pos)?;
            total = total.saturating_add(get_uvarint(block_bytes, &mut pos)?);
        }
        Ok(total)
    };
    walk().unwrap_or(u64::MAX)
}

/// The `(column_id, stored_len)` of every page in a block, read from the block
/// header's page-descriptor table (docs/segment-format.md). `rspan` decodes this
/// table internally but exposes only page *counts*
/// ([`DecodedBlock::pages_decoded`]/[`DecodedBlock::pages_skipped`]), not the
/// stored bytes ADR-0107 decision 4's `page_bytes_fetched`/`page_bytes_decoded`
/// account, so the byte split is read here from the frozen header layout. Called
/// only after `read_block` has validated the block's crc and structure, so the
/// walk is over known-good bytes; any inconsistency is still returned as a typed
/// `Corrupted` error rather than a panic.
fn parse_page_descs(block_bytes: &[u8]) -> Result<Vec<(u32, u64)>, SpanSegError> {
    let mut pos = 0usize;
    let _record_count = get_uvarint(block_bytes, &mut pos)?;
    let page_count = usize::try_from(get_uvarint(block_bytes, &mut pos)?)
        .map_err(|_| SpanSegError::Corrupted("page count range".into()))?;
    let mut out = Vec::with_capacity(page_count);
    for _ in 0..page_count {
        let column_id = u32::try_from(get_uvarint(block_bytes, &mut pos)?)
            .map_err(|_| SpanSegError::Corrupted("column id range".into()))?;
        // The enc (1 byte) and comp (1 byte) tags do not affect the stored byte
        // count, only `pos`; skip past them, bounds-checked.
        pos = pos
            .checked_add(2)
            .filter(|p| *p <= block_bytes.len())
            .ok_or_else(|| SpanSegError::Corrupted("block truncated at page enc/comp".into()))?;
        let len = get_uvarint(block_bytes, &mut pos)?;
        let _uncomp_len = get_uvarint(block_bytes, &mut pos)?;
        out.push((column_id, len));
    }
    Ok(out)
}

/// The rows of `block` that survive `query` — the ts window overlap and, when
/// set, the `trace_id` equality — evaluated over the [`COL_START_TS`],
/// [`COL_END_TS`], and [`COL_TRACE_ID`] columns through the block view, in
/// ascending row order. This is the columnar form of the old per-record
/// `matches`: `start_ts <= ts_max && end_ts >= ts_min`, plus `trace_id == tid`
/// when a trace filter is set. Both exits compute survivors here, so a block's
/// surviving set is identical whether the caller wants rows or columns.
fn surviving_rows(block: &DecodedBlock, query: &SpanQuery) -> Result<Vec<usize>, SpanSegError> {
    let view = block.view();
    let start = view.i64_column(COL_START_TS)?;
    let end = view.i64_column(COL_END_TS)?;
    let trace = match query.trace_id {
        Some(_) => Some(view.fixed_column(COL_TRACE_ID)?),
        None => None,
    };
    let mut rows = Vec::new();
    for row in 0..block.record_count() {
        if let (Some(want), Some(col)) = (query.trace_id.as_ref(), trace.as_ref())
            && col.value_at(row) != Some(want.as_slice())
        {
            continue;
        }
        // start_ts/end_ts are always-present columns (the writer stages them for
        // every row); a missing value is a corrupt block, the same input the row
        // path's `record` errors on, so it fails closed rather than silently
        // dropping the row.
        let s = start
            .value_at(row)
            .ok_or_else(|| SpanSegError::Corrupted("missing start_ts".into()))?;
        let e = end
            .value_at(row)
            .ok_or_else(|| SpanSegError::Corrupted("missing end_ts".into()))?;
        if s <= query.ts_max && e >= query.ts_min {
            rows.push(row);
        }
    }
    Ok(rows)
}

/// Rebuild one [`SpanRow`] from a fully-decoded block (the row exit's
/// [`BlockProjection::All`]), identically to the pre-columnar scan: the whole
/// record plus `service_name` read straight from the v3 dictionary column
/// (ADR-0054), not by scanning the merged attrs map.
fn build_span_row(block: &DecodedBlock, row: usize) -> Result<SpanRow, SpanSegError> {
    let record = block.record(row)?;
    let service_name = match block.service_name(row) {
        Some(v) => Some(
            String::from_utf8(v)
                .map_err(|_| SpanSegError::Corrupted("service_name not utf-8".into()))?,
        ),
        None => None,
    };
    Ok(SpanRow {
        record,
        service_name,
    })
}

/// Drain a full [`BlockProjection::All`] scan into the row exit's
/// [`SpanFetchOutput`]: every candidate block's surviving rows rebuilt as
/// `SpanRow`s, in scan order. This is the row exit expressed over the columnar
/// primitive, so `fetch`/`fetch_accounted` share candidate selection, bloom
/// probing, ts/`trace_id` filtering, and the page-byte fold with the columnar
/// exit. Dropping the scan folds its accounting (`finish`).
///
/// The scan's page counts ride out on the output next to `stats`, so the row
/// exit's caller can publish the same `pages_decoded`/`pages_skipped` figures
/// the columnar exit's caller reads off each [`ColumnarBlock`] (#669).
async fn drain_rows(mut scan: SpanColumnarScan) -> Result<SpanFetchOutput, SpanFetchError> {
    let mut records = Vec::new();
    while let Some(block) = scan.next_block_on_gate().await? {
        for &row in &block.rows {
            records.push(
                build_span_row(&block.block, row).map_err(|source| corrupt(&scan.key, source))?,
            );
        }
    }
    let stats = scan.stats();
    Ok(SpanFetchOutput {
        records,
        stats,
        pages_decoded: scan.pages_decoded(),
        pages_skipped: scan.pages_skipped(),
        key_index: scan.key_index(),
    })
}

/// The absolute, bounds-checked `[start, end)` byte range of one block within
/// the whole object. `block_offset`/`block_len` (from the block's SKIP_IDX
/// entry) are relative to the BLOCKS section; the range is checked against the
/// section's own length (`blocks_len`), not merely the whole object, so a
/// corrupt `(offset, len)` decoded from SKIP_IDX can never land past the section
/// end and slice foreign SKIP_IDX/footer bytes. Every violation is a typed
/// `Corrupted` error, never a panic. Mirrors `RspanReader::block_bytes`, which
/// is private to `ravel-rspan`.
fn abs_block_range(
    blocks_offset: u64,
    blocks_len: u64,
    entry: &BlockEntry,
) -> Result<(usize, usize), SpanSegError> {
    let rel_end = entry
        .block_offset
        .checked_add(entry.block_len)
        .ok_or_else(|| SpanSegError::Corrupted("block range overflow".into()))?;
    if rel_end > blocks_len {
        return Err(SpanSegError::Corrupted(
            "block range exceeds BLOCKS section".into(),
        ));
    }
    let abs = blocks_offset
        .checked_add(entry.block_offset)
        .ok_or_else(|| SpanSegError::Corrupted("block offset overflow".into()))?;
    let start =
        usize::try_from(abs).map_err(|_| SpanSegError::Corrupted("block offset range".into()))?;
    let len = usize::try_from(entry.block_len)
        .map_err(|_| SpanSegError::Corrupted("block len range".into()))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| SpanSegError::Corrupted("block range overflow".into()))?;
    Ok((start, end))
}

/// Bytes read from the object at absolute `offset`.
struct Extent {
    offset: u64,
    bytes: Bytes,
}

impl Extent {
    /// The absolute range `[offset, offset + len)` when this extent holds all
    /// of it.
    fn slice(&self, offset: u64, len: u64) -> Option<Bytes> {
        let start = usize::try_from(offset.checked_sub(self.offset)?).ok()?;
        let end = start.checked_add(usize::try_from(len).ok()?)?;
        self.bytes.get(start..end)?;
        Some(self.bytes.slice(start..end))
    }
}

/// A block set split into maximal runs of adjacent ordinals, ascending, with
/// duplicates dropped.
fn contiguous_runs(set: &[u32]) -> Vec<Vec<usize>> {
    let mut ordinals: Vec<usize> = set.iter().map(|&b| b as usize).collect();
    ordinals.sort_unstable();
    ordinals.dedup();
    let mut runs: Vec<Vec<usize>> = Vec::new();
    for b in ordinals {
        match runs.last_mut() {
            Some(run) if run.last().is_some_and(|&last| last + 1 == b) => run.push(b),
            _ => runs.push(vec![b]),
        }
    }
    runs
}

/// How many skip-index candidates the ascending KEY_IDX block `set` leaves
/// out. On an object whose blocks are in trace order, every candidate of a
/// present trace holds it, so this is zero; it is nonzero only when the
/// candidates are wider than the blocks the index names.
fn candidates_outside(candidates: &[usize], set: &[u32]) -> u64 {
    candidates
        .iter()
        .filter(|b| u32::try_from(**b).map_or(true, |b| set.binary_search(&b).is_err()))
        .count() as u64
}

/// Reclassifies a version refusal over the whole object `bytes`: an
/// out-of-window trailer version on bytes that match the recorded content hash
/// is [`SpanFetchError::UnsupportedVersion`]; on bytes that do not, it is
/// corruption. Every other error passes through unchanged.
fn settle_version_refusal(
    seg_ref: &SegmentRef,
    bytes: &[u8],
    err: SpanFetchError,
) -> SpanFetchError {
    match err {
        SpanFetchError::Corrupt {
            key,
            source: SpanSegError::UnsupportedVersion(version),
        } => {
            if blake3::hash(bytes).as_bytes() == &seg_ref.content_hash {
                SpanFetchError::UnsupportedVersion {
                    key,
                    version: u32::from(version),
                }
            } else {
                SpanFetchError::Corrupt {
                    key,
                    source: SpanSegError::Corrupted(crate::fetcher::version_hash_mismatch_detail(
                        version,
                    )),
                }
            }
        }
        other => other,
    }
}

fn corrupt(key: &str, source: SpanSegError) -> SpanFetchError {
    SpanFetchError::Corrupt {
        key: key.to_string(),
        source,
    }
}

/// A failed block decode job (ADR-1702 decision 2): a panic is the decode's
/// own error, a job that never ran is transient.
fn span_gate_failed(key: &str, err: CpuGateError) -> SpanFetchError {
    match err {
        CpuGateError::Panicked => corrupt(
            key,
            SpanSegError::Corrupted(format!("read CPU gate: {err}")),
        ),
        CpuGateError::Cancelled | CpuGateError::Closed => SpanFetchError::Store {
            key: key.to_string(),
            source: gate_not_run(err),
        },
    }
}

/// A [`SpanColumnarScan`] read again after a gated block decode was abandoned:
/// the caller dropped the future while the job was queued or running, so no
/// classified failure exists to repeat. Permanent: that block cannot be read
/// through this scan, and moving on to the next one would return a partial
/// segment. A block lost to a gate failure reports that failure's own class
/// instead, so a panicked decode does not turn into a permanent store error the
/// HTTP layer redacts.
fn span_scan_lost(key: &str) -> SpanFetchError {
    SpanFetchError::Store {
        key: key.to_string(),
        source: StoreError::Permanent(
            "read CPU gate: the scan lost a block with a failed block decode".to_string(),
        ),
    }
}

/// Maps a read-cache single-flight error onto [`SpanFetchError`], mirroring
/// `log_fetcher::from_cache_error`. `SpanFetchError` carries no `EtagChanged`
/// variant (RSPAN's cache closure never checks an etag, unlike the block-range
/// funnel this mapping shape was copied from), so that arm folds into
/// `Corrupt` rather than being silently dropped from the match.
fn from_cache_error(
    key: &str,
    err: SingleFlightError<crate::fetcher::CacheFetchError>,
) -> SpanFetchError {
    match err {
        SingleFlightError::Upstream(crate::fetcher::CacheFetchError::Store(source)) => {
            SpanFetchError::Store {
                key: key.to_string(),
                source: crate::fetcher::clone_store_error(&source),
            }
        }
        SingleFlightError::Upstream(crate::fetcher::CacheFetchError::EtagChanged { .. }) => {
            SpanFetchError::Corrupt {
                key: key.to_string(),
                source: SpanSegError::Corrupted(
                    "read cache reported a changed etag, which this funnel never checks".into(),
                ),
            }
        }
        SingleFlightError::Upstream(crate::fetcher::CacheFetchError::Corrupt {
            message, ..
        }) => SpanFetchError::Corrupt {
            key: key.to_string(),
            source: SpanSegError::Corrupted(message),
        },
        // Unreachable from this closure: it holds no `ReadLimits` and never
        // admits against a request or byte budget, so it never constructs
        // `BudgetRefused`. Handled explicitly rather than through a wildcard
        // so a future budget check added here cannot silently fall through
        // as a store error.
        SingleFlightError::Upstream(crate::fetcher::CacheFetchError::BudgetRefused) => {
            SpanFetchError::Store {
                key: key.to_string(),
                source: StoreError::Transient(
                    "cache single-flight closure reported a budget refusal, which the RSPAN \
                     funnel never produces"
                        .to_string(),
                ),
            }
        }
        SingleFlightError::LeaderLost => SpanFetchError::Store {
            key: key.to_string(),
            source: StoreError::Transient(
                "cache single-flight leader lost before producing a result".to_string(),
            ),
        },
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use ravel_catalog::SegmentLevel;
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_rspan::record::{
        COL_PARENT_SPAN_ID, COL_SPAN_ID, COL_STATUS_CODE, COL_STATUS_MESSAGE, EVENTS_RAW_KEY,
        reconstruct_events_raw,
    };
    use ravel_rspan::{
        COL_NAME, COL_SERVICE_NAME, ObjectIdentity, RspanWriter, SpanEvent, StatusCode,
    };
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([9u8; 16]);

    /// Deliverable 1: `SpanSegmentFetcher::new` defaults to an unlimited budget
    /// (inert accounting) and `with_memory_budget` installs the shared one the
    /// engine hands every fetcher.
    #[test]
    fn with_memory_budget_installs_the_shared_budget() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let fetcher = SpanSegmentFetcher::new(store);
        assert_eq!(
            fetcher.memory_budget_for_test().limit(),
            u64::MAX,
            "the default budget is unlimited, so the accounting is inert"
        );
        let budget = Arc::new(ravel_memory::MemoryBudget::new(1 << 20));
        let fetcher = fetcher.with_memory_budget(budget.clone());
        assert!(Arc::ptr_eq(fetcher.memory_budget_for_test(), &budget));
    }

    /// The columnar exit's projection for these tests: every FIXED column, and
    /// nothing dynamic. It decodes `trace_id`, `span_id`, `parent_span_id`,
    /// `name`, `start_ts`, `end_ts`, `status_code`, `status_message`, and
    /// `service_name`, and excludes ONLY the attribute pages (`attrs_raw` and the
    /// dynamic per-key columns) and the four event pages. That exclusion set is
    /// exactly the ADR-0110 win, so the divergence a test sees is attributable to
    /// attribute and event pages alone -- see the vacuity note on the acceptance
    /// test.
    const FIXED_PROJECTION: [u32; 9] = [
        COL_TRACE_ID,
        COL_SPAN_ID,
        COL_PARENT_SPAN_ID,
        COL_NAME,
        COL_START_TS,
        COL_END_TS,
        COL_STATUS_CODE,
        COL_STATUS_MESSAGE,
        COL_SERVICE_NAME,
    ];

    /// A span carrying both attribute pages (a lifted `service.name` plus two
    /// dynamic per-key attributes) and an event page (one `_events_raw` event),
    /// so every block a corpus of these produces has attribute and event pages
    /// the fixed-column projection excludes.
    fn span_with_attrs_and_events(
        trace_id: [u8; 16],
        span_id: [u8; 8],
        start: i64,
        end: i64,
    ) -> SpanRecord {
        let event = SpanEvent {
            ts_ns: start + 1,
            name: "boom".to_string(),
            attrs_blob: vec![0x0a, 0x03, b'a', b'b', b'c'],
        };
        SpanRecord {
            trace_id,
            span_id,
            parent_span_id: None,
            name: "op".to_string(),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: vec![
                ("service.name".to_string(), "api".to_string()),
                ("http.method".to_string(), "GET".to_string()),
                ("http.route".to_string(), "/v1/spans".to_string()),
                (EVENTS_RAW_KEY.to_string(), reconstruct_events_raw(&[event])),
            ],
        }
    }

    /// An attribute-free, event-free span: only fixed columns, so a block of
    /// these carries no attribute or event page at all.
    fn bare_span(trace_id: [u8; 16], span_id: [u8; 8], start: i64, end: i64) -> SpanRecord {
        SpanRecord {
            trace_id,
            span_id,
            parent_span_id: None,
            name: "op".to_string(),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: Vec::new(),
        }
    }

    /// Write one RSPAN object holding `records` with small blocks
    /// (`block_target_records = 2`, so several records make several blocks) and
    /// return its `SegmentRef`. The footer carries [`TENANT`], so the
    /// tenant-checked funnels accept it.
    async fn write_object(store: &MemoryStore, key: u64, records: &[SpanRecord]) -> SegmentRef {
        let cfg = RspanConfig {
            block_target_records: 2,
            ..RspanConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [4u8; 16],
            writer_epoch: 1,
            writer_seq: key,
        };
        let mut writer = RspanWriter::new(cfg, identity);
        for r in records {
            writer.push(r.clone());
        }
        let bytes = writer.finish().expect("finish rspan object");
        let size = bytes.len() as u64;
        let object_key = format!("spans/{key}.rspan");
        store
            .put(&object_key, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put span object");
        let min = records
            .iter()
            .map(|r| r.start_ts_ns)
            .min()
            .expect("nonempty");
        let max = records.iter().map(|r| r.end_ts_ns).max().expect("nonempty");
        SegmentRef {
            data_object_key: object_key,
            object_size: size,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: [key as u8; 32],
            writer_id: Uuid::from_u128(u128::from(key) + 1),
            writer_epoch: 1,
            writer_seq: key,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_rspan::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    fn trace(n: u8) -> [u8; 16] {
        [n; 16]
    }
    fn span(n: u8) -> [u8; 8] {
        [n; 8]
    }

    /// The full-window query that prunes nothing, so both exits scan every block.
    fn all_query() -> SpanQuery {
        SpanQuery::ts_range(i64::MIN, i64::MAX)
    }

    /// Acceptance test (ADR-0110 decision 7): over a corpus whose blocks carry
    /// attribute and event pages, the row and columnar exits fetch the SAME page
    /// bytes but the columnar exit DECODES strictly fewer, because its
    /// fixed-column projection skips the attribute and event pages.
    ///
    /// The corpus MUST carry attribute and event pages. Over an attribute-free,
    /// event-free corpus the fixed-column projection excludes no page the block
    /// actually holds (every fixed column is projected), both exits decode the
    /// same bytes, and `page_bytes_decoded` is EQUAL -- so the strict-lower
    /// assertion would pass vacuously for even a broken implementation that never
    /// skipped a page. `attrs_free_corpus_makes_the_divergence_vacuous` below
    /// pins that boundary. Here every span carries two dynamic attributes and an
    /// event, so every block carries the pages the projection skips.
    #[tokio::test]
    async fn columnar_and_row_exits_agree_on_page_bytes_fetched_and_diverge_on_decoded() {
        let store = Arc::new(MemoryStore::new());
        let records: Vec<SpanRecord> = (0..6)
            .map(|i| {
                span_with_attrs_and_events(
                    trace(i + 1),
                    span(i + 1),
                    100 + i64::from(i),
                    200 + i64::from(i),
                )
            })
            .collect();
        let seg = write_object(&store, 0, &records).await;
        let fetcher = SpanSegmentFetcher::new(store);
        let query = all_query();

        let row_acct = QueryAccounting::new();
        let row_out = fetcher
            .fetch_accounted(&seg, TENANT, &query, None, None, &[], &row_acct)
            .await
            .expect("row fetch")
            .expect("row output");

        let col_acct = QueryAccounting::new();
        let mut scan = fetcher
            .fetch_accounted_columnar(
                &seg,
                TENANT,
                &query,
                None,
                None,
                &[],
                &FIXED_PROJECTION,
                &col_acct,
            )
            .await
            .expect("columnar fetch")
            .expect("columnar scan");
        let mut col_rows = 0usize;
        while let Some(block) = scan.next_block().expect("next block") {
            col_rows += block.rows.len();
        }
        drop(scan);

        let row = row_acct.snapshot();
        let col = col_acct.snapshot();

        assert!(row.page_bytes_fetched > 0, "fixture blocks carry pages");
        assert_eq!(
            row.page_bytes_decoded, row.page_bytes_fetched,
            "the row exit decodes every page (BlockProjection::All)"
        );
        assert_eq!(
            col.page_bytes_fetched, row.page_bytes_fetched,
            "the object is fetched whole either way: page_bytes_fetched is identical"
        );
        assert!(
            col.page_bytes_decoded < row.page_bytes_decoded,
            "the columnar exit skips attribute and event page decode: col decoded {} vs row decoded {}",
            col.page_bytes_decoded,
            row.page_bytes_decoded,
        );
        assert_eq!(
            col_rows,
            row_out.records.len(),
            "both exits select the identical surviving-row multiset"
        );
    }

    /// Issue #669: the row exit reports the page COUNTS its decode performed, so
    /// its caller can publish the same `pages_decoded`/`pages_skipped` metrics
    /// the columnar exit's caller publishes. Exact figures, from the fixture's
    /// geometry: 6 spans cut every 2 records is 3 blocks, and each block stages
    /// 13 pages (`trace_id`, `span_id`, `name`, `start_ts`, `end_ts`,
    /// `status_code`, `service_name`, the two dynamic attribute columns, and the
    /// four event columns; `parent_span_id`, `status_message`, and `attrs_raw`
    /// have no value on any fixture span, and a column with no present value
    /// stages no page). The row exit decodes all 39 and skips none; the
    /// fixed-column projection splits those same 39 into 21 decoded and 18
    /// skipped.
    #[tokio::test]
    async fn row_exit_reports_the_page_counts_the_columnar_exit_splits() {
        let store = Arc::new(MemoryStore::new());
        let records: Vec<SpanRecord> = (0..6)
            .map(|i| {
                span_with_attrs_and_events(
                    trace(i + 1),
                    span(i + 1),
                    100 + i64::from(i),
                    200 + i64::from(i),
                )
            })
            .collect();
        let seg = write_object(&store, 0, &records).await;
        let fetcher = SpanSegmentFetcher::new(store);
        let query = all_query();

        let acct = QueryAccounting::new();
        let row_out = fetcher
            .fetch_accounted(&seg, TENANT, &query, None, None, &[], &acct)
            .await
            .expect("row fetch")
            .expect("row output");
        assert_eq!(row_out.stats.blocks_scanned, 3, "three blocks scanned");
        assert_eq!(
            (row_out.pages_decoded, row_out.pages_skipped),
            (39, 0),
            "the row exit decodes every page of every block it scans"
        );

        let mut scan = fetcher
            .fetch_accounted_columnar(
                &seg,
                TENANT,
                &query,
                None,
                None,
                &[],
                &FIXED_PROJECTION,
                &QueryAccounting::new(),
            )
            .await
            .expect("columnar fetch")
            .expect("columnar scan");
        while scan.next_block().expect("next block").is_some() {}
        assert_eq!(
            (scan.pages_decoded(), scan.pages_skipped()),
            (21, 18),
            "the fixed-column projection's split of those same pages"
        );
        assert_eq!(
            scan.pages_decoded() + scan.pages_skipped(),
            row_out.pages_decoded,
            "both exits walk the same pages; only the split differs"
        );
    }

    /// The boundary the acceptance test's comment names: with an attribute-free,
    /// event-free corpus the fixed-column projection excludes no page the block
    /// carries, so both exits decode identical bytes and the strict-lower
    /// assertion would be vacuous. This test asserts the EQUALITY, so it stands
    /// guard: if a future change makes the fixed projection skip an
    /// always-present page, this breaks and forces the vacuity note to be
    /// revisited.
    #[tokio::test]
    async fn attrs_free_corpus_makes_the_divergence_vacuous() {
        let store = Arc::new(MemoryStore::new());
        let records: Vec<SpanRecord> = (0..6)
            .map(|i| {
                bare_span(
                    trace(i + 1),
                    span(i + 1),
                    100 + i64::from(i),
                    200 + i64::from(i),
                )
            })
            .collect();
        let seg = write_object(&store, 1, &records).await;
        let fetcher = SpanSegmentFetcher::new(store);
        let query = all_query();

        let acct = QueryAccounting::new();
        let mut scan = fetcher
            .fetch_accounted_columnar(
                &seg,
                TENANT,
                &query,
                None,
                None,
                &[],
                &FIXED_PROJECTION,
                &acct,
            )
            .await
            .expect("columnar fetch")
            .expect("columnar scan");
        while scan.next_block().expect("next block").is_some() {}
        drop(scan);

        let snap = acct.snapshot();
        assert!(
            snap.page_bytes_fetched > 0,
            "bare spans still have fixed-column pages"
        );
        assert_eq!(
            snap.page_bytes_decoded, snap.page_bytes_fetched,
            "with no attribute or event page, the projection skips nothing"
        );
    }

    /// Abandonment test (ADR-0107 decision 4): a columnar scan dropped after one
    /// consumed block still folds that block's page bytes into accounting, and
    /// the folded magnitude is EXACTLY the consumed block's decoded/fetched
    /// sizes, not merely non-zero. A non-zero assertion would pass just as well
    /// on a fraction-of-the-truth undercount, which is how an accounting bug
    /// survives a green suite; pinning the magnitude catches it.
    #[tokio::test]
    async fn columnar_scan_folds_exactly_one_consumed_blocks_bytes_on_early_drop() {
        let store = Arc::new(MemoryStore::new());
        let records: Vec<SpanRecord> = (0..6)
            .map(|i| {
                span_with_attrs_and_events(
                    trace(i + 1),
                    span(i + 1),
                    100 + i64::from(i),
                    200 + i64::from(i),
                )
            })
            .collect();
        let seg = write_object(&store, 2, &records).await;
        let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
        let fetcher = SpanSegmentFetcher::new(store_dyn);
        let query = all_query();

        // The first block's own fetched/decoded page bytes, computed directly
        // from that block under the same projection. This is the magnitude the
        // fold must land on exactly after one consumed block.
        let bytes = store
            .get(&seg.data_object_key, GetRange::Full)
            .await
            .expect("get object")
            .data;
        let (candidates, _stats) = fetcher
            .plan_candidates(&bytes, &query, None, None, &[])
            .expect("plan candidates");
        assert!(
            candidates.len() >= 2,
            "multi-block corpus for an abandonment test"
        );
        let (start, end, crc) = candidates[0];
        let projection = BlockProjection::columnar(&FIXED_PROJECTION);
        let block0 =
            decode_block_accounted(&bytes[start..end], crc, &projection).expect("decode block 0");
        let (exp_fetched, exp_decoded) = (block0.page_bytes_fetched, block0.page_bytes_decoded);
        assert!(
            exp_decoded < exp_fetched,
            "block 0 carries attribute/event pages the projection skips"
        );

        let acct = QueryAccounting::new();
        let mut scan = fetcher
            .fetch_accounted_columnar(
                &seg,
                TENANT,
                &query,
                None,
                None,
                &[],
                &FIXED_PROJECTION,
                &acct,
            )
            .await
            .expect("columnar fetch")
            .expect("columnar scan");
        let _first = scan.next_block().expect("first block").expect("some block");
        // The fold is deferred to exhaustion or drop: nothing is accounted yet.
        assert_eq!(
            acct.snapshot().page_bytes_fetched,
            0,
            "page-byte fold is deferred to finish(), not done per block"
        );
        drop(scan);

        let snap = acct.snapshot();
        assert_eq!(
            snap.page_bytes_fetched, exp_fetched,
            "one consumed block's fetched page bytes are folded on drop"
        );
        assert_eq!(
            snap.page_bytes_decoded, exp_decoded,
            "one consumed block's decoded page bytes are folded on drop"
        );
    }

    /// The row and columnar exits return the identical surviving-row set for a
    /// `trace_id`-filtered query too, proving the shared predicate
    /// ([`surviving_rows`]) matches the old per-record `matches` on both axes
    /// (ts window and trace_id equality).
    #[tokio::test]
    async fn both_exits_agree_on_surviving_rows_under_a_trace_filter() {
        let store = Arc::new(MemoryStore::new());
        let records = vec![
            span_with_attrs_and_events(trace(1), span(1), 100, 200),
            span_with_attrs_and_events(trace(2), span(2), 100, 200),
            span_with_attrs_and_events(trace(3), span(3), 100, 200),
            // Same trace as the query, but its ts window is disjoint, so the ts
            // half of the predicate must exclude it.
            span_with_attrs_and_events(trace(2), span(4), 400, 500),
        ];
        let seg = write_object(&store, 3, &records).await;
        let fetcher = SpanSegmentFetcher::new(store);

        // With `block_target_records = 2`, trace 1 and trace 2 land in one block
        // whose trace range `[1, 2]` brackets the queried trace 2: block-level
        // pruning keeps the block, so it is the per-row predicate that must drop
        // the trace-1 row. Exactly the in-window trace-2 span survives.
        let query = SpanQuery {
            trace_id: Some(trace(2)),
            ts_min: 50,
            ts_max: 300,
        };

        let row_acct = QueryAccounting::new();
        let row_out = fetcher
            .fetch_accounted(&seg, TENANT, &query, None, None, &[], &row_acct)
            .await
            .expect("row fetch")
            .expect("row output");
        assert_eq!(
            row_out.records.len(),
            1,
            "exactly one span matches trace and ts"
        );
        assert_eq!(
            row_out.records[0].record.trace_id,
            trace(2),
            "the surviving row is trace 2"
        );
        assert_eq!(
            row_out.records[0].record.start_ts_ns, 100,
            "and it is the in-window trace-2 span, not the ts-excluded one"
        );

        let col_acct = QueryAccounting::new();
        let mut scan = fetcher
            .fetch_accounted_columnar(
                &seg,
                TENANT,
                &query,
                None,
                None,
                &[],
                &FIXED_PROJECTION,
                &col_acct,
            )
            .await
            .expect("columnar fetch")
            .expect("columnar scan");
        let mut col_rows = 0usize;
        while let Some(block) = scan.next_block().expect("next block") {
            col_rows += block.rows.len();
        }
        drop(scan);

        assert_eq!(
            col_rows,
            row_out.records.len(),
            "both exits select the identical surviving-row multiset under a trace filter"
        );
    }

    /// ADR-1195: `SpanSegmentFetcher` had no GET concurrency bound at all
    /// before this change. `with_max_concurrent_gets(1)` must cap in-flight
    /// GETs at exactly 1 -- proven the same way as the RSEG/RLOG shared-bound
    /// tests (`crate::fetcher`'s `shared_get_limiter_bounds_peak_concurrent_gets_to_one`):
    /// a `FaultStore` hold gate observes the store's own in-flight GET count
    /// directly, so this pins the fetcher's own bookkeeping against the
    /// store's view, not just against itself.
    #[tokio::test]
    async fn get_limiter_bounds_peak_concurrent_gets_to_permit_count() {
        use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence};

        let memory = MemoryStore::new();
        let records = vec![span_with_attrs_and_events(trace(1), span(1), 100, 200)];
        let seg = write_object(&memory, 0, &records).await;
        let fault = Arc::new(FaultStore::new(memory, FaultPlan::default()));
        let gate: GateHandle =
            fault.hold(ravel_object_store::fault::Op::Get, None, Occurrence::Always);
        let backend: Arc<dyn ObjectStoreBackend> = fault;

        let fetcher = SpanSegmentFetcher::new(backend).with_max_concurrent_gets(1);
        let query = all_query();

        let fetcher_a = fetcher.clone();
        let seg_a = seg.clone();
        let query_a = query;
        let handle_a =
            tokio::spawn(async move { fetcher_a.fetch(&seg_a, &query_a, None, None, &[]).await });
        let fetcher_b = fetcher.clone();
        let seg_b = seg.clone();
        let query_b = query;
        let handle_b =
            tokio::spawn(async move { fetcher_b.fetch(&seg_b, &query_b, None, None, &[]).await });

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
            "the second GET must never become in-flight while the one permit is held"
        );
        assert_eq!(
            gate.held_count(),
            1,
            "one permit must cap in-flight GETs at exactly 1"
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
            "the second GET must be the only one held once the first releases"
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

    /// The permits=1 test above cannot distinguish "at most 1", "exactly 1",
    /// and "exactly the permit count": all three coincide at 1. This pins the
    /// count itself: three concurrent fetches from two INDEPENDENTLY
    /// constructed `SpanSegmentFetcher`s sharing one `GetLimiter::new(2)`
    /// (ADR-1195) must peak at EXACTLY 2 in-flight GETs, never all 3.
    #[tokio::test]
    async fn shared_get_limiter_bounds_three_fetches_to_permit_count_two() {
        use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence};

        let memory = MemoryStore::new();
        let records = vec![span_with_attrs_and_events(trace(1), span(1), 100, 200)];
        let seg = write_object(&memory, 0, &records).await;
        let fault = Arc::new(FaultStore::new(memory, FaultPlan::default()));
        let gate: GateHandle =
            fault.hold(ravel_object_store::fault::Op::Get, None, Occurrence::Always);
        let backend: Arc<dyn ObjectStoreBackend> = fault;

        let shared = Arc::new(crate::GetLimiter::new(2).expect("2 permits is valid"));
        let fetcher_a = SpanSegmentFetcher::new(backend.clone()).with_get_limiter(shared.clone());
        let fetcher_b = SpanSegmentFetcher::new(backend).with_get_limiter(shared);
        let query = all_query();

        let seg_a = seg.clone();
        let query_a = query;
        let fa1 = fetcher_a.clone();
        let handle_a1 =
            tokio::spawn(async move { fa1.fetch(&seg_a, &query_a, None, None, &[]).await });
        let seg_a2 = seg.clone();
        let query_a2 = query;
        let fa2 = fetcher_a.clone();
        let handle_a2 =
            tokio::spawn(async move { fa2.fetch(&seg_a2, &query_a2, None, None, &[]).await });
        let seg_b = seg.clone();
        let query_b = query;
        let handle_b =
            tokio::spawn(async move { fetcher_b.fetch(&seg_b, &query_b, None, None, &[]).await });

        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(2))
            .await
            .expect("two of the three fetches issue their GET within 30 s");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                gate.wait_until_held(3)
            )
            .await
            .is_err(),
            "two permits must cap in-flight GETs at exactly 2, never all 3"
        );
        assert_eq!(
            gate.held_count(),
            2,
            "peak in-flight GETs must be exactly 2, not 1 and not 3"
        );

        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
            .await
            .expect("releasing two permits lets the third GET proceed within 30 s");
        assert_eq!(
            gate.held_count(),
            1,
            "the third GET must be the only one held once the first two release"
        );
        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }

        let a1 = tokio::time::timeout(std::time::Duration::from_secs(30), handle_a1)
            .await
            .expect("fetch a1 completes within 30 s")
            .expect("join fetch a1")
            .expect("fetch a1")
            .expect("fetch a1 found the segment relevant");
        let a2 = tokio::time::timeout(std::time::Duration::from_secs(30), handle_a2)
            .await
            .expect("fetch a2 completes within 30 s")
            .expect("join fetch a2")
            .expect("fetch a2")
            .expect("fetch a2 found the segment relevant");
        let b = tokio::time::timeout(std::time::Duration::from_secs(30), handle_b)
            .await
            .expect("fetch b completes within 30 s")
            .expect("join fetch b")
            .expect("fetch b")
            .expect("fetch b found the segment relevant");
        assert_eq!(a1.records.len(), 1);
        assert_eq!(a2.records.len(), 1);
        assert_eq!(b.records.len(), 1);
    }

    /// ADR-1702 follow-up task 7, RSPAN site: with the byte floor at 0, the
    /// accounted row exit decodes each candidate block as exactly one
    /// `SpanBlock` job, reaching exhaustion submits none, nothing runs inline,
    /// and the rows equal the ungated fetch's. Six records at two per block
    /// make three blocks.
    ///
    /// FLIP: in `next_block_on_gate`, read the gate as `None` so every block
    /// takes the inline `next_block`; the `SpanBlock` assertion then reads
    /// `left: (0, 0), right: (3, 0)`.
    #[tokio::test]
    async fn span_block_decodes_run_through_the_read_gate() {
        use crate::read_gate_test_support::{floor_zero_gate, site_counts, total_inline};
        let store = Arc::new(MemoryStore::new());
        let records: Vec<SpanRecord> = (0..6)
            .map(|i| {
                span_with_attrs_and_events(
                    trace(i + 1),
                    span(i + 1),
                    100 + i64::from(i),
                    200 + i64::from(i),
                )
            })
            .collect();
        let seg = write_object(&store, 0, &records).await;
        let query = all_query();

        let inline = SpanSegmentFetcher::new(store.clone())
            .fetch_accounted(
                &seg,
                TENANT,
                &query,
                None,
                None,
                &[],
                &QueryAccounting::new(),
            )
            .await
            .expect("ungated fetch")
            .expect("relevant");
        let gate = floor_zero_gate();
        let gated = SpanSegmentFetcher::new(store)
            .with_read_gate(gate.clone())
            .fetch_accounted(
                &seg,
                TENANT,
                &query,
                None,
                None,
                &[],
                &QueryAccounting::new(),
            )
            .await
            .expect("gated fetch")
            .expect("relevant");
        assert_eq!(inline.stats.blocks_scanned, 3);
        assert_eq!(gated.records, inline.records);
        assert_eq!(gated.stats, inline.stats);
        assert_eq!(site_counts(&gate, ReadSite::SpanBlock), (3, 0));
        assert_eq!(total_inline(&gate), 0);
    }

    /// A block's job size is the sum of its pages' uncompressed lengths, and a
    /// header that does not parse sizes the block as unbounded. The expected
    /// sum is measured independently: each page's stored bytes are
    /// decompressed here and their lengths added, which is what `read_block`
    /// checks every page's `uncomp_len` against. Long, repetitive span names
    /// make the name page compress, so summing stored lengths would not pass.
    ///
    /// FLIP: summing each page's stored `len` instead of its `uncomp_len` in
    /// `block_uncompressed_len` reads `left: 323, right: 11321`.
    #[test]
    fn span_block_job_size_reads_the_header() {
        let records: Vec<SpanRecord> = (0..64)
            .map(|i| SpanRecord {
                name: format!("operation-{i}-{}", "abcdefgh".repeat(20)),
                ..bare_span(trace(1), span(1), 100 + i, 200 + i)
            })
            .collect();
        let mut writer = RspanWriter::new(
            RspanConfig::default(),
            ObjectIdentity {
                tenant_hash: TENANT.0,
                shard: 0,
                writer_id: [4u8; 16],
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for r in &records {
            writer.push(r.clone());
        }
        let bytes = writer.finish().expect("finish");
        let reader = RspanReader::new(&bytes, &RspanConfig::default()).expect("reader");
        let entry = reader.skip_index().blocks[0].clone();
        let footer = open(&bytes).expect("footer");
        let blocks = footer.section(kind::BLOCKS).expect("BLOCKS");
        let (start, end) = abs_block_range(blocks.offset, blocks.len, &entry).expect("range");
        let block_bytes = &bytes[start..end];
        let block =
            read_block(block_bytes, entry.block_crc32c, DEFAULT_MAX_UNCOMP).expect("decode");
        assert_eq!(block.record_count(), 64);

        let mut pos = 0usize;
        let _records = get_uvarint(block_bytes, &mut pos).expect("record count");
        let page_count = get_uvarint(block_bytes, &mut pos).expect("page count");
        let mut pages = Vec::new();
        for _ in 0..page_count {
            let _column = get_uvarint(block_bytes, &mut pos).expect("column id");
            let comp = block_bytes[pos + 1];
            pos += 2;
            let len = get_uvarint(block_bytes, &mut pos).expect("len");
            let _uncomp = get_uvarint(block_bytes, &mut pos).expect("uncomp_len");
            pages.push((comp, len as usize));
        }
        let dynamic = get_uvarint(block_bytes, &mut pos).expect("dynamic columns");
        for _ in 0..dynamic {
            let _column = get_uvarint(block_bytes, &mut pos).expect("column id");
            let name_len = get_uvarint(block_bytes, &mut pos).expect("name len");
            pos += name_len as usize;
        }
        let (mut stored, mut decoded) = (0u64, 0u64);
        for (comp, len) in pages {
            let page = &block_bytes[pos..pos + len];
            pos += len;
            stored += len as u64;
            decoded += match comp {
                0 => len as u64,
                _ => zstd::bulk::decompress(page, 1 << 26)
                    .expect("zstd page")
                    .len() as u64,
            };
        }
        assert_eq!(pos, block_bytes.len(), "every payload byte is a page");
        assert!(decoded > stored, "a page compressed: {decoded} vs {stored}");
        assert_eq!(block_uncompressed_len(block_bytes), decoded);
        assert_eq!(block_uncompressed_len(&[0x80]), u64::MAX);
    }

    /// A gated block decode that never ran leaves the scan lost: the call that
    /// submitted it fails transient, and every later call, gated or inline,
    /// fails with that same class instead of handing out the next block of a
    /// partial segment. The scan is opened on a live runtime and drained on one
    /// that has shut down, whose blocking pool cancels each job the gate
    /// dispatches.
    ///
    /// FLIP: drop the `lost` check from `next_candidate` and the second call
    /// takes the next block and submits its own job, so the submitted-job
    /// assertion reads `left: (2, 0), right: (1, 0)`.
    #[test]
    fn a_scan_fails_after_a_cancelled_block_decode_rather_than_skip_it() {
        use crate::read_gate_test_support::{floor_zero_gate, site_counts};
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store = Arc::new(MemoryStore::new());
        let records: Vec<SpanRecord> = (0..6)
            .map(|i| bare_span(trace(i + 1), span(i + 1), 100, 200))
            .collect();
        let gate = floor_zero_gate();
        let fetcher = SpanSegmentFetcher::new(store.clone()).with_read_gate(gate.clone());
        let mut scan = rt.block_on(async {
            let seg = write_object(&store, 0, &records).await;
            fetcher
                .fetch_accounted_columnar(
                    &seg,
                    TENANT,
                    &all_query(),
                    None,
                    None,
                    &[],
                    &[],
                    &QueryAccounting::new(),
                )
                .await
                .expect("open")
                .expect("relevant")
        });
        assert_eq!(scan.remaining_blocks(), 3);
        let handle = rt.handle().clone();
        rt.shutdown_background();
        let _entered = handle.enter();

        match futures::executor::block_on(scan.next_block_on_gate()) {
            Err(SpanFetchError::Store {
                source: StoreError::Transient(message),
                ..
            }) => assert!(message.contains("cancelled"), "{message}"),
            other => panic!("expected the cancelled job's transient error, got {other:?}"),
        }
        assert_eq!(site_counts(&gate, ReadSite::SpanBlock), (1, 0));
        // The lost scan keeps the class of the failure that lost it, so a
        // later call repeats the transient error rather than reporting a
        // permanent one the HTTP layer redacts to a 503.
        match futures::executor::block_on(scan.next_block_on_gate()) {
            Err(SpanFetchError::Store {
                source: StoreError::Transient(message),
                ..
            }) => assert!(message.contains("cancelled"), "{message}"),
            other => panic!("expected the same transient class, got {other:?}"),
        }
        assert_eq!(
            site_counts(&gate, ReadSite::SpanBlock),
            (1, 0),
            "no later block was submitted"
        );
        match scan.next_block() {
            Err(SpanFetchError::Store {
                source: StoreError::Transient(message),
                ..
            }) => assert!(message.contains("cancelled"), "{message}"),
            other => panic!("the inline exit reports that class too, got {other:?}"),
        }
    }

    /// A gated block decode that panicked surfaces as the span fetcher's decode
    /// error, the `Corrupt` class a corrupt block reports.
    ///
    /// FLIP: move `CpuGateError::Panicked` into the other arm of
    /// `span_gate_failed` and the match panics with a transient `Store` error.
    #[tokio::test]
    async fn a_panicked_span_block_job_is_the_decode_error() {
        let gate = crate::read_gate_test_support::floor_zero_gate();
        let err = gate
            .run(ReadSite::SpanBlock, JobSize::Bytes(1), || -> u8 {
                panic!("block decode panicked")
            })
            .await
            .expect_err("a panicking job fails");
        match span_gate_failed("spans/0.rspan", err) {
            SpanFetchError::Corrupt {
                source: SpanSegError::Corrupted(message),
                ..
            } => assert!(message.contains("panicked"), "{message}"),
            other => panic!("expected the decode error, got {other:?}"),
        }
    }

    /// Records the `(key, range)` of every GET and forwards it to a
    /// `MemoryStore`, optionally truncating one chosen GET's answer.
    struct GetLog {
        inner: MemoryStore,
        gets: std::sync::Mutex<Vec<(String, GetRange)>>,
        truncate: std::sync::Mutex<Option<GetRange>>,
    }

    impl GetLog {
        fn new(inner: MemoryStore) -> Self {
            GetLog {
                inner,
                gets: std::sync::Mutex::new(Vec::new()),
                truncate: std::sync::Mutex::new(None),
            }
        }

        /// The next GET of exactly `range` returns one byte short.
        fn truncate_next(&self, range: GetRange) {
            *self.truncate.lock().expect("truncate") = Some(range);
        }

        /// The ranges GET'd on `key`, in issue order, clearing the log for
        /// that key.
        fn take(&self, key: &str) -> Vec<GetRange> {
            let mut gets = self.gets.lock().expect("get log");
            let (mine, rest): (Vec<_>, Vec<_>) = gets.drain(..).partition(|(k, _)| k == key);
            *gets = rest;
            mine.into_iter().map(|(_, range)| range).collect()
        }

        fn total(&self) -> usize {
            self.gets.lock().expect("get log").len()
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for GetLog {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }
        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            self.gets
                .lock()
                .expect("get log")
                .push((key.to_string(), range));
            let mut got = self.inner.get(key, range).await?;
            let mut truncate = self.truncate.lock().expect("truncate");
            if *truncate == Some(range) {
                *truncate = None;
                got.data = got.data.slice(..got.data.len().saturating_sub(1));
            }
            Ok(got)
        }
        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }
        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// Two RSPAN objects over two-record blocks. Object A holds traces 1..=5,
    /// trace 3 with four spans and the rest with two, so its six blocks are
    /// `[t1] [t2] [t3] [t3] [t4] [t5]` and trace 3 spans the adjacent blocks 2
    /// and 3. Object B holds traces 6..=10 and never trace 3.
    struct TraceFixture {
        memory: Option<MemoryStore>,
        a: SegmentRef,
        b: SegmentRef,
        /// The tail probe length the fetcher is built with: the longer of the
        /// two objects' footer-plus-sections tails, so the probe holds every
        /// section and the ranged path's first GET is exactly the probe.
        probe: u64,
        /// Object A's absolute `[start, end)` of each block, from its footer.
        blocks_a: Vec<(u64, u64)>,
    }

    impl TraceFixture {
        /// Trace 3's run in object A: block 2's start to block 3's end.
        fn run(&self) -> GetRange {
            GetRange::Range(self.blocks_a[2].0, self.blocks_a[3].1)
        }
        fn block(&self, b: usize) -> GetRange {
            GetRange::Range(self.blocks_a[b].0, self.blocks_a[b].1)
        }
        fn probe_of(&self, seg: &SegmentRef) -> GetRange {
            GetRange::Range(seg.object_size - self.probe, seg.object_size)
        }
    }

    /// The BLOCKS section's end and every block's absolute range, read from
    /// the whole object through `RspanReader`, independently of the ranged
    /// path under test.
    fn layout(bytes: &[u8]) -> (u64, Vec<(u64, u64)>, Vec<usize>) {
        let footer = open(bytes).expect("footer");
        let blocks = *footer.section(kind::BLOCKS).expect("BLOCKS section");
        let reader = RspanReader::new(bytes, &RspanConfig::default()).expect("reader");
        let skip = reader.skip_index();
        let ranges = skip
            .blocks
            .iter()
            .map(|e| {
                let (s, e) = abs_block_range(blocks.offset, blocks.len, e).expect("block range");
                (s as u64, e as u64)
            })
            .collect();
        let trace3 = skip.candidate_blocks(Some(&trace(3)), i64::MIN, i64::MAX, None, None);
        (blocks.offset + blocks.len, ranges, trace3)
    }

    async fn trace_fixture() -> TraceFixture {
        let memory = MemoryStore::new();
        let mut a_records = Vec::new();
        for (t, n) in [(1u8, 2u8), (2, 2), (3, 4), (4, 2), (5, 2)] {
            for s in 0..n {
                let at = 100 + i64::from(s);
                a_records.push(bare_span(trace(t), [t * 16 + s; 8], at, at + 50));
            }
        }
        let mut b_records = Vec::new();
        for t in 6u8..=10 {
            for s in 0..2u8 {
                let at = 100 + i64::from(s);
                b_records.push(bare_span(trace(t), [t * 16 + s; 8], at, at + 50));
            }
        }
        let a = write_object(&memory, 1, &a_records).await;
        let b = write_object(&memory, 2, &b_records).await;
        let whole = |seg: &SegmentRef| {
            let memory = &memory;
            let key = seg.data_object_key.clone();
            async move {
                memory
                    .get(&key, GetRange::Full)
                    .await
                    .expect("whole object")
                    .data
            }
        };
        let (a_end, blocks_a, a_trace3) = layout(&whole(&a).await);
        let (b_end, _, b_trace3) = layout(&whole(&b).await);
        assert_eq!(blocks_a.len(), 6, "fixture geometry: six blocks in A");
        assert_eq!(
            a_trace3,
            vec![2, 3],
            "fixture geometry: trace 3 is blocks 2..=3"
        );
        assert!(
            b_trace3.is_empty(),
            "fixture geometry: B cannot hold trace 3"
        );
        let probe = (a.object_size - a_end).max(b.object_size - b_end);
        assert!(
            blocks_a[3].1 <= a.object_size - probe,
            "fixture geometry: trace 3's run lies outside the probe"
        );
        TraceFixture {
            memory: Some(memory),
            a,
            b,
            probe,
            blocks_a,
        }
    }

    /// ADR-2707 decision 7: a trace lookup reads the tail probe of each object
    /// and exactly one ranged GET of the trace's block run where the trace
    /// lives, and no block bytes where it does not.
    ///
    /// FLIP: reading the run through `GetRange::Full`, or issuing one GET per
    /// candidate block instead of one per run, fails the object-A range list;
    /// the pre-change fetcher's single `GetRange::Full` per object fails both.
    #[tokio::test]
    async fn trace_lookup_reads_only_the_trace_block_span() {
        let mut fx = trace_fixture().await;
        let log = Arc::new(GetLog::new(fx.memory.take().expect("memory")));
        let fetcher = SpanSegmentFetcher::new(log.clone()).with_trace_probe_bytes(fx.probe);
        let query = SpanQuery::trace(trace(3), i64::MIN, i64::MAX);
        let acct = QueryAccounting::new();

        let a_out = fetcher
            .fetch_accounted(&fx.a, TENANT, &query, None, None, &[], &acct)
            .await
            .expect("fetch A")
            .expect("A is ts-relevant");
        let b_out = fetcher
            .fetch_accounted(&fx.b, TENANT, &query, None, None, &[], &acct)
            .await
            .expect("fetch B")
            .expect("B is ts-relevant");

        assert_eq!(
            log.take(&fx.a.data_object_key),
            vec![fx.probe_of(&fx.a), fx.run()],
            "A: the tail probe, then one GET of exactly blocks 2..=3"
        );
        assert_eq!(
            log.take(&fx.b.data_object_key),
            vec![fx.probe_of(&fx.b)],
            "B: the tail probe and zero block GETs"
        );
        assert_eq!(log.total(), 0, "no other GET was issued");

        let run_len = fx.blocks_a[3].1 - fx.blocks_a[2].0;
        let snap = acct.snapshot();
        assert_eq!(snap.total_s3_requests(), 3);
        assert_eq!(snap.s3_requests(AccountedOp::Get), 3);
        assert_eq!(
            snap.s3_bytes(AccountedOp::Get),
            2 * fx.probe + run_len,
            "wire bytes are the two probes plus the run, as transferred"
        );

        assert_eq!(a_out.records.len(), 4, "every trace-3 span");
        assert!(a_out.records.iter().all(|r| r.record.trace_id == trace(3)));
        assert!(b_out.records.is_empty());
        assert_eq!(a_out.stats.blocks_total, 6);
        assert_eq!(a_out.stats.blocks_after_skip, 2);
        assert_eq!(a_out.stats.blocks_scanned, 2);
        assert_eq!(b_out.stats.blocks_total, 5);
        assert_eq!(b_out.stats.blocks_after_skip, 0);
    }

    /// The columnar exit takes the same ranged read and names it; a query with
    /// no `trace_id` still reads the whole object and names that.
    #[tokio::test]
    async fn columnar_trace_lookup_takes_the_ranged_path_and_names_it() {
        let mut fx = trace_fixture().await;
        let log = Arc::new(GetLog::new(fx.memory.take().expect("memory")));
        let fetcher = SpanSegmentFetcher::new(log.clone()).with_trace_probe_bytes(fx.probe);
        let acct = QueryAccounting::new();

        let query = SpanQuery::trace(trace(3), i64::MIN, i64::MAX);
        assert_eq!(SpanReadPath::for_query(&query).as_str(), "ranged-trace");
        let mut scan = fetcher
            .fetch_accounted_columnar(
                &fx.a,
                TENANT,
                &query,
                None,
                None,
                &[],
                &FIXED_PROJECTION,
                &acct,
            )
            .await
            .expect("columnar fetch")
            .expect("A is ts-relevant");
        assert_eq!(scan.read_path(), SpanReadPath::RangedTrace);
        let mut rows = 0;
        while let Some(block) = scan.next_block().expect("next block") {
            rows += block.rows.len();
        }
        assert_eq!(rows, 4);
        assert_eq!(
            log.take(&fx.a.data_object_key),
            vec![fx.probe_of(&fx.a), fx.run()]
        );

        let window = all_query();
        assert_eq!(SpanReadPath::for_query(&window).as_str(), "whole-object");
        let scan = fetcher
            .fetch_accounted_columnar(
                &fx.a,
                TENANT,
                &window,
                None,
                None,
                &[],
                &FIXED_PROJECTION,
                &acct,
            )
            .await
            .expect("columnar fetch")
            .expect("A is ts-relevant");
        assert_eq!(scan.read_path(), SpanReadPath::WholeObject);
        assert_eq!(log.take(&fx.a.data_object_key), vec![GetRange::Full]);
    }

    /// A caller-supplied block set is read as one ranged GET per contiguous
    /// run that holds a candidate block, and a set that is not a valid run in
    /// the object, or a set with no trace, is refused.
    #[tokio::test]
    async fn a_known_block_set_reads_one_range_per_contiguous_run() {
        let mut fx = trace_fixture().await;
        let log = Arc::new(GetLog::new(fx.memory.take().expect("memory")));
        let fetcher = SpanSegmentFetcher::new(log.clone()).with_trace_probe_bytes(fx.probe);
        let query = SpanQuery::trace(trace(3), i64::MIN, i64::MAX);
        let key = fx.a.data_object_key.clone();
        let fetch = |blocks: &'static [u32]| {
            let fetcher = fetcher.clone();
            let seg = fx.a.clone();
            async move {
                fetcher
                    .fetch_accounted_in_blocks(
                        &seg,
                        TENANT,
                        &query,
                        None,
                        None,
                        &[],
                        Some(blocks),
                        &PhaseAccounting::new(),
                    )
                    .await
            }
        };

        let out = fetch(&[3, 2, 3]).await.expect("fetch").expect("relevant");
        assert_eq!(out.records.len(), 4);
        assert_eq!(log.take(&key), vec![fx.probe_of(&fx.a), fx.run()]);

        let out = fetch(&[2]).await.expect("fetch").expect("relevant");
        assert_eq!(out.records.len(), 2, "block 2 holds two of trace 3's spans");
        assert_eq!(log.take(&key), vec![fx.probe_of(&fx.a), fx.block(2)]);

        // Block 1 holds only trace 2, so its run is skipped without a GET.
        let out = fetch(&[1, 3]).await.expect("fetch").expect("relevant");
        assert_eq!(out.records.len(), 2);
        assert_eq!(log.take(&key), vec![fx.probe_of(&fx.a), fx.block(3)]);

        let err = fetch(&[5, 6]).await.expect_err("block 6 does not exist");
        assert!(matches!(err, SpanFetchError::Corrupt { .. }), "{err:?}");
        assert_eq!(log.take(&key), vec![fx.probe_of(&fx.a)]);

        let err = fetcher
            .fetch_accounted_in_blocks(
                &fx.a,
                TENANT,
                &all_query(),
                None,
                None,
                &[],
                Some(&[2]),
                &PhaseAccounting::new(),
            )
            .await
            .expect_err("a block set without a trace_id");
        assert!(matches!(err, SpanFetchError::Corrupt { .. }), "{err:?}");
        assert_eq!(log.total(), 0, "refused before any GET");
    }

    /// One object whose blocks are `[t1] [t11] [t11] [t11] [t12]`, where trace
    /// 11's middle block holds its only long spans, so [`PRUNE_MIDDLE`] prunes
    /// block 2 alone. Returns the store, the object, every block's absolute
    /// range, and a probe length that holds every section and no block of
    /// trace 11.
    async fn pruned_middle_fixture() -> (MemoryStore, SegmentRef, Vec<(u64, u64)>, u64) {
        let memory = MemoryStore::new();
        let mut records = Vec::new();
        for t in [1u8, 12] {
            for s in 0..2u8 {
                let at = 100 + i64::from(s);
                records.push(bare_span(trace(t), [t * 16 + s; 8], at, at + 50));
            }
        }
        for s in 0..6u8 {
            let at = 100 + i64::from(s);
            let duration = if s == 2 || s == 3 { 5_000 } else { 50 };
            records.push(bare_span(trace(11), [176 + s; 8], at, at + duration));
        }
        let seg = write_object(&memory, 3, &records).await;
        let whole = memory
            .get(&seg.data_object_key, GetRange::Full)
            .await
            .expect("whole object")
            .data;
        let (blocks_end, blocks, _) = layout(&whole);
        let reader = RspanReader::new(&whole, &RspanConfig::default()).expect("reader");
        let skip = reader.skip_index();
        assert_eq!(blocks.len(), 5, "fixture geometry: five blocks");
        assert_eq!(
            skip.candidate_blocks(Some(&trace(11)), i64::MIN, i64::MAX, None, None),
            vec![1, 2, 3],
            "fixture geometry: trace 11 is blocks 1..=3"
        );
        assert_eq!(
            skip.candidate_blocks(Some(&trace(11)), i64::MIN, i64::MAX, PRUNE_MIDDLE, None),
            vec![1, 3],
            "fixture geometry: the duration bound prunes block 2 alone"
        );
        let probe = seg.object_size - blocks_end;
        assert!(
            blocks[3].1 <= seg.object_size - probe,
            "fixture geometry: trace 11's blocks lie outside the probe"
        );
        (memory, seg, blocks, probe)
    }

    /// The `duration_ns` bound that prunes the middle block of
    /// [`pruned_middle_fixture`]'s trace-11 run.
    const PRUNE_MIDDLE: Option<(i64, i64)> = Some((0, 100));

    /// A caller's set over three adjacent blocks whose middle block the query
    /// prunes is read as two ranged GETs of exactly the outer blocks, so the
    /// pruned block's bytes are never transferred. The probe is charged to the
    /// plan phase and the two block GETs to the scan phase.
    ///
    /// FLIP: reading the caller's set unintersected (the `Some(set)` arm's
    /// filter replaced by `.filter(|_b| true)`) yields one run over blocks
    /// 1..=3 and one GET spanning all three, which fails the range list.
    #[tokio::test]
    async fn a_block_set_with_a_pruned_middle_block_reads_two_runs() {
        let (memory, seg, blocks, probe) = pruned_middle_fixture().await;
        let log = Arc::new(GetLog::new(memory));
        let fetcher = SpanSegmentFetcher::new(log.clone()).with_trace_probe_bytes(probe);
        let query = SpanQuery::trace(trace(11), i64::MIN, i64::MAX);
        let acct = PhaseAccounting::new();
        let out = fetcher
            .fetch_accounted_in_blocks(
                &seg,
                TENANT,
                &query,
                PRUNE_MIDDLE,
                None,
                &[],
                Some(&[1, 2, 3]),
                &acct,
            )
            .await
            .expect("fetch")
            .expect("relevant");

        let block = |b: usize| GetRange::Range(blocks[b].0, blocks[b].1);
        assert_eq!(
            log.take(&seg.data_object_key),
            vec![
                GetRange::Range(seg.object_size - probe, seg.object_size),
                block(1),
                block(3),
            ],
            "the probe, then one GET of exactly block 1 and one of exactly block 3"
        );
        assert_eq!(log.total(), 0, "no other GET was issued");
        let snap = acct.snapshot();
        assert_eq!(snap.probe.s3_requests(AccountedOp::Get), 1, "the probe");
        assert_eq!(snap.probe.s3_bytes(AccountedOp::Get), probe);
        assert_eq!(snap.scan.s3_requests(AccountedOp::Get), 2, "two block GETs");
        assert_eq!(
            snap.scan.s3_bytes(AccountedOp::Get),
            (blocks[1].1 - blocks[1].0) + (blocks[3].1 - blocks[3].0),
            "block 2's bytes were never transferred"
        );
        assert_eq!(snap.pooled().s3_requests(AccountedOp::Get), 3);
        assert_eq!(out.records.len(), 4, "the four short trace-11 spans");
        assert!(out.records.iter().all(|r| r.record.trace_id == trace(11)));
        assert_eq!(out.stats.blocks_after_skip, 2);
        assert_eq!(out.stats.blocks_scanned, 2);
    }

    /// Every GET the span fetcher issues is timed under the phase its
    /// accounting charges (issue #2976): the ranged trace read's probe under
    /// probe and its two block GETs under scan, then a time-range query's
    /// whole-object read under scan. None is unattributed.
    ///
    /// FLIP: dropping the `in_get_phase` on the block-run `ranged_read` leaves
    /// its two GETs outside every phase scope, so they read as unattributed
    /// and the scan count is 1, not 3.
    #[tokio::test]
    async fn span_gets_are_timed_under_their_accounted_phases() {
        use ravel_object_store::InstrumentedStore;
        use ravel_object_store::get_latency::GetPhase;

        let (memory, seg, _blocks, probe) = pruned_middle_fixture().await;
        let store = Arc::new(InstrumentedStore::new(memory));
        let fetcher = SpanSegmentFetcher::new(store.clone()).with_trace_probe_bytes(probe);
        let acct = PhaseAccounting::new();
        fetcher
            .fetch_accounted_in_blocks(
                &seg,
                TENANT,
                &SpanQuery::trace(trace(11), i64::MIN, i64::MAX),
                PRUNE_MIDDLE,
                None,
                &[],
                Some(&[1, 2, 3]),
                &acct,
            )
            .await
            .expect("ranged fetch")
            .expect("relevant");
        fetcher
            .fetch_accounted_in_blocks(
                &seg,
                TENANT,
                &SpanQuery::ts_range(i64::MIN, i64::MAX),
                None,
                None,
                &[],
                None,
                &acct,
            )
            .await
            .expect("whole-object fetch")
            .expect("relevant");

        let snap = acct.snapshot();
        assert_eq!(snap.probe.s3_requests(AccountedOp::Get), 1);
        assert_eq!(snap.scan.s3_requests(AccountedOp::Get), 3);
        let latency = store.metrics().get_latency().snapshot();
        let counts = GetPhase::ALL.map(|phase| latency.phase(phase).ok_count());
        assert_eq!(
            counts,
            [0, 0, 1, 3, 0],
            "resolve, plan, probe, scan, unattributed"
        );
    }

    /// With no block set, the trace's whole run is one ranged GET, pruned
    /// middle block included, and only the two candidate blocks of that run
    /// are decoded.
    ///
    /// FLIP: replacing the per-run re-filter in `ranged_trace_scan` with
    /// `.filter(|_b| true)` decodes all three blocks of the run, so
    /// `blocks_scanned` reads 3.
    #[tokio::test]
    async fn a_trace_run_with_a_pruned_middle_block_decodes_two_blocks() {
        let (memory, seg, blocks, probe) = pruned_middle_fixture().await;
        let log = Arc::new(GetLog::new(memory));
        let fetcher = SpanSegmentFetcher::new(log.clone()).with_trace_probe_bytes(probe);
        let query = SpanQuery::trace(trace(11), i64::MIN, i64::MAX);
        let out = fetcher
            .fetch_accounted_in_blocks(
                &seg,
                TENANT,
                &query,
                PRUNE_MIDDLE,
                None,
                &[],
                None,
                &PhaseAccounting::new(),
            )
            .await
            .expect("fetch")
            .expect("relevant");

        assert_eq!(
            log.take(&seg.data_object_key),
            vec![
                GetRange::Range(seg.object_size - probe, seg.object_size),
                GetRange::Range(blocks[1].0, blocks[3].1),
            ],
            "the probe, then one GET of the whole run of blocks 1..=3"
        );
        assert_eq!(log.total(), 0, "no other GET was issued");
        assert_eq!(out.stats.blocks_scanned, 2, "block 2 is never decoded");
        assert_eq!(out.stats.blocks_after_skip, 2);
    }

    /// A ranged GET that returns one byte short is `Corrupt` and never
    /// admitted to the read cache: the next identical lookup goes back to the
    /// store for that extent and returns the trace's rows.
    ///
    /// FLIP: deleting the length check inside `read_extent`'s `get_or_fetch`
    /// closure admits the short bytes; the first lookup is still `Corrupt`
    /// (`ranged_read`'s own check), but the second is a cache hit on the short
    /// entry, issues no GET for the run, and is `Corrupt` again.
    #[tokio::test]
    async fn a_short_ranged_read_is_corrupt_and_never_admitted() {
        use ravel_cache::{Cache, CacheLimits};

        let mut fx = trace_fixture().await;
        let log = Arc::new(GetLog::new(fx.memory.take().expect("memory")));
        let cache: Arc<Cache<crate::fetcher::CacheFetchError>> =
            Arc::new(Cache::new(CacheLimits::new(1 << 20, 100, 1 << 20)));
        let fetcher = SpanSegmentFetcher::new(log.clone())
            .with_trace_probe_bytes(fx.probe)
            .with_cache(cache);
        let query = SpanQuery::trace(trace(3), i64::MIN, i64::MAX);
        let key = fx.a.data_object_key.clone();
        let lookup = || {
            let fetcher = fetcher.clone();
            let seg = fx.a.clone();
            async move {
                let acct = QueryAccounting::new();
                let out = fetcher
                    .fetch_accounted(&seg, TENANT, &query, None, None, &[], &acct)
                    .await;
                (out, acct.snapshot())
            }
        };

        log.truncate_next(fx.run());
        let (first, _) = lookup().await;
        let err = first.expect_err("a short run read");
        assert!(matches!(err, SpanFetchError::Corrupt { .. }), "{err:?}");
        assert_eq!(log.take(&key), vec![fx.probe_of(&fx.a), fx.run()]);

        let (second, snap) = lookup().await;
        let out = second
            .expect("the run is read again from the store")
            .expect("relevant");
        assert_eq!(
            log.take(&key),
            vec![fx.run()],
            "the probe is a hit and the run goes back to the store"
        );
        assert_eq!(snap.cache_hits, 1, "the probe");
        assert_eq!(snap.cache_misses, 1, "the run");
        assert_eq!(out.records.len(), 4, "every trace-3 span");
        assert!(out.records.iter().all(|r| r.record.trace_id == trace(3)));
    }

    /// A repeated trace lookup through the read cache issues no GET: the probe
    /// and the run are cache entries keyed by their absolute extents.
    #[tokio::test]
    async fn a_warm_trace_lookup_is_served_from_the_cache() {
        use ravel_cache::{Cache, CacheLimits};

        let mut fx = trace_fixture().await;
        let log = Arc::new(GetLog::new(fx.memory.take().expect("memory")));
        let cache: Arc<Cache<crate::fetcher::CacheFetchError>> =
            Arc::new(Cache::new(CacheLimits::new(1 << 20, 100, 1 << 20)));
        let fetcher = SpanSegmentFetcher::new(log.clone())
            .with_trace_probe_bytes(fx.probe)
            .with_cache(cache);
        let query = SpanQuery::trace(trace(3), i64::MIN, i64::MAX);

        let lookup = |acct: QueryAccounting| {
            let fetcher = fetcher.clone();
            let (a, b) = (fx.a.clone(), fx.b.clone());
            async move {
                let mut rows = 0;
                for seg in [&a, &b] {
                    let out = fetcher
                        .fetch_accounted(seg, TENANT, &query, None, None, &[], &acct)
                        .await
                        .expect("fetch")
                        .expect("relevant");
                    rows += out.records.len();
                }
                (rows, acct.snapshot())
            }
        };

        let (cold_rows, cold) = lookup(QueryAccounting::new()).await;
        assert_eq!(cold_rows, 4);
        assert_eq!(cold.total_s3_requests(), 3);
        assert_eq!(cold.cache_misses, 3);
        assert_eq!(log.total(), 3);
        log.take(&fx.a.data_object_key);
        log.take(&fx.b.data_object_key);

        let (warm_rows, warm) = lookup(QueryAccounting::new()).await;
        assert_eq!(warm_rows, 4);
        assert_eq!(log.total(), 0, "the warm lookup issues no GET");
        assert_eq!(warm.total_s3_requests(), 0);
        assert_eq!(warm.cache_hits, 3, "probe A, run A, probe B");
        assert_eq!(warm.cache_misses, 0);
        let run_len = fx.blocks_a[3].1 - fx.blocks_a[2].0;
        assert_eq!(warm.cache_bytes, 2 * fx.probe + run_len);
    }

    /// A span read admits its extents under the caller's access intent: a
    /// bulk lookup puts every extent in the bulk region, an interactive one
    /// puts none there, and a warm lookup counts its hits under its own
    /// intent.
    ///
    /// FLIP: passing `AccessIntent::Interactive` to `read_extent`'s
    /// `get_or_fetch` leaves the bulk lookup's bookkeeping at 0 and counts
    /// its warm hits as interactive.
    #[tokio::test]
    async fn a_trace_lookup_admits_and_reads_under_the_callers_access_intent() {
        use ravel_cache::{Cache, CacheKey, CacheLimits};
        use ravel_types::accounting::AccessIntent;

        let limits = CacheLimits::new(1 << 20, 100, 1 << 20);
        let bookkeeping_for = |entries: usize| {
            let scratch: Cache<crate::fetcher::CacheFetchError> = Cache::new(limits);
            for i in 0..entries {
                scratch.insert_with_intent(
                    CacheKey::new(TENANT.0, [1u8; 32], i as u64, 1),
                    bytes::Bytes::from_static(b"x"),
                    ravel_cache::AccessIntent::BulkScan,
                );
            }
            assert_eq!(scratch.len(), entries);
            scratch.bulk_bookkeeping_bytes()
        };

        for intent in [AccessIntent::BulkScan, AccessIntent::Interactive] {
            let mut fx = trace_fixture().await;
            let log = Arc::new(GetLog::new(fx.memory.take().expect("memory")));
            let cache: Arc<Cache<crate::fetcher::CacheFetchError>> = Arc::new(Cache::new(limits));
            let fetcher = SpanSegmentFetcher::new(log.clone())
                .with_trace_probe_bytes(fx.probe)
                .with_cache(Arc::clone(&cache));
            let query = SpanQuery::trace(trace(3), i64::MIN, i64::MAX);
            let lookup = || {
                let acct = QueryAccounting::new();
                acct.set_access_intent(intent);
                let fetcher = fetcher.clone();
                let (a, b) = (fx.a.clone(), fx.b.clone());
                async move {
                    for seg in [&a, &b] {
                        fetcher
                            .fetch_accounted(seg, TENANT, &query, None, None, &[], &acct)
                            .await
                            .expect("fetch")
                            .expect("relevant");
                    }
                    acct.snapshot()
                }
            };

            let cold = lookup().await;
            assert_eq!(cold.cache_misses, 3, "{intent:?}");
            assert_eq!(cache.len(), 3, "{intent:?}: probe A, run A, probe B");
            let expected = match intent {
                AccessIntent::BulkScan => bookkeeping_for(3),
                _ => 0,
            };
            assert_eq!(
                cache.bulk_bookkeeping_bytes(),
                expected,
                "{intent:?}: an extent was admitted on the wrong side of the bulk region"
            );

            let before = cache.metrics().intent_snapshot();
            let warm = lookup().await;
            assert_eq!(warm.cache_hits, 3, "{intent:?}");
            let after = cache.metrics().intent_snapshot();
            let hits = (
                after.bulk_scan.hits - before.bulk_scan.hits,
                after.interactive.hits - before.interactive.hits,
            );
            let expected = match intent {
                AccessIntent::BulkScan => (3, 0),
                _ => (0, 3),
            };
            assert_eq!(hits, expected, "{intent:?}: {after:?}");
        }
    }

    /// A ranged GET that fails surfaces as a typed store error, and the
    /// fetcher does not retry the read as a whole-object GET.
    #[tokio::test]
    async fn a_failed_ranged_trace_get_is_a_store_error_with_no_whole_object_fallback() {
        use ravel_object_store::fault::ScriptedFault;
        use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule};

        let mut fx = trace_fixture().await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Transient("injected".into()))
                .with_key_contains(fx.a.data_object_key.clone())
                .with_occurrence(Occurrence::Nth(2)),
        );
        let fault = Arc::new(FaultStore::new(
            GetLog::new(fx.memory.take().expect("memory")),
            plan,
        ));
        let fetcher = SpanSegmentFetcher::new(fault.clone()).with_trace_probe_bytes(fx.probe);
        let query = SpanQuery::trace(trace(3), i64::MIN, i64::MAX);

        let err = fetcher
            .fetch_accounted(
                &fx.a,
                TENANT,
                &query,
                None,
                None,
                &[],
                &QueryAccounting::new(),
            )
            .await
            .expect_err("the run GET fails");
        assert!(
            matches!(
                err,
                SpanFetchError::Store {
                    source: StoreError::Transient(_),
                    ..
                }
            ),
            "{err:?}"
        );
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the injected fault fired on the run GET"
        );
        let reached = fault.inner().take(&fx.a.data_object_key);
        assert!(!reached.contains(&GetRange::Full), "{reached:?}");
        assert_eq!(
            reached,
            vec![fx.probe_of(&fx.a)],
            "only the probe reached the store: no retry, no whole-object read"
        );
    }

    /// A trace id whose sixteen bytes all differ, ordered by `n`: byte `i` is
    /// `n ^ (i * 0x11)`. A probe that reads the id's bytes in another order
    /// keys a different `key8`, which a `[n; 16]` id would hide.
    fn gap_trace(n: u8) -> [u8; 16] {
        std::array::from_fn(|i| n ^ (i as u8).wrapping_mul(0x11))
    }

    /// The `(key8, block)` entries of one KEY_IDX bucket frame, decoded from
    /// the frame layout (u32 LE entry count, u32 crc32c, then the zstd bytes of
    /// count 12-byte entries, an 8-byte key and a u32 LE block) without the
    /// probe code under test.
    fn bucket_entries(frame: &[u8]) -> Vec<([u8; 8], u32)> {
        let count = u32::from_le_bytes(frame[..4].try_into().expect("count")) as usize;
        let raw = zstd::bulk::decompress(&frame[8..], count * 12).expect("bucket payload");
        assert_eq!(raw.len(), count * 12);
        raw.chunks_exact(12)
            .map(|e| {
                (
                    e[..8].try_into().expect("key8"),
                    u32::from_le_bytes(e[8..].try_into().expect("block")),
                )
            })
            .collect()
    }

    /// One object of single-span traces 1, 3, 5, 7, 9 and 11 over two-record
    /// blocks, `[t1 t3] [t5 t7] [t9 t11]`, so trace 2 lies inside block 0's
    /// SKIP_IDX trace range and is absent from the object. The fetcher's probe
    /// is the object's tail past KEY_IDX, so every KEY_IDX read is a GET.
    struct GapFixture {
        memory: Option<MemoryStore>,
        seg: SegmentRef,
        bytes: Bytes,
        probe: u64,
        blocks: Vec<(u64, u64)>,
    }

    impl GapFixture {
        fn probe_get(&self) -> GetRange {
            GetRange::Range(self.seg.object_size - self.probe, self.seg.object_size)
        }
        fn block(&self, b: usize) -> GetRange {
            GetRange::Range(self.blocks[b].0, self.blocks[b].1)
        }
        /// KEY_IDX's `[0, prefix_len)` as an absolute range, from the footer
        /// entry alone.
        fn prefix_get(&self) -> GetRange {
            let footer = open(&self.bytes).expect("footer");
            let desc = footer.section(kind::KEY_IDX).expect("KEY_IDX");
            GetRange::Range(desc.offset, desc.offset + u64::from(desc.prefix_len))
        }
        fn read(&self, range: &GetRange) -> &[u8] {
            let GetRange::Range(start, end) = *range else {
                panic!("expected a bounded range, got {range:?}");
            };
            &self.bytes[start as usize..end as usize]
        }
        /// The answer for `trace_id` and the KEY_IDX ranges a probe reads,
        /// taken from the ranged reader over the whole object, independently of
        /// the fetcher under test.
        fn key_reads(&self, trace_id: &[u8; 16]) -> (TraceKeyProbe, Vec<GetRange>) {
            let footer = open(&self.bytes).expect("footer");
            let skip_desc = footer.section(kind::SKIP_IDX).expect("SKIP_IDX");
            let skip_raw = ravel_rspan::read_section(
                &self.bytes,
                skip_desc,
                ravel_rspan::footer::DEFAULT_MAX_SECTION_UNCOMP,
            )
            .expect("SKIP_IDX bytes");
            let reader = RspanRangeReader::from_sections(&footer, &skip_raw).expect("reader");
            let mut reads = Vec::new();
            let answer = reader
                .probe_trace(trace_id, |r: std::ops::Range<u64>| {
                    reads.push(GetRange::Range(r.start, r.end));
                    Ok::<_, SpanSegError>(self.bytes.slice(r.start as usize..r.end as usize))
                })
                .expect("probe");
            (answer, reads)
        }
    }

    fn range_len(range: &GetRange) -> u64 {
        match range {
            GetRange::Range(start, end) => end - start,
            other => panic!("expected a bounded range, got {other:?}"),
        }
    }

    async fn gap_fixture() -> GapFixture {
        let memory = MemoryStore::new();
        let records: Vec<SpanRecord> = [1u8, 3, 5, 7, 9, 11]
            .into_iter()
            .map(|t| bare_span(gap_trace(t), span(t), 100, 150))
            .collect();
        let seg = write_object(&memory, 7, &records).await;
        let bytes = memory
            .get(&seg.data_object_key, GetRange::Full)
            .await
            .expect("whole object")
            .data;
        let footer = open(&bytes).expect("footer");
        let key_idx = *footer.section(kind::KEY_IDX).expect("KEY_IDX section");
        let probe = seg.object_size - (key_idx.offset + key_idx.len);
        let (_, blocks, _) = layout(&bytes);
        assert_eq!(blocks.len(), 3, "fixture geometry: three blocks");
        let reader = RspanReader::new(&bytes, &RspanConfig::default()).expect("reader");
        assert_eq!(
            reader.skip_index().candidate_blocks(
                Some(&gap_trace(2)),
                i64::MIN,
                i64::MAX,
                None,
                None
            ),
            vec![0],
            "fixture geometry: SKIP_IDX keeps block 0 for trace 2"
        );
        GapFixture {
            memory: Some(memory),
            seg,
            bytes,
            probe,
            blocks,
        }
    }

    /// ADR-2707 decision 5: a trace SKIP_IDX keeps a block for but KEY_IDX
    /// says is absent reads the tail probe, the KEY_IDX reads and no block,
    /// each KEY_IDX read on the key-index phase, and the object counts as
    /// pruned by the key index.
    ///
    /// FLIP: `blocks.is_none() && !candidate_idx.is_empty()` to `false` in
    /// `ranged_trace_scan` reads block 0 and counts nothing.
    #[tokio::test]
    async fn an_absent_trace_reads_the_key_index_and_no_block() {
        let mut fx = gap_fixture().await;
        let (answer, key_reads) = fx.key_reads(&gap_trace(2));
        assert_eq!(answer, TraceKeyProbe::Absent);
        assert_eq!(key_reads[0], fx.prefix_get());
        let key8: [u8; 8] = gap_trace(2)[..8].try_into().expect("key8");
        if let Some(bucket) = key_reads.get(1) {
            assert!(
                bucket_entries(fx.read(bucket))
                    .iter()
                    .all(|(k, _)| *k != key8),
                "an absent trace's bucket holds no entry for its key8"
            );
        }
        let log = Arc::new(GetLog::new(fx.memory.take().expect("memory")));
        let fetcher = SpanSegmentFetcher::new(log.clone()).with_trace_probe_bytes(fx.probe);
        let acct = PhaseAccounting::new();
        let out = fetcher
            .fetch_accounted_in_blocks(
                &fx.seg,
                TENANT,
                &SpanQuery::trace(gap_trace(2), i64::MIN, i64::MAX),
                None,
                None,
                &[],
                None,
                &acct,
            )
            .await
            .expect("fetch")
            .expect("relevant");

        let mut want = vec![fx.probe_get()];
        want.extend(key_reads.iter().copied());
        assert_eq!(
            log.take(&fx.seg.data_object_key),
            want,
            "the tail probe, then KEY_IDX, and no block"
        );
        assert_eq!(log.total(), 0);
        assert!(out.records.is_empty());
        assert_eq!(
            out.key_index,
            KeyIndexPruning {
                segments_pruned: 1,
                blocks_pruned: 0
            }
        );
        assert_eq!(out.stats.blocks_after_skip, 0);
        assert_eq!(out.stats.blocks_scanned, 0);

        let snap = acct.snapshot();
        assert_eq!(
            snap.key_index.s3_requests(AccountedOp::Get),
            key_reads.len() as u64
        );
        assert_eq!(
            snap.key_index.s3_bytes(AccountedOp::Get),
            key_reads.iter().map(range_len).sum::<u64>()
        );
        assert_eq!(snap.probe.s3_requests(AccountedOp::Get), 1);
        assert_eq!(snap.probe.s3_bytes(AccountedOp::Get), fx.probe);
        assert_eq!(snap.scan.total_s3_requests(), 0, "no block GET");
    }

    /// A present trace reads the KEY_IDX prefix and its bucket, then only the
    /// blocks the index names that SKIP_IDX also kept.
    ///
    /// FLIP: `blocks.is_none() && !candidate_idx.is_empty()` to `false` drops
    /// the two KEY_IDX reads and fails the GET list.
    #[tokio::test]
    async fn a_present_trace_reads_the_key_index_then_only_its_blocks() {
        let mut fx = gap_fixture().await;
        let (answer, key_reads) = fx.key_reads(&gap_trace(3));
        assert_eq!(answer, TraceKeyProbe::Present(vec![0]));
        assert_eq!(key_reads.len(), 2, "a present trace reads its bucket");
        assert_eq!(key_reads[0], fx.prefix_get());
        let key8: [u8; 8] = gap_trace(3)[..8].try_into().expect("key8");
        assert!(
            bucket_entries(fx.read(&key_reads[1])).contains(&(key8, 0)),
            "the bucket read holds trace 3's first eight bytes in block 0"
        );
        let log = Arc::new(GetLog::new(fx.memory.take().expect("memory")));
        let fetcher = SpanSegmentFetcher::new(log.clone()).with_trace_probe_bytes(fx.probe);
        let acct = PhaseAccounting::new();
        let out = fetcher
            .fetch_accounted_in_blocks(
                &fx.seg,
                TENANT,
                &SpanQuery::trace(gap_trace(3), i64::MIN, i64::MAX),
                None,
                None,
                &[],
                None,
                &acct,
            )
            .await
            .expect("fetch")
            .expect("relevant");

        let mut want = vec![fx.probe_get()];
        want.extend(key_reads.iter().copied());
        want.push(fx.block(0));
        assert_eq!(log.take(&fx.seg.data_object_key), want);
        assert_eq!(log.total(), 0);
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].record.trace_id, gap_trace(3));
        assert_eq!(out.key_index, KeyIndexPruning::default());
        let snap = acct.snapshot();
        assert_eq!(snap.key_index.s3_requests(AccountedOp::Get), 2);
        assert_eq!(
            snap.scan.s3_bytes(AccountedOp::Get),
            range_len(&fx.block(0))
        );
    }

    /// The count of candidates a KEY_IDX block set leaves out: zero when the
    /// set covers them, every uncovered one otherwise.
    #[test]
    fn candidates_outside_counts_the_blocks_the_index_removes() {
        assert_eq!(candidates_outside(&[1, 2, 3], &[1, 2, 3]), 0);
        assert_eq!(candidates_outside(&[1, 2, 3], &[0, 2]), 2);
        assert_eq!(candidates_outside(&[], &[0]), 0);
        assert_eq!(candidates_outside(&[4], &[]), 1);
    }

    /// A KEY_IDX bucket whose frame checksum no longer matches falls back to
    /// SKIP_IDX: the rows equal the clean object's and nothing is counted as
    /// pruned.
    ///
    /// FLIP: the `bucket.finish` error arm returning
    /// `Ok(Some(TraceKeyProbe::Absent))` instead of `Ok(None)` loses the row.
    #[tokio::test]
    async fn a_corrupt_key_index_bucket_falls_back_with_the_same_rows() {
        let mut fx = gap_fixture().await;
        let (_, key_reads) = fx.key_reads(&gap_trace(3));
        let memory = fx.memory.take().expect("memory");
        let query = SpanQuery::trace(gap_trace(3), i64::MIN, i64::MAX);
        let fetch = |store: Arc<dyn ObjectStoreBackend>| {
            let seg = fx.seg.clone();
            let probe = fx.probe;
            async move {
                SpanSegmentFetcher::new(store)
                    .with_trace_probe_bytes(probe)
                    .fetch_accounted(
                        &seg,
                        TENANT,
                        &query,
                        None,
                        None,
                        &[],
                        &QueryAccounting::new(),
                    )
                    .await
                    .expect("fetch")
                    .expect("relevant")
            }
        };
        let log = Arc::new(GetLog::new(memory));
        let clean = fetch(log.clone()).await;
        log.take(&fx.seg.data_object_key);

        let GetRange::Range(bucket_start, _) = key_reads[1] else {
            panic!("bucket read is a range");
        };
        let mut damaged = fx.bytes.to_vec();
        // The frame's crc32c field follows its u32 entry count.
        damaged[bucket_start as usize + 4] ^= 0xFF;
        log.put(
            &fx.seg.data_object_key,
            Bytes::from(damaged),
            PutOptions::default(),
        )
        .await
        .expect("overwrite");
        let fallen = fetch(log.clone()).await;

        let mut want = vec![fx.probe_get()];
        want.extend(key_reads.iter().copied());
        want.push(fx.block(0));
        assert_eq!(
            log.take(&fx.seg.data_object_key),
            want,
            "the bucket was read, refused, and SKIP_IDX's run read"
        );
        assert_eq!(fallen.records.len(), 1);
        assert_eq!(fallen.records, clean.records, "identical rows");
        assert_eq!(fallen.stats, clean.stats);
        assert_eq!(fallen.key_index, KeyIndexPruning::default(), "no pruning");
    }

    /// A store failure on a KEY_IDX GET is a typed store error, not a
    /// fallback to SKIP_IDX.
    ///
    /// FLIP: `key_index_read(..).await?` on the prefix replaced by a match
    /// that falls back on `Err` returns the row instead of the error.
    #[tokio::test]
    async fn a_failed_key_index_get_is_a_store_error() {
        use ravel_object_store::fault::ScriptedFault;
        use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule};

        let mut fx = gap_fixture().await;
        let (_, key_reads) = fx.key_reads(&gap_trace(3));
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Transient("injected".into()))
                .with_key_contains(fx.seg.data_object_key.clone())
                .with_occurrence(Occurrence::Nth(2)),
        );
        let fault = Arc::new(FaultStore::new(
            GetLog::new(fx.memory.take().expect("memory")),
            plan,
        ));
        let fetcher = SpanSegmentFetcher::new(fault.clone()).with_trace_probe_bytes(fx.probe);
        let err = fetcher
            .fetch_accounted(
                &fx.seg,
                TENANT,
                &SpanQuery::trace(gap_trace(3), i64::MIN, i64::MAX),
                None,
                None,
                &[],
                &QueryAccounting::new(),
            )
            .await
            .expect_err("the KEY_IDX prefix GET fails");
        assert!(
            matches!(
                err,
                SpanFetchError::Store {
                    source: StoreError::Transient(_),
                    ..
                }
            ),
            "{err:?}"
        );
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the injected fault fired on the KEY_IDX prefix GET"
        );
        assert_ne!(key_reads[0], fx.probe_get());
        assert_eq!(
            fault.inner().take(&fx.seg.data_object_key),
            vec![fx.probe_get()],
            "only the tail probe reached the store"
        );
    }

    /// A KEY_IDX prefix that fails its own checks, in the header or in the
    /// directory, falls back to SKIP_IDX: no bucket read, the clean object's
    /// rows, and nothing counted as pruned.
    ///
    /// FLIP: the `locate_trace_key` error arm in `probe_key_index` returning
    /// `Ok(Some(TraceKeyProbe::Absent))` instead of `Ok(None)` loses the row.
    #[tokio::test]
    async fn a_corrupt_key_index_prefix_falls_back_with_the_same_rows() {
        let mut fx = gap_fixture().await;
        let (_, key_reads) = fx.key_reads(&gap_trace(3));
        let GetRange::Range(prefix_start, prefix_end) = fx.prefix_get() else {
            panic!("prefix read is a range");
        };
        let memory = fx.memory.take().expect("memory");
        let query = SpanQuery::trace(gap_trace(3), i64::MIN, i64::MAX);
        let log = Arc::new(GetLog::new(memory));
        let fetch = |log: Arc<GetLog>| {
            let seg = fx.seg.clone();
            let probe = fx.probe;
            async move {
                SpanSegmentFetcher::new(log)
                    .with_trace_probe_bytes(probe)
                    .fetch_accounted(
                        &seg,
                        TENANT,
                        &query,
                        None,
                        None,
                        &[],
                        &QueryAccounting::new(),
                    )
                    .await
                    .expect("fetch")
                    .expect("relevant")
            }
        };
        let clean = fetch(log.clone()).await;
        assert_eq!(clean.records.len(), 1);
        assert_eq!(
            log.take(&fx.seg.data_object_key),
            vec![fx.probe_get(), key_reads[0], key_reads[1], fx.block(0)]
        );

        // The header's first byte, and the directory's last byte (the end of
        // the prefix).
        for (at, what) in [(prefix_start, "header"), (prefix_end - 1, "directory")] {
            let mut damaged = fx.bytes.to_vec();
            damaged[at as usize] ^= 0xFF;
            log.put(
                &fx.seg.data_object_key,
                Bytes::from(damaged),
                PutOptions::default(),
            )
            .await
            .expect("overwrite");
            let fallen = fetch(log.clone()).await;
            assert_eq!(
                log.take(&fx.seg.data_object_key),
                vec![fx.probe_get(), fx.prefix_get(), fx.block(0)],
                "{what}: the prefix was read, refused, and SKIP_IDX's run read"
            );
            assert_eq!(fallen.records, clean.records, "{what}: identical rows");
            assert_eq!(fallen.stats, clean.stats, "{what}");
            assert_eq!(
                fallen.key_index,
                KeyIndexPruning::default(),
                "{what}: no pruning"
            );
        }
    }

    /// A store failure on the KEY_IDX bucket GET is a typed store error, not a
    /// fallback to SKIP_IDX.
    ///
    /// FLIP: the bucket's `key_index_read(..).await?` replaced by a match that
    /// falls back on `Err` returns the row instead of the error.
    #[tokio::test]
    async fn a_failed_key_index_bucket_get_is_a_store_error() {
        use ravel_object_store::fault::ScriptedFault;
        use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule};

        let mut fx = gap_fixture().await;
        let (_, key_reads) = fx.key_reads(&gap_trace(3));
        assert_eq!(key_reads.len(), 2, "the probe reads a bucket");
        // GETs in order: the tail probe, the KEY_IDX prefix, the bucket.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Transient("injected".into()))
                .with_key_contains(fx.seg.data_object_key.clone())
                .with_occurrence(Occurrence::Nth(3)),
        );
        let fault = Arc::new(FaultStore::new(
            GetLog::new(fx.memory.take().expect("memory")),
            plan,
        ));
        let fetcher = SpanSegmentFetcher::new(fault.clone()).with_trace_probe_bytes(fx.probe);
        let err = fetcher
            .fetch_accounted(
                &fx.seg,
                TENANT,
                &SpanQuery::trace(gap_trace(3), i64::MIN, i64::MAX),
                None,
                None,
                &[],
                &QueryAccounting::new(),
            )
            .await
            .expect_err("the KEY_IDX bucket GET fails");
        assert!(
            matches!(
                err,
                SpanFetchError::Store {
                    source: StoreError::Transient(_),
                    ..
                }
            ),
            "{err:?}"
        );
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the injected fault fired on the KEY_IDX bucket GET"
        );
        assert_eq!(
            fault.inner().take(&fx.seg.data_object_key),
            vec![fx.probe_get(), key_reads[0]],
            "the tail probe and the prefix reached the store, no block"
        );
    }

    /// With the read cache, a repeated lookup serves the KEY_IDX reads from
    /// the cache with no GET, still on the key-index phase.
    ///
    /// FLIP: passing `None` for `tenant_hash` to `ranged_read` in
    /// `key_index_read` bypasses the cache and the warm lookup GETs KEY_IDX.
    #[tokio::test]
    async fn a_warm_key_index_probe_is_served_from_the_cache() {
        use ravel_cache::{Cache, CacheLimits};

        let mut fx = gap_fixture().await;
        let (_, key_reads) = fx.key_reads(&gap_trace(3));
        let log = Arc::new(GetLog::new(fx.memory.take().expect("memory")));
        let cache: Arc<Cache<crate::fetcher::CacheFetchError>> =
            Arc::new(Cache::new(CacheLimits::new(1 << 20, 100, 1 << 20)));
        let fetcher = SpanSegmentFetcher::new(log.clone())
            .with_trace_probe_bytes(fx.probe)
            .with_cache(cache);
        let query = SpanQuery::trace(gap_trace(3), i64::MIN, i64::MAX);
        let lookup = || {
            let fetcher = fetcher.clone();
            let seg = fx.seg.clone();
            async move {
                let acct = PhaseAccounting::new();
                let out = fetcher
                    .fetch_accounted_in_blocks(&seg, TENANT, &query, None, None, &[], None, &acct)
                    .await
                    .expect("fetch")
                    .expect("relevant");
                (out.records.len(), acct.snapshot())
            }
        };

        let (cold_rows, cold) = lookup().await;
        assert_eq!(cold_rows, 1);
        assert_eq!(cold.key_index.cache_misses, 2);
        assert_eq!(log.total(), 4, "probe, prefix, bucket, block 0");
        log.take(&fx.seg.data_object_key);

        let (warm_rows, warm) = lookup().await;
        assert_eq!(warm_rows, 1);
        assert_eq!(log.total(), 0, "the warm lookup issues no GET");
        assert_eq!(warm.key_index.total_s3_requests(), 0);
        assert_eq!(warm.key_index.cache_hits, 2);
        assert_eq!(
            warm.key_index.cache_bytes,
            key_reads.iter().map(range_len).sum::<u64>()
        );
        assert_eq!(warm.pooled().cache_hits, 4, "the probe and the block too");
    }

    /// Writes one RSPAN object, overwrites its trailer version with an
    /// out-of-window value, and returns its `SegmentRef`. With `rehash` the ref
    /// carries the blake3 of the rewritten bytes (an object a writer of that
    /// version produced); without it, the blake3 of the original bytes, so the
    /// version bytes are the only damage.
    async fn write_stamped_object(store: &MemoryStore, rehash: bool) -> (SegmentRef, u16) {
        let version = ravel_rspan::footer::VERSION + 1;
        assert!(!ravel_rspan::footer::SUPPORTED_VERSIONS.contains(version));
        let mut seg = write_object(store, 41, &[bare_span(trace(1), span(1), 10, 20)]).await;
        let original = store
            .get(&seg.data_object_key, GetRange::Full)
            .await
            .expect("object present")
            .data;
        let mut bytes = original.to_vec();
        let at = bytes.len() - ravel_rspan::footer::TRAILER_LEN + 8;
        bytes[at..at + 2].copy_from_slice(&version.to_le_bytes());
        seg.content_hash = if rehash {
            *blake3::hash(&bytes).as_bytes()
        } else {
            *blake3::hash(&original).as_bytes()
        };
        store
            .put(
                &seg.data_object_key,
                Bytes::from(bytes),
                PutOptions::default(),
            )
            .await
            .expect("overwrite");
        (seg, version)
    }

    /// Every RSPAN read funnel's result for `seg`, the tenant-blind one included.
    async fn every_funnel(store: Arc<MemoryStore>, seg: &SegmentRef) -> Vec<SpanFetchError> {
        let fetcher = SpanSegmentFetcher::new(store);
        let q = all_query();
        let accounting = QueryAccounting::new();
        vec![
            fetcher
                .fetch(seg, &q, None, None, &[])
                .await
                .expect_err("fetch refuses"),
            fetcher
                .fetch_accounted(seg, TENANT, &q, None, None, &[], &accounting)
                .await
                .expect_err("fetch_accounted refuses"),
            fetcher
                .fetch_accounted_columnar(
                    seg,
                    TENANT,
                    &q,
                    None,
                    None,
                    &[],
                    &FIXED_PROJECTION,
                    &accounting,
                )
                .await
                .err()
                .expect("fetch_accounted_columnar refuses"),
        ]
    }

    /// ADR-2708 D5a: an out-of-window RSPAN object whose bytes match their
    /// recorded content hash is `UnsupportedVersion` on every funnel.
    #[tokio::test]
    async fn an_out_of_window_object_is_unsupported_version() {
        let store = Arc::new(MemoryStore::new());
        let (seg, version) = write_stamped_object(&store, true).await;
        for err in every_funnel(store, &seg).await {
            match err {
                SpanFetchError::UnsupportedVersion { key, version: got } => {
                    assert_eq!(key, seg.data_object_key);
                    assert_eq!(got, u32::from(version));
                }
                other => panic!("expected UnsupportedVersion, got {other:?}"),
            }
        }
    }

    /// The same trailer version over bytes that no longer match the recorded
    /// content hash is corruption, not a version to migrate.
    #[tokio::test]
    async fn a_corrupt_version_byte_is_reported_as_corruption() {
        let store = Arc::new(MemoryStore::new());
        let (seg, _) = write_stamped_object(&store, false).await;
        for err in every_funnel(store, &seg).await {
            match err {
                SpanFetchError::Corrupt {
                    source: SpanSegError::Corrupted(message),
                    ..
                } => assert!(message.contains("content hash"), "{message}"),
                other => panic!("expected Corrupt, got {other:?}"),
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod late_serve_accounting_tests {
    //! Two whole-object span reads of one object through a cache at once, the
    //! second joining the first's in-flight GET: the follower is charged no
    //! GET and records a query cache miss, on both cache kinds. A `FaultStore`
    //! hold gate parks the leader's GET until the follower is in position.

    use super::*;
    use crate::fetcher::CacheFetchError;
    use ravel_cache::{Cache, CacheLimits, DiskCache, TieredCache};
    use ravel_catalog::SegmentLevel;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([5u8; 16]);
    const CONTENT_HASH: [u8; 32] = [6u8; 32];
    const KEY: &str = "t/late-serve.rspan";
    const SIZE: u64 = 40;

    fn limits() -> CacheLimits {
        CacheLimits::new(1024 * 1024, 100, 1024 * 1024)
    }

    fn seg_ref() -> SegmentRef {
        SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: SIZE,
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
            segment_format_version: u32::from(ravel_rspan::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    /// FLIP: mapping `ReadOutcome::LateServe` to the `ReadOutcome::Hit` arm in
    /// `SpanSegmentFetcher::whole_object_bytes` makes the follower a hit with
    /// cache bytes on both kinds.
    #[tokio::test]
    async fn a_whole_object_span_follower_is_a_miss_charged_no_get() {
        let object = Bytes::from((0..SIZE as u8).collect::<Vec<u8>>());
        let key = CacheKey::new(TENANT.0, CONTENT_HASH, 0, SIZE);
        for tiered in [false, true] {
            let memory = MemoryStore::new();
            memory
                .put(KEY, object.clone(), PutOptions::default())
                .await
                .expect("put");
            let fault = Arc::new(FaultStore::new(memory, FaultPlan::default()));
            let gate = fault.hold(Op::Get, None, Occurrence::Always);
            let tmp = tempfile::TempDir::new().expect("tempdir");
            let (cache, parked): (ReadCache, Box<dyn Fn() -> bool>) = if tiered {
                let tiered = Arc::new(TieredCache::new(
                    Cache::new(limits()),
                    DiskCache::new(tmp.path().to_path_buf(), limits()),
                ));
                (
                    ReadCache::Tiered(tiered.clone()),
                    Box::new(move || tiered.in_flight_waiters(&key) >= 1),
                )
            } else {
                let ram: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(limits()));
                let metrics = ram.metrics();
                // The follower joins the flight in the same poll that records
                // its peek's miss.
                (
                    ReadCache::Ram(ram),
                    Box::new(move || metrics.snapshot().misses >= 2),
                )
            };
            let fetcher = SpanSegmentFetcher::new(fault).with_cache(cache);
            let seg = seg_ref();
            let leader_acc = QueryAccounting::new();
            let follower_acc = QueryAccounting::new();
            let release = async {
                tokio::time::timeout(std::time::Duration::from_secs(30), async {
                    gate.wait_until_held(1).await;
                    while !parked() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the leader's GET is held and the follower parked within 30 s");
                assert_eq!(gate.held_count(), 1, "exactly one GET reached the store");
                for id in gate.held() {
                    assert!(gate.release(id), "held id must release");
                }
            };
            let (leader, follower, ()) = tokio::join!(
                fetcher.whole_object_bytes(&seg, TENANT, &leader_acc),
                fetcher.whole_object_bytes(&seg, TENANT, &follower_acc),
                release,
            );
            assert_eq!(leader.expect("leader read").as_ref(), object.as_ref());
            assert_eq!(follower.expect("follower read").as_ref(), object.as_ref());

            let leader_snap = leader_acc.snapshot();
            assert_eq!(leader_snap.total_s3_requests(), 1, "tiered={tiered}");
            assert_eq!(leader_snap.cache_misses, 1, "tiered={tiered}");
            assert_eq!(leader_snap.cache_hits, 0, "tiered={tiered}");
            let follower_snap = follower_acc.snapshot();
            assert_eq!(follower_snap.total_s3_requests(), 0, "tiered={tiered}");
            assert_eq!(follower_snap.total_s3_bytes(), 0, "tiered={tiered}");
            assert_eq!(follower_snap.cache_misses, 1, "tiered={tiered}");
            assert_eq!(follower_snap.cache_hits, 0, "tiered={tiered}");
            assert_eq!(follower_snap.cache_bytes, 0, "tiered={tiered}");
        }
    }
}
