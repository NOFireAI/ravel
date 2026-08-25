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

use bytes::Bytes;
use ravel_catalog::SegmentRef;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_rspan::block::{DEFAULT_MAX_UNCOMP, DecodedBlock, read_block, read_block_projected};
use ravel_rspan::footer::kind;
use ravel_rspan::record::{
    COL_END_TS, COL_EVENT_ATTRS_BLOB, COL_EVENT_COUNT, COL_EVENT_NAME, COL_EVENT_TS, COL_START_TS,
    COL_TRACE_ID,
};
use ravel_rspan::skip_index::BlockEntry;
use ravel_rspan::varint::get_uvarint;
use ravel_rspan::{
    BloomPredicate, RspanConfig, RspanReader, ScanStats, SpanQuery, SpanRecord, SpanSegError, open,
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
}

/// One candidate block resolved to its absolute, bounds-checked
/// `(start, end, crc32c)` byte range within the whole object, ready for the
/// cursor to re-slice without holding a borrow of the reader.
type CandidateBlock = (usize, usize, u32);

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
}

/// Fetches and scans one RSPAN span segment at a time. Constructed with the
/// same [`ObjectStoreBackend`] trait object the log/metric fetchers take.
#[derive(Clone)]
pub struct SpanSegmentFetcher {
    store: Arc<dyn ObjectStoreBackend>,
    cfg: RspanConfig,
}

impl SpanSegmentFetcher {
    pub fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        SpanSegmentFetcher {
            store,
            cfg: RspanConfig::default(),
        }
    }

    /// Overrides the [`RspanConfig`] used for section-size caps when decoding.
    #[must_use]
    pub fn with_config(mut self, cfg: RspanConfig) -> Self {
        self.cfg = cfg;
        self
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
    /// with no GET. Otherwise the whole object is fetched once
    /// ([`GetRange::Full`]) and scanned block by block:
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

        let key = &seg_ref.data_object_key;
        let got = self
            .store
            .get(key, GetRange::Full)
            .await
            .map_err(|source| SpanFetchError::Store {
                key: key.to_string(),
                source,
            })?;

        // Unaccounted entry point: the page-byte fold lands in a throwaway
        // handle, exactly as the logs side routes `fetch` through
        // `fetch_accounted(&QueryAccounting::new())`. The row output is
        // unchanged.
        let scan = self.open_scan(
            got.data,
            key.to_string(),
            *query,
            duration_ns,
            status_mask,
            predicates,
            BlockProjection::All,
            QueryAccounting::new(),
        )?;
        Ok(Some(drain_rows(scan)?))
    }

    /// Accounted, tenant-checked counterpart of [`fetch`](Self::fetch):
    /// identical prune-and-scan behavior, plus two things (ADR-0044). The
    /// object GET is recorded against `accounting` at this funnel -- the funnel
    /// the span scan did not account before -- exactly as
    /// `LogSegmentFetcher::fetch_accounted` records the log GET; and the fetched
    /// object's footer `tenant_hash` is verified against `tenant_hash` before it
    /// is decoded, failing closed with [`SpanFetchError::TenantMismatch`].
    ///
    /// `tenant_hash` mirrors the logs scan chain, which threads a
    /// `TenantHash` from `LogsTableProvider` through `LogsScanExec` into
    /// `LogSegmentFetcher::fetch_accounted_with_tenant`. There it keys the
    /// ADR-0046 read cache; RSPAN has no read cache in this crate, so the same
    /// threaded identity instead guards against decoding an object that belongs
    /// to another tenant. The GET is recorded before the guard runs: the
    /// request was really issued regardless of whose object came back.
    ///
    /// This is the funnel a production `spans` query takes once a caller is
    /// wired in (ADR-0045 decision 5, phase 2); the unaccounted, tenant-blind
    /// [`fetch`](Self::fetch) stays for unit tests and block-stat spies.
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
        if !Self::ts_range_relevant(seg_ref, query.ts_min, query.ts_max) {
            return Ok(None);
        }

        let key = &seg_ref.data_object_key;
        let got = self
            .store
            .get(key, GetRange::Full)
            .await
            .map_err(|source| SpanFetchError::Store {
                key: key.to_string(),
                source,
            })?;
        // This funnel issues exactly one whole-object GET per call, the same
        // shape the logs funnel records (one `Get` request, the transferred
        // byte count). `estimate_spans_cost` bounds this at one GET per segment.
        accounting.record_s3_request(AccountedOp::Get);
        accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
        let bytes = got.data;

        // Tenant identity guard: an object whose footer names a different
        // tenant is never decoded. The footer trailer is parsed here (cheap);
        // `open_scan` re-opens it for the BLOCKS section descriptor.
        let footer = open(&bytes).map_err(|source| corrupt(key, source))?;
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
            BlockProjection::All,
            accounting.clone(),
        )?;
        Ok(Some(drain_rows(scan)?))
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
        if !Self::ts_range_relevant(seg_ref, query.ts_min, query.ts_max) {
            return Ok(None);
        }

        let key = &seg_ref.data_object_key;
        let got = self
            .store
            .get(key, GetRange::Full)
            .await
            .map_err(|source| SpanFetchError::Store {
                key: key.to_string(),
                source,
            })?;
        // Same one whole-object GET the row exit records: this change skips
        // decode, not fetch, so `page_bytes_fetched` cannot diverge here.
        accounting.record_s3_request(AccountedOp::Get);
        accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
        let bytes = got.data;

        let footer = open(&bytes).map_err(|source| corrupt(key, source))?;
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
            BlockProjection::columnar(projected_columns),
            accounting.clone(),
        )?;
        Ok(Some(scan))
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
        Ok(SpanColumnarScan {
            bytes,
            key,
            query,
            projection,
            candidates,
            cursor: 0,
            stats,
            accounting,
            page_bytes_fetched: 0,
            page_bytes_decoded: 0,
            finished: false,
        })
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
    /// The whole object. Candidate byte ranges are absolute offsets into it.
    bytes: Bytes,
    /// The object key, for error attribution.
    key: String,
    query: SpanQuery,
    projection: BlockProjection,
    /// Absolute, bounds-checked `(start, end, crc32c)` byte ranges of the
    /// candidate blocks, in scan order.
    candidates: Vec<CandidateBlock>,
    cursor: usize,
    stats: ScanStats,
    /// This query's accounting handle, folded once at exhaustion or drop with
    /// the scan's decode-time `page_bytes_fetched`/`page_bytes_decoded` totals
    /// (ADR-0107 decision 4). A separate, additive axis from the wire bytes the
    /// fetch funnel records through `add_s3_bytes`.
    accounting: QueryAccounting,
    page_bytes_fetched: u64,
    page_bytes_decoded: u64,
    /// Set once the accounting fold has run, so it runs exactly once.
    finished: bool,
}

impl SpanColumnarScan {
    /// The scan's pruning counters. `blocks_scanned` grows as blocks are
    /// consumed; read it after the last [`next_block`](Self::next_block).
    pub fn stats(&self) -> ScanStats {
        self.stats
    }

    /// Candidate blocks not yet decoded.
    pub fn remaining_blocks(&self) -> usize {
        self.candidates.len() - self.cursor
    }

    /// Decode the next candidate block and return it with its surviving row
    /// indices, or `None` once every candidate has been decoded.
    ///
    /// `Some(block)` with an empty `rows` is normal and distinct from `None`: a
    /// candidate block can survive pruning yet hold no row inside the ts window.
    /// Only `None` ends the scan and triggers the accounting fold.
    pub fn next_block(&mut self) -> Result<Option<ColumnarBlock>, SpanFetchError> {
        let Some(&(start, end, crc)) = self.candidates.get(self.cursor) else {
            self.finish();
            return Ok(None);
        };
        self.cursor += 1;
        let block_bytes = self.bytes.get(start..end).ok_or_else(|| {
            corrupt(
                &self.key,
                SpanSegError::Corrupted("block out of bounds".into()),
            )
        })?;
        let decoded = decode_block_accounted(block_bytes, crc, &self.projection)
            .map_err(|source| corrupt(&self.key, source))?;
        self.page_bytes_fetched += decoded.page_bytes_fetched;
        self.page_bytes_decoded += decoded.page_bytes_decoded;
        self.stats.blocks_scanned += 1;
        let rows = surviving_rows(&decoded.block, &self.query)
            .map_err(|source| corrupt(&self.key, source))?;
        Ok(Some(ColumnarBlock {
            block: decoded.block,
            rows,
        }))
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
fn drain_rows(mut scan: SpanColumnarScan) -> Result<SpanFetchOutput, SpanFetchError> {
    let mut records = Vec::new();
    while let Some(block) = scan.next_block()? {
        for &row in &block.rows {
            records.push(
                build_span_row(&block.block, row).map_err(|source| corrupt(&scan.key, source))?,
            );
        }
    }
    let stats = scan.stats();
    Ok(SpanFetchOutput { records, stats })
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

fn corrupt(key: &str, source: SpanSegError) -> SpanFetchError {
    SpanFetchError::Corrupt {
        key: key.to_string(),
        source,
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
}
