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
//! [`ravel_rspan::block::DecodedBlock::service_name`] -- and since this stays
//! scoped to `ravel-sql`, this fetcher assembles that scan here on top of the
//! public format surface rather than adding a bloom-aware `scan` to the reader.
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

use ravel_catalog::SegmentRef;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_rspan::block::{DEFAULT_MAX_UNCOMP, read_block};
use ravel_rspan::footer::kind;
use ravel_rspan::skip_index::BlockEntry;
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
        let bytes = got.data;

        let output = self
            .scan_object(&bytes, query, duration_ns, status_mask, predicates)
            .map_err(|source| corrupt(key, source))?;
        Ok(Some(output))
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
        // `scan_object` re-opens it for the BLOCKS section descriptor.
        let footer = open(&bytes).map_err(|source| corrupt(key, source))?;
        if footer.tenant_hash != tenant_hash.0 {
            return Err(SpanFetchError::TenantMismatch {
                key: key.to_string(),
            });
        }

        let output = self
            .scan_object(&bytes, query, duration_ns, status_mask, predicates)
            .map_err(|source| corrupt(key, source))?;
        Ok(Some(output))
    }

    /// Bloom-backed block scan over one whole-object's bytes. Kept a pure,
    /// store-free function so the block-level logic is unit-testable without an
    /// object store.
    fn scan_object(
        &self,
        bytes: &[u8],
        query: &SpanQuery,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        predicates: &[BloomPredicate<'_>],
    ) -> Result<SpanFetchOutput, SpanSegError> {
        let reader = RspanReader::new(bytes, &self.cfg)?;
        let skip = reader.skip_index();

        let mut stats = ScanStats {
            blocks_total: skip.blocks.len() as u32,
            ..ScanStats::default()
        };
        if query.ts_min > query.ts_max {
            return Ok(SpanFetchOutput {
                records: Vec::new(),
                stats,
            });
        }

        // Candidate blocks: the skip index's trace_id/ts prune, the
        // duration_ns/status_mask prune, plus the bloom-backed
        // prune when service_name/name predicates were pushed.
        let candidates = if predicates.is_empty() {
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
        stats.blocks_after_skip = candidates.len() as u32;

        // The BLOCKS section's absolute offset and length, for bounds-checked
        // per-block slicing (mirrors RspanReader's own block_bytes).
        let footer = open(bytes)?;
        let blocks = footer
            .section(kind::BLOCKS)
            .ok_or_else(|| SpanSegError::Corrupted("missing BLOCKS section".into()))?;
        let (blocks_offset, blocks_len) = (blocks.offset, blocks.len);

        let mut records = Vec::new();
        for &b in &candidates {
            let entry = &skip.blocks[b];
            let block_bytes = block_bytes(bytes, blocks_offset, blocks_len, entry)?;
            let decoded = read_block(block_bytes, entry.block_crc32c, DEFAULT_MAX_UNCOMP)?;
            for row in 0..decoded.record_count() {
                let record = decoded.record(row)?;
                if !matches(&record, query) {
                    continue;
                }
                // Read service_name straight from the v3 dictionary column
                // (ADR-0054), not by scanning the merged attrs map.
                let service_name =
                    match decoded.service_name(row) {
                        Some(v) => Some(String::from_utf8(v).map_err(|_| {
                            SpanSegError::Corrupted("service_name not utf-8".into())
                        })?),
                        None => None,
                    };
                records.push(SpanRow {
                    record,
                    service_name,
                });
            }
        }
        stats.blocks_scanned = candidates.len() as u32;
        Ok(SpanFetchOutput { records, stats })
    }
}

/// Slice of one block's stored bytes. `block_offset`/`block_len` are relative to
/// the BLOCKS section; the read is bounds-checked against the section's own
/// length (`blocks_len`), not merely the whole object, so a corrupt
/// `(offset, len)` decoded from SKIP_IDX can never land past the section end and
/// return foreign SKIP_IDX/footer bytes. Every violation is a typed
/// `Corrupted` error, never a panic. Mirrors `RspanReader::block_bytes`, which
/// is private to `ravel-rspan`.
fn block_bytes<'b>(
    bytes: &'b [u8],
    blocks_offset: u64,
    blocks_len: u64,
    entry: &BlockEntry,
) -> Result<&'b [u8], SpanSegError> {
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
    bytes
        .get(start..end)
        .ok_or_else(|| SpanSegError::Corrupted("block out of bounds".into()))
}

/// Exact per-row predicate: the span's `[start, end]` interval must overlap the
/// query window, and (when set) its trace_id must match. Identical to
/// `RspanReader`'s own `matches`, re-checked here because the skip index and
/// bloom are bounds only, not exact membership.
fn matches(rec: &SpanRecord, query: &SpanQuery) -> bool {
    if let Some(tid) = &query.trace_id
        && rec.trace_id != *tid
    {
        return false;
    }
    rec.start_ts_ns <= query.ts_max && rec.end_ts_ns >= query.ts_min
}

fn corrupt(key: &str, source: SpanSegError) -> SpanFetchError {
    SpanFetchError::Corrupt {
        key: key.to_string(),
        source,
    }
}
