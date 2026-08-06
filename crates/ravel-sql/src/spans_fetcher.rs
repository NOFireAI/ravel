//! `SpanSegmentFetcher`: a fetch-and-scan abstraction over one RSPAN span
//! segment (crate `ravel-rspan`, docs/span-segment-format.md; ADR-0041 phase 5,
//! ADR-0054 bloom pruning).
//!
//! This is the span-signal sibling of `ravel-query`'s `LogSegmentFetcher`. It
//! lives here, in `ravel-sql`, rather than in `ravel-query` for one reason:
//! ADR-0041 phase 5 is the `spans` SQL table alone, and no span query surface
//! exists in `ravel-query` yet (`LogSegmentFetcher`'s home). The fetcher is
//! small, span-specific, and used only by [`crate::spans_scan`], so keeping it
//! beside the scan is the least-coupling choice until a broader span query path
//! (trace-by-id endpoint, service graph) earns a home in `ravel-query`.
//!
//! # Why this drives the block scan itself (issue #650, ADR-0054)
//!
//! v1 delegated the whole per-object scan to [`RspanReader::scan`], which prunes
//! blocks by trace_id range and time interval and re-evaluates survivors
//! exactly. v3 adds a per-block BLOOM over `service.name`/`name` tokens
//! ([`SkipIndex::candidate_blocks_with_bloom`], issue #649), but the reader's
//! `scan` still only takes a [`SpanQuery`] (ts window + trace_id) and cannot
//! carry a bloom predicate. `ravel-rspan` deliberately exposes the primitives
//! for a caller to assemble a bloom-backed scan itself -- [`RspanReader::bloom`]
//! and [`RspanReader::skip_index`], `candidate_blocks_with_bloom`,
//! [`ravel_rspan::block::read_block`], and
//! [`ravel_rspan::block::DecodedBlock::service_name`] -- and issue #650 is
//! scoped to `ravel-sql`, so this fetcher assembles that scan here on top of the
//! public format surface rather than adding a bloom-aware `scan` to the reader.
//! The block-slicing bounds check mirrors the reader's own (issue #349): a
//! `(block_offset, block_len)` decoded from SKIP_IDX is checked against the
//! BLOCKS section's own length, never merely the whole object, so a corrupt
//! offset can never return foreign bytes.
//!
//! Pruning stays widen-only (ADR-0013): the bloom's negative probe is a proof
//! the token is absent, so a skipped block truly held no matching row; a bloom
//! false positive only costs a wasted block decode, never a wrong or missing
//! row. `SpansTableProvider::supports_filters_pushdown` is `Inexact`, so
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

/// One scanned span: the rebuilt record plus its `service_name` read straight
/// from the v3 dictionary-encoded `COL_SERVICE_NAME` column (ADR-0054), rather
/// than looked up by linear scan of the record's merged `attrs` map at build
/// time. `None` when the span carried no `service.name`. The record's `attrs`
/// still carry `service.name` (the reader re-inserts it), so the public
/// `spans.attrs` column is unchanged; this field is the direct read that backs
/// the dedicated `service_name` column.
#[derive(Clone, Debug)]
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
/// `ravel_query::LogFetchError` so [`crate::error::SqlError`] can redact it the
/// same way (the `Display` embeds the object key, logged server-side only).
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
    /// - candidate blocks are chosen by the skip index's trace_id/ts prune and,
    ///   when `predicates` is non-empty, the bloom-backed
    ///   [`SkipIndex::candidate_blocks_with_bloom`] prune (a block whose bloom
    ///   proves a predicate's token absent is dropped before decode);
    /// - each surviving block is crc-verified and decoded, its rows
    ///   re-evaluated exactly against the ts window and (when set) trace_id, and
    ///   its `service_name` read straight from the v3 dictionary column.
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
            .scan_object(&bytes, query, predicates)
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

        // Candidate blocks: the skip index's trace_id/ts prune, plus the
        // bloom-backed prune when service_name/name predicates were pushed.
        let candidates = if predicates.is_empty() {
            skip.candidate_blocks(
                query.trace_id.as_ref(),
                query.ts_min,
                query.ts_max,
                None,
                None,
            )
        } else {
            let bloom = reader.bloom()?;
            skip.candidate_blocks_with_bloom(
                query.trace_id.as_ref(),
                query.ts_min,
                query.ts_max,
                None,
                None,
                &bloom,
                predicates,
            )?
        };
        stats.blocks_after_skip = candidates.len() as u32;

        // The BLOCKS section's absolute offset and length, for bounds-checked
        // per-block slicing (mirrors RspanReader's own block_bytes, issue #349).
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
/// return foreign SKIP_IDX/footer bytes (issue #349). Every violation is a typed
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
