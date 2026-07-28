//! SegmentFetcher: footer-first suffix reads, identity verification, matcher
//! pruning, and coalesced byte-range page fetches over one segment
//! (docs/query-engine.md "Flow", docs/segment-format.md reader protocol).

use std::collections::HashMap;

use bytes::Bytes;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_object_store::{Etag, GetRange, ObjectStoreBackend, StoreError};
use ravel_promql::{LabelMatcher, matches_series};
use ravel_segment::{
    ExpectedIdentity, Footer, FooterOutcome, ReaderLimits, SeriesEntry, SeriesEntryV4, ValPageKind,
    ValueKind, check_identity, decode_catalog, decode_catalog_v2, decode_catalog_v3,
    decode_catalog_v4, decode_pages, decode_pages_soa, decode_run_pages_soa, open_from_suffix,
    plan_ranges, plan_ranges_v3, plan_ranges_v4, select,
};
use ravel_types::{LabelSet, Sample, SeriesId, TenantHash};

/// RSEG trailer versions this fetcher decodes. v1/v2/v3 are L0 flush
/// formats (ADR-0014, ADR-0017); v4 is the compaction (L1) format
/// (docs/compaction-retention-plan.md §4), whose per-series runs each
/// become one [`FetchedSeriesSoa`].
const VERSION_V3: u16 = 3;
const VERSION_V4: u16 = 4;

/// Section kinds from docs/segment-format.md. Not exported by `ravel-segment`
/// (its `format` module is private); these values are a persistent,
/// documented part of the on-disk contract, not an implementation detail.
/// Kinds 1-4 are v1 (SERIES_TABLE only exists in v1 objects); 5/6 are v2
/// only (docs/segment-format.md "RSEG v2 amendment", ADR-0014).
mod section_kind {
    pub const LABEL_DICT: u32 = 1;
    pub const SERIES_TABLE: u32 = 2;
    #[allow(dead_code)] // completeness with the format doc; reads go via SeriesEntry offsets
    pub const TS_PAGES: u32 = 3;
    #[allow(dead_code)]
    pub const VAL_PAGES: u32 = 4;
    pub const SERIES_IDS: u32 = 5;
    pub const SERIES_META: u32 = 6;
}

/// Default suffix length fetched on the first GET of a segment object.
pub const DEFAULT_SUFFIX_LEN: u64 = 64 * 1024;
/// Default maximum gap between two planned byte ranges that still get
/// coalesced into a single GET.
pub const DEFAULT_COALESCE_GAP: u64 = 64 * 1024;

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
}

/// SoA counterpart to `FetchedSeries` (docs/arrow-datafusion-plan.md ticket
/// A1a): timestamps and values as separate vecs, ready for zero-copy Arrow
/// buffer adoption in `ravel-sql` (Phase B). Same provenance fields, same
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
}

/// Page-kind counters accumulated over one `fetch_soa` call, for issue #25
/// (X1) to consume later. Currently tracks VAL_RAW_F64 pages only.
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
                // copy (docs/arrow-datafusion-plan.md hop 6, review F1):
                // `b` is `Bytes`, so `slice` shares the backing allocation.
                // `end_rel <= b.len()` holds by the range check above.
                Some(b.slice(start_rel..end_rel))
            } else {
                None
            }
        })
    }
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

fn section_range(footer: &Footer, kind: u32) -> Option<(u64, u64)> {
    footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .map(|s| (s.offset, s.len))
}

/// Fetches and decodes one segment at a time: suffix-GET the footer, verify
/// identity, prune series by matchers, plan and coalesce page ranges, decode
/// selected pages. See docs/query-engine.md "Flow" for the full contract.
#[derive(Clone)]
pub struct SegmentFetcher {
    store: std::sync::Arc<dyn ObjectStoreBackend>,
    suffix_len: u64,
    coalesce_gap: u64,
    limits: ReaderLimits,
}

impl SegmentFetcher {
    pub fn new(store: std::sync::Arc<dyn ObjectStoreBackend>) -> Self {
        SegmentFetcher {
            store,
            suffix_len: DEFAULT_SUFFIX_LEN,
            coalesce_gap: DEFAULT_COALESCE_GAP,
            limits: ReaderLimits::default(),
        }
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

    async fn ensure_ranges(
        &self,
        key: &str,
        suffix_etag: &Etag,
        needed: &[(u64, u64)],
        regions: &mut FetchedRegions,
    ) -> Result<(), FetchError> {
        let missing: Vec<(u64, u64)> = needed
            .iter()
            .copied()
            .filter(|(start, end)| !regions.covers(*start, *end))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        for (start, end) in coalesce_ranges(missing, self.coalesce_gap) {
            let got = self
                .store
                .get(key, GetRange::Range(start, end))
                .await
                .map_err(|source| FetchError::Store {
                    key: key.to_string(),
                    source,
                })?;
            if &got.etag != suffix_etag {
                return Err(FetchError::EtagChanged {
                    key: key.to_string(),
                });
            }
            regions.insert(start, got.data);
        }
        Ok(())
    }

    /// Opens a segment: suffix-GET, chase `NeedRange` for the footer if
    /// necessary, and verify identity. Returns the trailer `version`
    /// alongside the footer so callers can dispatch page decode on it.
    ///
    /// Identity verification is level-aware
    /// (docs/compaction-retention-plan.md §3.5). An L0 ref verifies the
    /// footer's writer identity against the commit record (ADR-0010 §7,
    /// unchanged). An L1 part ref verifies the v4 footer's
    /// tenant/shard/ingest_hour/input_set_hash/part_index against the
    /// compaction record's fields the ref carries: a part has no writer
    /// identity, so the five record-derived fields are the identity.
    async fn open_segment(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
    ) -> Result<(Footer, u16, Etag, FetchedRegions), FetchError> {
        let key = &seg_ref.data_object_key;
        let first = self
            .store
            .get(key, GetRange::Suffix(self.suffix_len))
            .await
            .map_err(|source| FetchError::Store {
                key: key.to_string(),
                source,
            })?;
        let total_size = first.total_size;
        let suffix_etag = first.etag.clone();
        let mut regions = FetchedRegions::default();
        let first_start = total_size.saturating_sub(first.data.len() as u64);
        regions.insert(first_start, first.data.clone());

        let (footer, version) = match open_from_suffix(&first.data, total_size, self.limits)
            .map_err(|source| corrupt(key, source))?
        {
            FooterOutcome::Ready(loc) => (loc.footer, loc.version),
            FooterOutcome::NeedRange { offset, len } => {
                let got = self
                    .store
                    .get(key, GetRange::Range(offset, offset + len))
                    .await
                    .map_err(|source| FetchError::Store {
                        key: key.to_string(),
                        source,
                    })?;
                if got.etag != suffix_etag {
                    return Err(FetchError::EtagChanged {
                        key: key.to_string(),
                    });
                }
                regions.insert(offset, got.data.clone());
                match open_from_suffix(&got.data, total_size, self.limits)
                    .map_err(|source| corrupt(key, source))?
                {
                    FooterOutcome::Ready(loc) => (loc.footer, loc.version),
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
                verify_l1_identity(
                    &footer,
                    version,
                    tenant_hash,
                    seg_ref,
                    input_set_hash,
                    *part_index,
                )
                .map_err(|source| corrupt(key, source))?;
            }
        }
        Ok((footer, version, suffix_etag, regions))
    }

    /// Decodes the catalog and returns the per-series [`SeriesEntry`] view
    /// of the series matching `matchers`, fetching whatever byte ranges are
    /// not already covered by `regions`. Version-dispatched: v1 uses
    /// LABEL_DICT+SERIES_TABLE (`decode_catalog`); v2/v3/v4 use
    /// LABEL_DICT+SERIES_IDS+SERIES_META (`decode_catalog_v2`/`_v3`/`_v4`),
    /// with v4 folded to the per-series `SeriesEntry` shape. All produce the
    /// same shape, so labels-only callers (`fetch_series`) are version-blind;
    /// the sample paths handle v4's per-run page data separately.
    async fn decode_selected(
        &self,
        key: &str,
        footer: &Footer,
        version: u16,
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SeriesEntry>, FetchError> {
        let entries = match version {
            1 => {
                let (ld_off, ld_len) =
                    section_range(footer, section_kind::LABEL_DICT).ok_or_else(|| {
                        corrupt(
                            key,
                            ravel_segment::SegmentError::MissingSection("LABEL_DICT"),
                        )
                    })?;
                let (st_off, st_len) = section_range(footer, section_kind::SERIES_TABLE)
                    .ok_or_else(|| {
                        corrupt(
                            key,
                            ravel_segment::SegmentError::MissingSection("SERIES_TABLE"),
                        )
                    })?;
                self.ensure_ranges(
                    key,
                    suffix_etag,
                    &[(ld_off, ld_off + ld_len), (st_off, st_off + st_len)],
                    regions,
                )
                .await?;
                let label_dict_bytes = regions
                    .slice(ld_off, ld_len)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                let series_table_bytes = regions
                    .slice(st_off, st_len)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                decode_catalog(footer, &label_dict_bytes, &series_table_bytes, self.limits)
                    .map_err(|source| corrupt(key, source))?
            }
            2 | VERSION_V3 | VERSION_V4 => {
                let (label_dict_bytes, series_ids_bytes, series_meta_bytes) = self
                    .ensure_catalog_sections(key, footer, suffix_etag, regions)
                    .await?;
                match version {
                    2 => decode_catalog_v2(
                        footer,
                        &label_dict_bytes,
                        &series_ids_bytes,
                        &series_meta_bytes,
                        self.limits,
                    )
                    .map_err(|source| corrupt(key, source))?,
                    VERSION_V3 => decode_catalog_v3(
                        footer,
                        &label_dict_bytes,
                        &series_ids_bytes,
                        &series_meta_bytes,
                        self.limits,
                    )
                    .map_err(|source| corrupt(key, source))?,
                    // v4: fold each multi-run series to the per-series
                    // `SeriesEntry` shape (labels only are needed here; the
                    // per-run page data is decoded by the v4 sample path).
                    _ => decode_catalog_v4(
                        footer,
                        &label_dict_bytes,
                        &series_ids_bytes,
                        &series_meta_bytes,
                        self.limits,
                    )
                    .map_err(|source| corrupt(key, source))?
                    .into_iter()
                    .map(|e| e.entry)
                    .collect(),
                }
            }
            other => {
                return Err(corrupt(
                    key,
                    ravel_segment::SegmentError::UnsupportedVersion(other),
                ));
            }
        };
        let predicate: &dyn Fn(&LabelSet) -> bool = &|labels| matches_series(matchers, labels);
        Ok(select(&entries, &[], Some(predicate))
            .into_iter()
            .cloned()
            .collect())
    }

    /// Fetches the LABEL_DICT, SERIES_IDS, and SERIES_META section byte
    /// slices shared by the v2/v3/v4 catalog decoders (their stored,
    /// crc-covered bytes; the decoders decompress and verify). v2/v3/v4 all
    /// write the three contiguously, so `ensure_ranges` coalesces them into
    /// one GET.
    async fn ensure_catalog_sections(
        &self,
        key: &str,
        footer: &Footer,
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
    ) -> Result<(Bytes, Bytes, Bytes), FetchError> {
        let (ld_off, ld_len) =
            section_range(footer, section_kind::LABEL_DICT).ok_or_else(|| {
                corrupt(
                    key,
                    ravel_segment::SegmentError::MissingSection("LABEL_DICT"),
                )
            })?;
        let (si_off, si_len) =
            section_range(footer, section_kind::SERIES_IDS).ok_or_else(|| {
                corrupt(
                    key,
                    ravel_segment::SegmentError::MissingSection("SERIES_IDS"),
                )
            })?;
        let (sm_off, sm_len) =
            section_range(footer, section_kind::SERIES_META).ok_or_else(|| {
                corrupt(
                    key,
                    ravel_segment::SegmentError::MissingSection("SERIES_META"),
                )
            })?;
        self.ensure_ranges(
            key,
            suffix_etag,
            &[
                (ld_off, ld_off + ld_len),
                (si_off, si_off + si_len),
                (sm_off, sm_off + sm_len),
            ],
            regions,
        )
        .await?;
        let label_dict_bytes = regions
            .slice(ld_off, ld_len)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let series_ids_bytes = regions
            .slice(si_off, si_len)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let series_meta_bytes = regions
            .slice(sm_off, sm_len)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        Ok((label_dict_bytes, series_ids_bytes, series_meta_bytes))
    }

    /// Decodes the v4 catalog (per-series folded entry plus per-run view)
    /// and returns the series matching `matchers`
    /// (docs/compaction-retention-plan.md §4). The per-run page data lives
    /// in [`SeriesEntryV4::runs`]; the sample paths turn each run into one
    /// [`FetchedSeriesSoa`]/[`FetchedSeries`].
    async fn decode_v4_selected(
        &self,
        key: &str,
        footer: &Footer,
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SeriesEntryV4>, FetchError> {
        let (label_dict_bytes, series_ids_bytes, series_meta_bytes) = self
            .ensure_catalog_sections(key, footer, suffix_etag, regions)
            .await?;
        let entries = decode_catalog_v4(
            footer,
            &label_dict_bytes,
            &series_ids_bytes,
            &series_meta_bytes,
            self.limits,
        )
        .map_err(|source| corrupt(key, source))?;
        Ok(entries
            .into_iter()
            .filter(|e| matches_series(matchers, &e.entry.labels))
            .collect())
    }

    /// Returns the series (labels only, no samples) in this segment matching
    /// `matchers`. Used by the labels/label-values/series HTTP endpoints,
    /// which never need page data. Version-blind: v4 multi-run series fold
    /// to the same per-series `SeriesEntry` shape.
    pub async fn fetch_series(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SeriesEntry>, FetchError> {
        let key = &seg_ref.data_object_key;
        let (footer, version, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref).await?;
        self.decode_selected(key, &footer, version, &suffix_etag, &mut regions, matchers)
            .await
    }

    /// Fetches and decodes the samples of every series in this segment
    /// matching `matchers`. For a v4 (L1) part this emits one
    /// [`FetchedSeries`] per (series, run) with the run's provenance; for
    /// v1/v2/v3 one per series with the segment's provenance.
    pub async fn fetch(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<FetchedSeries>, FetchError> {
        let key = &seg_ref.data_object_key;
        let (footer, version, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref).await?;
        if version == VERSION_V4 {
            let (out, _stats) = self
                .fetch_v4_runs(key, &footer, &suffix_etag, &mut regions, matchers, false)
                .await?;
            return Ok(out.into_iter().map(RunDecode::into_aos).collect());
        }

        let entries = self
            .decode_selected(key, &footer, version, &suffix_etag, &mut regions, matchers)
            .await?;
        let selected_refs = scalar_refs(&entries);
        if selected_refs.is_empty() {
            return Ok(Vec::new());
        }
        let planned = plan_scalar_ranges(&footer, &selected_refs, version)
            .map_err(|source| corrupt(key, source))?;
        self.ensure_ranges(
            key,
            &suffix_etag,
            &scalar_page_ranges(&planned),
            &mut regions,
        )
        .await?;

        let mut out = Vec::with_capacity(selected_refs.len());
        for (entry, plan) in selected_refs.iter().zip(planned.iter()) {
            let ts_bytes = regions
                .slice(plan.ts_range.0, plan.ts_range.1)
                .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
            let val_bytes = regions
                .slice(plan.val_range.0, plan.val_range.1)
                .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
            let samples = decode_pages(entry, &ts_bytes, &val_bytes, self.limits)
                .map_err(|source| corrupt(key, source))?;
            out.push(FetchedSeries {
                series_id: entry.series_id,
                labels: entry.labels.clone(),
                samples,
                created_unix_ns: seg_ref.created_unix_ns,
                writer_epoch: seg_ref.writer_epoch,
                writer_seq: seg_ref.writer_seq,
            });
        }
        Ok(out)
    }

    /// SoA counterpart to `fetch` (docs/arrow-datafusion-plan.md ticket
    /// A1a): decodes the same selected series but returns timestamps and
    /// values as separate vecs, plus page-kind stats. For a v4 (L1) part
    /// this emits one [`FetchedSeriesSoa`] per (series, run) with the run's
    /// provenance from the v4 catalog (docs/compaction-retention-plan.md
    /// §3.5); for v1/v2/v3 one per series. Reuses one decompression scratch
    /// buffer across every page in the segment.
    pub async fn fetch_soa(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<FetchedSeriesSoa>, FetchStats), FetchError> {
        let key = &seg_ref.data_object_key;
        let (footer, version, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref).await?;
        if version == VERSION_V4 {
            let (runs, stats) = self
                .fetch_v4_runs(key, &footer, &suffix_etag, &mut regions, matchers, true)
                .await?;
            return Ok((runs.into_iter().map(RunDecode::into_soa).collect(), stats));
        }

        let entries = self
            .decode_selected(key, &footer, version, &suffix_etag, &mut regions, matchers)
            .await?;
        let selected_refs = scalar_refs(&entries);
        if selected_refs.is_empty() {
            return Ok((Vec::new(), FetchStats::default()));
        }
        let planned = plan_scalar_ranges(&footer, &selected_refs, version)
            .map_err(|source| corrupt(key, source))?;
        self.ensure_ranges(
            key,
            &suffix_etag,
            &scalar_page_ranges(&planned),
            &mut regions,
        )
        .await?;

        let mut stats = FetchStats::default();
        let mut scratch = Vec::new();
        let mut out = Vec::with_capacity(selected_refs.len());
        for (entry, plan) in selected_refs.iter().zip(planned.iter()) {
            let ts_bytes = regions
                .slice(plan.ts_range.0, plan.ts_range.1)
                .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
            let val_bytes = regions
                .slice(plan.val_range.0, plan.val_range.1)
                .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
            let mut timestamps = Vec::new();
            let mut values = Vec::new();
            let val_kind = decode_pages_soa(
                entry,
                &ts_bytes,
                &val_bytes,
                self.limits,
                &mut scratch,
                &mut timestamps,
                &mut values,
            )
            .map_err(|source| corrupt(key, source))?;
            stats.record_val_page(val_kind, val_bytes.len());
            out.push(FetchedSeriesSoa {
                series_id: entry.series_id,
                labels: entry.labels.clone(),
                timestamps,
                values,
                created_unix_ns: seg_ref.created_unix_ns,
                writer_epoch: seg_ref.writer_epoch,
                writer_seq: seg_ref.writer_seq,
            });
        }
        Ok((out, stats))
    }

    /// Decodes a v4 (L1) part's scalar series into one [`RunDecode`] per
    /// (series, run) (docs/compaction-retention-plan.md §3.5). Each run
    /// carries its own provenance (`created_unix_ns`, `writer_epoch`,
    /// `writer_seq`) copied from the v4 catalog, so cross-input duplicate
    /// samples resolve under the same total order as the pre-compaction L0
    /// segments would. Histogram-valued series are skipped here: histogram
    /// query support has no fetch path yet, and a scalar SoA cannot hold
    /// them (the same reason `count_raw` gates VAL stats).
    async fn fetch_v4_runs(
        &self,
        key: &str,
        footer: &Footer,
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
        matchers: &[LabelMatcher],
        count_stats: bool,
    ) -> Result<(Vec<RunDecode>, FetchStats), FetchError> {
        let selected = self
            .decode_v4_selected(key, footer, suffix_etag, regions, matchers)
            .await?;
        let scalar: Vec<&SeriesEntryV4> = selected
            .iter()
            .filter(|e| e.entry.value_kind == ValueKind::Scalar)
            .collect();
        if scalar.is_empty() {
            return Ok((Vec::new(), FetchStats::default()));
        }
        let planned = plan_ranges_v4(footer, &scalar).map_err(|source| corrupt(key, source))?;
        let page_ranges: Vec<(u64, u64)> = planned
            .iter()
            .flat_map(|p| {
                [
                    (p.ts_range.0, p.ts_range.0 + p.ts_range.1),
                    (p.val_range.0, p.val_range.0 + p.val_range.1),
                ]
            })
            .collect();
        self.ensure_ranges(key, suffix_etag, &page_ranges, regions)
            .await?;

        let by_id: HashMap<SeriesId, &SeriesEntryV4> =
            scalar.iter().map(|e| (e.entry.series_id, *e)).collect();

        let mut stats = FetchStats::default();
        let mut scratch = Vec::new();
        let mut out = Vec::with_capacity(planned.len());
        for plan in &planned {
            let series = by_id
                .get(&plan.series_id)
                .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
            let run = series
                .runs
                .get(plan.run_index)
                .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
            let ts_bytes = regions
                .slice(plan.ts_range.0, plan.ts_range.1)
                .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
            let val_bytes = regions
                .slice(plan.val_range.0, plan.val_range.1)
                .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
            let mut timestamps = Vec::new();
            let mut values = Vec::new();
            let val_kind = decode_run_pages_soa(
                &plan.series_id,
                run,
                &ts_bytes,
                &val_bytes,
                self.limits,
                &mut scratch,
                &mut timestamps,
                &mut values,
            )
            .map_err(|source| corrupt(key, source))?;
            if count_stats {
                stats.record_val_page(val_kind, val_bytes.len());
            }
            out.push(RunDecode {
                series_id: plan.series_id,
                labels: series.entry.labels.clone(),
                timestamps,
                values,
                created_unix_ns: run.created_unix_ns,
                writer_epoch: run.writer_epoch,
                writer_seq: run.writer_seq,
            });
        }
        Ok((out, stats))
    }
}

/// One decoded v4 run, convertible to either the AoS or SoA fetched shape.
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
    fn into_soa(self) -> FetchedSeriesSoa {
        FetchedSeriesSoa {
            series_id: self.series_id,
            labels: self.labels,
            timestamps: self.timestamps,
            values: self.values,
            created_unix_ns: self.created_unix_ns,
            writer_epoch: self.writer_epoch,
            writer_seq: self.writer_seq,
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
        }
    }
}

/// Selects the scalar series among decoded entries. v1/v2 entries are all
/// scalar; v3 may carry histogram series, which the scalar sample path
/// cannot decode and no histogram query path consumes yet, so they are
/// filtered out here.
fn scalar_refs(entries: &[SeriesEntry]) -> Vec<&SeriesEntry> {
    entries
        .iter()
        .filter(|e| e.value_kind == ValueKind::Scalar)
        .collect()
}

/// Plans TS/VAL ranges for scalar `selected` entries, using the
/// version-appropriate planner: v3 keeps VAL_PAGES optional
/// (`plan_ranges_v3`), while v1/v2 always have it (`plan_ranges`).
fn plan_scalar_ranges(
    footer: &Footer,
    selected: &[&SeriesEntry],
    version: u16,
) -> Result<Vec<ravel_segment::PlannedRange>, ravel_segment::SegmentError> {
    if version == VERSION_V3 {
        plan_ranges_v3(footer, selected)
    } else {
        plan_ranges(footer, selected)
    }
}

fn scalar_page_ranges(planned: &[ravel_segment::PlannedRange]) -> Vec<(u64, u64)> {
    planned
        .iter()
        .flat_map(|p| {
            [
                (p.ts_range.0, p.ts_range.0 + p.ts_range.1),
                (p.val_range.0, p.val_range.0 + p.val_range.1),
            ]
        })
        .collect()
}

/// Verify an L1 part's v4 footer against the compaction record's identity
/// fields the [`SegmentRef`] carries (docs/compaction-retention-plan.md
/// §3.5: readers verify tenant/shard/ingest_hour/input_set_hash/part_index
/// against the record, the L1 analog of ADR-0010 §7). A part has no writer
/// identity, so these five fields are the identity. `level` must also be 1.
fn verify_l1_identity(
    footer: &Footer,
    version: u16,
    tenant_hash: TenantHash,
    seg_ref: &SegmentRef,
    input_set_hash: &[u8; 32],
    part_index: u32,
) -> Result<(), ravel_segment::SegmentError> {
    if version != VERSION_V4 {
        return Err(ravel_segment::SegmentError::IdentityMismatch(
            "segment_format_version",
        ));
    }
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use ravel_catalog::SegmentRef;
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
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

    // --- RSEG v2 fetch tests (docs/segment-format.md "RSEG v2 amendment",
    // docs/rseg-v2-plan.md phase P3, issue #31): same MemoryStore-backed
    // pattern as the v1 tests above, constructing the object via
    // `SegmentWriter::write_v2` instead of `write`. `fetch`/`fetch_soa`
    // never branch on version explicitly (only `open_segment` and
    // `decode_selected` do), so these tests are the proof that dispatch
    // actually reaches `decode_catalog_v2` for a real v2 object end to end
    // (suffix GET -> footer -> section GETs -> catalog decode -> page
    // decode), not just that `ravel-segment`'s decoder works in isolation.

    /// Builds a real RSEG v2 object (same two-series shape as
    /// `write_test_segment`: one Gorilla-friendly series, one that forces
    /// VAL_RAW_F64) and its matching `SegmentRef`, without putting it on
    /// any store -- callers choose the backend (plain `MemoryStore`,
    /// `MemoryStore::with_page_size`, or a `FaultStore`-wrapped one).
    fn build_v2_segment() -> (bytes::Bytes, TenantHash, SegmentRef) {
        let tenant_hash = TenantHash([7u8; 16]);
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
        let smooth = series(
            "smooth_metric",
            &[(1_000 * NS, 1.0), (1_001 * NS, 1.0), (1_002 * NS, 1.0)],
        );
        let chaotic = series(
            "chaotic_metric",
            &[(1_000 * NS, 0.0), (1_001 * NS, f64::from_bits(u64::MAX))],
        );
        let written = SegmentWriter::write_v2(vec![smooth, chaotic], identity, bounds)
            .expect("write v2 segment");

        let key = "test/segment_v2.rseg";
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
        (written.bytes, tenant_hash, seg_ref)
    }

    /// Asserts the shape `write_test_segment`/`build_v2_segment` both
    /// produce: two series ("smooth_metric" Gorilla-coded, "chaotic_metric"
    /// forced to VAL_RAW_F64), decoded correctly regardless of which
    /// section kinds backed the catalog.
    fn assert_two_metric_fetch(fetched: &mut [FetchedSeries]) {
        assert_eq!(fetched.len(), 2);
        fetched.sort_by_key(|s| s.labels.get("__name__").map(str::to_string));
        assert_eq!(fetched[0].labels.get("__name__"), Some("chaotic_metric"));
        assert_eq!(fetched[0].samples.len(), 2);
        assert_eq!(
            fetched[0].samples[1].value.to_bits(),
            f64::from_bits(u64::MAX).to_bits()
        );
        assert_eq!(fetched[1].labels.get("__name__"), Some("smooth_metric"));
        assert_eq!(fetched[1].samples.len(), 3);
        for s in &fetched[1].samples {
            assert_eq!(s.value.to_bits(), 1.0f64.to_bits());
        }
    }

    #[tokio::test]
    async fn fetch_decodes_v2_segments_via_memory_store() {
        let (bytes, tenant_hash, seg_ref) = build_v2_segment();
        let store = Arc::new(MemoryStore::new());
        store
            .put(&seg_ref.data_object_key, bytes, PutOptions::default())
            .await
            .expect("put v2 segment object");
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let fetcher = SegmentFetcher::new(backend);

        let mut fetched = fetcher
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch v2 segment");
        assert_two_metric_fetch(&mut fetched);

        // fetch_series (labels-only, no page GETs) must agree on identity
        // and label sets with the full fetch above.
        let mut series_only = fetcher
            .fetch_series(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch_series v2 segment");
        series_only.sort_by_key(|e| e.series_id.0);
        let mut full_ids: Vec<_> = fetched.iter().map(|f| f.series_id).collect();
        full_ids.sort_by_key(|id| id.0);
        assert_eq!(
            series_only.iter().map(|e| e.series_id).collect::<Vec<_>>(),
            full_ids
        );

        // fetch_soa must decode the same bytes to the same values and
        // still count the VAL_RAW_F64 page.
        let (mut soa, stats) = fetcher
            .fetch_soa(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch_soa v2 segment");
        assert_eq!(soa.len(), 2);
        soa.sort_by_key(|s| s.labels.get("__name__").map(str::to_string));
        assert_eq!(soa[0].labels.get("__name__"), Some("chaotic_metric"));
        assert_eq!(soa[1].labels.get("__name__"), Some("smooth_metric"));
        assert_eq!(stats.raw_f64_pages, 1);
        assert_eq!(stats.raw_f64_bytes, 6 + 2 * 8);
    }

    /// `MemoryStore::with_page_size` shrinks the store's *listing* page
    /// size; the fetch path here never calls `list`/`list_delimited` (it
    /// resolves the object purely by key, via suffix and range GETs), so
    /// this knob is a no-op for `SegmentFetcher`. Included because the
    /// ticket asks for it explicitly; the assertions are the same as the
    /// plain-`MemoryStore` test above, run against a store constructed
    /// with a tiny page size to document that it makes no difference here.
    #[tokio::test]
    async fn fetch_decodes_v2_segments_with_small_store_page_size() {
        let (bytes, tenant_hash, seg_ref) = build_v2_segment();
        let store = Arc::new(MemoryStore::with_page_size(2));
        store
            .put(&seg_ref.data_object_key, bytes, PutOptions::default())
            .await
            .expect("put v2 segment object");
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let fetcher = SegmentFetcher::new(backend);

        let mut fetched = fetcher
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch v2 segment");
        assert_two_metric_fetch(&mut fetched);
    }

    /// Fault injection targeted at the v2 catalog section GET specifically,
    /// not just "some GET on this object". With `with_suffix_len(64)` (well
    /// under this fixture's ~400-byte object size), a successful
    /// `fetch_series` (no page GETs, isolating `open_segment` +
    /// `decode_selected`) makes exactly 3 `Get` calls: (1) the initial
    /// suffix, (2) `open_segment`'s `NeedRange` chase for the footer, (3)
    /// `decode_selected`'s v2-branch GET for LABEL_DICT+SERIES_IDS+
    /// SERIES_META (coalesced into one range, since v2 writes them
    /// contiguously). Verified empirically (not assumed) by sweeping
    /// `Occurrence::Nth(1..=6)` against this exact fixture: Nth(1..=3) each
    /// independently caused the fetch to fail, Nth(4)+ never fired. `Nth(3)`
    /// is therefore the call that only happens once the v2 catalog decode
    /// path has been reached -- unlike `Occurrence::Always`, which would
    /// equally fail on call 1 (the footer suffix GET) and prove nothing
    /// v2-specific. A control run with the same `with_suffix_len(64)` and
    /// no fault plan confirms the multi-GET sequence is real and would
    /// otherwise succeed, so the failure below is attributable to the
    /// injected fault, not some other reason the 3rd GET might fail.
    #[tokio::test]
    async fn fetch_v2_segment_catalog_get_fault_is_surfaced_and_counted() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };

        let (bytes, tenant_hash, seg_ref) = build_v2_segment();

        // Control: same multi-GET setup, no fault, must succeed -- proves
        // call #3 exists and is reachable absent the injected fault.
        let control_inner = MemoryStore::new();
        control_inner
            .put(
                &seg_ref.data_object_key,
                bytes.clone(),
                PutOptions::default(),
            )
            .await
            .expect("put v2 segment object");
        let control_backend: Arc<dyn ObjectStoreBackend> = Arc::new(control_inner);
        let control_fetcher = SegmentFetcher::new(control_backend).with_suffix_len(64);
        control_fetcher
            .fetch_series(tenant_hash, &seg_ref, &[])
            .await
            .expect("control fetch (no fault) must succeed");

        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("injected".into()))
                .with_key_contains(seg_ref.data_object_key.clone())
                .with_occurrence(Occurrence::Nth(3)),
        );
        let inner = MemoryStore::new();
        inner
            .put(&seg_ref.data_object_key, bytes, PutOptions::default())
            .await
            .expect("put v2 segment object");
        let store = Arc::new(FaultStore::new(inner, plan));
        let backend: Arc<dyn ObjectStoreBackend> = store.clone();
        let fetcher = SegmentFetcher::new(backend).with_suffix_len(64);

        let result = fetcher.fetch_series(tenant_hash, &seg_ref, &[]).await;
        assert!(
            matches!(result, Err(FetchError::Store { .. })),
            "expected a store error, got {result:?}"
        );
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Permanent),
            1,
            "expected the injected fault to have fired exactly once"
        );
    }

    // --- coalesce_ranges / FetchedRegions unit coverage (a5-F01).
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

    #[test]
    fn fetched_regions_does_not_cover_a_range_straddling_two_buffers() {
        // A sub-range that spans a buffer boundary is reported uncovered, so
        // the fetcher refetches it rather than stitching bytes from two GETs
        // (docs/reviews .../a5-fetch-object-store.md §5 "duplicate fetches").
        let mut regions = FetchedRegions::default();
        regions.insert(0, Bytes::from(vec![0u8; 10]));
        regions.insert(10, Bytes::from(vec![1u8; 10]));
        assert!(regions.covers(2, 8));
        assert!(regions.covers(12, 18));
        assert!(!regions.covers(8, 12), "a straddling range is not covered");
        assert!(regions.slice(8, 4).is_none());
    }
}
