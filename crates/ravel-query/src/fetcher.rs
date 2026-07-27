//! SegmentFetcher: footer-first suffix reads, identity verification, matcher
//! pruning, and coalesced byte-range page fetches over one segment
//! (docs/query-engine.md "Flow", docs/segment-format.md reader protocol).

use bytes::Bytes;
use ravel_catalog::SegmentRef;
use ravel_object_store::{Etag, GetRange, ObjectStoreBackend, StoreError};
use ravel_promql::{LabelMatcher, matches_series};
use ravel_segment::{
    ExpectedIdentity, Footer, FooterOutcome, ReaderLimits, SeriesEntry, check_identity,
    decode_catalog, decode_pages, open_from_suffix, plan_ranges, select,
};
use ravel_types::{LabelSet, Sample, SeriesId, TenantHash};

/// Section kinds from docs/segment-format.md. Not exported by `ravel-segment`
/// (its `format` module is private); these values are a persistent,
/// documented part of the on-disk contract, not an implementation detail.
mod section_kind {
    pub const LABEL_DICT: u32 = 1;
    pub const SERIES_TABLE: u32 = 2;
    #[allow(dead_code)] // completeness with the format doc; reads go via SeriesEntry offsets
    pub const TS_PAGES: u32 = 3;
    #[allow(dead_code)]
    pub const VAL_PAGES: u32 = 4;
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
                b.get(start_rel..end_rel).map(Bytes::copy_from_slice)
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
    /// necessary, and verify identity against `expected`.
    async fn open_segment(
        &self,
        key: &str,
        expected: &ExpectedIdentity,
    ) -> Result<(Footer, Etag, FetchedRegions), FetchError> {
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

        let footer = match open_from_suffix(&first.data, total_size, self.limits)
            .map_err(|source| corrupt(key, source))?
        {
            FooterOutcome::Ready(loc) => loc.footer,
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
                    FooterOutcome::Ready(loc) => loc.footer,
                    FooterOutcome::NeedRange { .. } => {
                        return Err(corrupt(key, ravel_segment::SegmentError::Truncated));
                    }
                }
            }
        };

        check_identity(&footer, expected).map_err(|source| corrupt(key, source))?;
        Ok((footer, suffix_etag, regions))
    }

    /// Decodes LABEL_DICT/SERIES_TABLE and returns the series matching
    /// `matchers`, fetching whatever byte ranges are not already covered by
    /// `regions`.
    async fn decode_selected(
        &self,
        key: &str,
        footer: &Footer,
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SeriesEntry>, FetchError> {
        let (ld_off, ld_len) =
            section_range(footer, section_kind::LABEL_DICT).ok_or_else(|| {
                corrupt(
                    key,
                    ravel_segment::SegmentError::MissingSection("LABEL_DICT"),
                )
            })?;
        let (st_off, st_len) =
            section_range(footer, section_kind::SERIES_TABLE).ok_or_else(|| {
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

        let entries = decode_catalog(footer, &label_dict_bytes, &series_table_bytes, self.limits)
            .map_err(|source| corrupt(key, source))?;
        let predicate: &dyn Fn(&LabelSet) -> bool = &|labels| matches_series(matchers, labels);
        Ok(select(&entries, &[], Some(predicate))
            .into_iter()
            .cloned()
            .collect())
    }

    /// Returns the series (labels only, no samples) in this segment matching
    /// `matchers`. Used by the labels/label-values/series HTTP endpoints,
    /// which never need page data.
    pub async fn fetch_series(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SeriesEntry>, FetchError> {
        let key = &seg_ref.data_object_key;
        let expected = expected_identity(tenant_hash, seg_ref);
        let (footer, suffix_etag, mut regions) = self.open_segment(key, &expected).await?;
        self.decode_selected(key, &footer, &suffix_etag, &mut regions, matchers)
            .await
    }

    /// Fetches and decodes the samples of every series in this segment
    /// matching `matchers`.
    pub async fn fetch(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<FetchedSeries>, FetchError> {
        let key = &seg_ref.data_object_key;
        let expected = expected_identity(tenant_hash, seg_ref);
        let (footer, suffix_etag, mut regions) = self.open_segment(key, &expected).await?;
        let entries = self
            .decode_selected(key, &footer, &suffix_etag, &mut regions, matchers)
            .await?;
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let selected_refs: Vec<&SeriesEntry> = entries.iter().collect();
        let planned =
            plan_ranges(&footer, &selected_refs).map_err(|source| corrupt(key, source))?;
        let page_ranges: Vec<(u64, u64)> = planned
            .iter()
            .flat_map(|p| {
                [
                    (p.ts_range.0, p.ts_range.0 + p.ts_range.1),
                    (p.val_range.0, p.val_range.0 + p.val_range.1),
                ]
            })
            .collect();
        self.ensure_ranges(key, &suffix_etag, &page_ranges, &mut regions)
            .await?;

        let mut out = Vec::with_capacity(entries.len());
        for (entry, plan) in entries.iter().zip(planned.iter()) {
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
