//! SegmentFetcher: footer-first suffix reads, identity verification, matcher
//! pruning, and coalesced byte-range page fetches over one segment
//! (docs/query-engine.md "Flow", docs/segment-format.md reader protocol).

use bytes::Bytes;
use ravel_catalog::SegmentRef;
use ravel_object_store::{Etag, GetRange, ObjectStoreBackend, StoreError};
use ravel_promql::{LabelMatcher, matches_series};
use ravel_segment::{
    ExpectedIdentity, Footer, FooterOutcome, ReaderLimits, SeriesEntry, ValPageKind,
    check_identity, decode_catalog, decode_catalog_v2, decode_pages, decode_pages_soa,
    open_from_suffix, plan_ranges, select,
};
use ravel_types::{LabelSet, Sample, SeriesId, TenantHash};

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
    /// necessary, and verify identity against `expected`. Returns the
    /// trailer `version` (1 or 2) alongside the footer so callers can
    /// dispatch `decode_selected` on it (docs/segment-format.md "RSEG v2
    /// amendment": v1 and v2 objects coexist indefinitely, no compactor
    /// exists yet to retire v1 objects).
    async fn open_segment(
        &self,
        key: &str,
        expected: &ExpectedIdentity,
    ) -> Result<(Footer, u16, Etag, FetchedRegions), FetchError> {
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

        check_identity(&footer, expected).map_err(|source| corrupt(key, source))?;
        Ok((footer, version, suffix_etag, regions))
    }

    /// Decodes the catalog and returns the series matching `matchers`,
    /// fetching whatever byte ranges are not already covered by `regions`.
    /// Version-dispatched (docs/segment-format.md "RSEG v2 amendment"): v1
    /// fetches LABEL_DICT+SERIES_TABLE and decodes via `decode_catalog`,
    /// unchanged from before v2 existed; v2 fetches
    /// LABEL_DICT+SERIES_IDS+SERIES_META and decodes via `decode_catalog_v2`.
    /// Both produce the same `SeriesEntry` shape, so everything downstream
    /// of this function (`select`, `plan_ranges`, page decode) is
    /// version-blind.
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
            2 => {
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
                let (sm_off, sm_len) = section_range(footer, section_kind::SERIES_META)
                    .ok_or_else(|| {
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
                decode_catalog_v2(
                    footer,
                    &label_dict_bytes,
                    &series_ids_bytes,
                    &series_meta_bytes,
                    self.limits,
                )
                .map_err(|source| corrupt(key, source))?
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
        let (footer, version, suffix_etag, mut regions) = self.open_segment(key, &expected).await?;
        self.decode_selected(key, &footer, version, &suffix_etag, &mut regions, matchers)
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
        let (footer, version, suffix_etag, mut regions) = self.open_segment(key, &expected).await?;
        let entries = self
            .decode_selected(key, &footer, version, &suffix_etag, &mut regions, matchers)
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

    /// SoA counterpart to `fetch` (docs/arrow-datafusion-plan.md ticket
    /// A1a): decodes the same selected series but returns timestamps and
    /// values as separate vecs per series, plus page-kind stats. Reuses one
    /// decompression scratch buffer across every series in the segment;
    /// `timestamps`/`values` are fresh per series, since each is returned
    /// to the caller inside its own `FetchedSeriesSoa`.
    pub async fn fetch_soa(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<FetchedSeriesSoa>, FetchStats), FetchError> {
        let key = &seg_ref.data_object_key;
        let expected = expected_identity(tenant_hash, seg_ref);
        let (footer, version, suffix_etag, mut regions) = self.open_segment(key, &expected).await?;
        let entries = self
            .decode_selected(key, &footer, version, &suffix_etag, &mut regions, matchers)
            .await?;
        if entries.is_empty() {
            return Ok((Vec::new(), FetchStats::default()));
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

        let mut stats = FetchStats::default();
        let mut scratch = Vec::new();
        let mut out = Vec::with_capacity(entries.len());
        for (entry, plan) in entries.iter().zip(planned.iter()) {
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
}
