//! SegmentFetcher: footer-first suffix reads, identity verification, matcher
//! pruning, and coalesced byte-range page fetches over one segment
//! (docs/query-engine.md "Flow", docs/segment-format.md reader protocol).

use bytes::Bytes;
use ravel_catalog::SegmentRef;
use ravel_object_store::{Etag, GetRange, ObjectStoreBackend, StoreError};
use ravel_promql::{LabelMatcher, matches_series};
use ravel_segment::{
    ExpectedIdentity, Footer, FooterOutcome, ReaderLimits, SeriesEntry, SeriesEntryV4, ValPageKind,
    ValueKind, check_identity, decode_catalog_v4, decode_catalog_v5, decode_run_pages_soa,
    open_from_suffix, plan_ranges_v4,
};
use ravel_types::{LabelSet, Sample, SeriesId, TenantHash};

/// Section kinds from docs/segment-format.md (not exported by
/// `ravel-segment`). LABEL_DICT + SERIES_IDS + SERIES_META are the catalog
/// sections a below-threshold v5 object carries; their absence (SERIES_META
/// replaced by the chunked SERIES_META_CHUNKS) marks a sparse object.
const SECTION_LABEL_DICT: u32 = 1;
const SECTION_SERIES_IDS: u32 = 5;
const SECTION_SERIES_META: u32 = 6;

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
    /// necessary, and verify identity against `expected`. Returns the object's
    /// `total_size` alongside the footer; ADR-0027 leaves v5 the only version,
    /// so `open_from_suffix` has already rejected anything else and there is
    /// no per-version dispatch left for callers to do.
    async fn open_segment(
        &self,
        key: &str,
        expected: &ExpectedIdentity,
    ) -> Result<(Footer, u64, Etag, FetchedRegions), FetchError> {
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
        Ok((footer, total_size, suffix_etag, regions))
    }

    /// Decodes the catalog and returns the run-major series matching
    /// `matchers`, fetching only the catalog sections. Below the sparse
    /// threshold a v5 object carries the whole SERIES_META (kind 6): fetch
    /// LABEL_DICT + SERIES_IDS + SERIES_META and decode the run-major catalog,
    /// so a label-pruned read fetches no page bytes it will not return. At or
    /// above the threshold the chunked catalog spans sections, so fall back to
    /// a whole-object decode (selective sparse reads within one large segment
    /// are a compacted-tier concern, #111). Page bytes are fetched afterwards
    /// by the caller from `regions`.
    async fn decode_selected(
        &self,
        key: &str,
        footer: &Footer,
        total_size: u64,
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SeriesEntryV4>, FetchError> {
        let entries = if let Some((sm_off, sm_len)) = section_range(footer, SECTION_SERIES_META) {
            let (ld_off, ld_len) = section_range(footer, SECTION_LABEL_DICT).ok_or_else(|| {
                corrupt(
                    key,
                    ravel_segment::SegmentError::MissingSection("LABEL_DICT"),
                )
            })?;
            let (si_off, si_len) = section_range(footer, SECTION_SERIES_IDS).ok_or_else(|| {
                corrupt(
                    key,
                    ravel_segment::SegmentError::MissingSection("SERIES_IDS"),
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
        } else {
            self.ensure_ranges(key, suffix_etag, &[(0, total_size)], regions)
                .await?;
            let object = regions
                .slice(0, total_size)
                .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
            decode_catalog_v5(footer, &object, self.limits)
                .map_err(|source| corrupt(key, source))?
        };
        Ok(entries
            .into_iter()
            .filter(|e| matches_series(matchers, &e.entry.labels))
            .collect())
    }

    /// Coalesced page ranges for the scalar runs of `selected` (histogram
    /// runs carry no scalar samples and are skipped), fetched into `regions`.
    async fn fetch_scalar_pages(
        &self,
        key: &str,
        footer: &Footer,
        selected: &[SeriesEntryV4],
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
    ) -> Result<Vec<ravel_segment::PlannedRunRange>, FetchError> {
        let selected_refs: Vec<&SeriesEntryV4> = selected.iter().collect();
        let planned =
            plan_ranges_v4(footer, &selected_refs).map_err(|source| corrupt(key, source))?;
        let mut page_ranges = Vec::new();
        for entry in selected {
            if entry.entry.value_kind == ValueKind::Histogram {
                continue;
            }
            for run_index in 0..entry.runs.len() {
                if let Some(p) = find_run_plan(&planned, &entry.entry.series_id, run_index) {
                    page_ranges.push((p.ts_range.0, p.ts_range.0 + p.ts_range.1));
                    page_ranges.push((p.val_range.0, p.val_range.0 + p.val_range.1));
                }
            }
        }
        self.ensure_ranges(key, suffix_etag, &page_ranges, regions)
            .await?;
        Ok(planned)
    }

    /// Returns the series (labels only, no samples) in this segment matching
    /// `matchers`. Used by the labels/label-values/series HTTP endpoints,
    /// which never need page data. Returns the folded per-series
    /// [`SeriesEntry`] view (labels + identity).
    pub async fn fetch_series(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SeriesEntry>, FetchError> {
        let key = &seg_ref.data_object_key;
        let expected = expected_identity(tenant_hash, seg_ref);
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(key, &expected).await?;
        let selected = self
            .decode_selected(
                key,
                &footer,
                total_size,
                &suffix_etag,
                &mut regions,
                matchers,
            )
            .await?;
        Ok(selected.into_iter().map(|e| e.entry).collect())
    }

    /// Fetches and decodes the scalar samples of every series in this segment
    /// matching `matchers`. Histogram-kind series carry no scalar samples and
    /// are skipped: the scalar query path (PromQL/SQL) does not consume them.
    pub async fn fetch(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<FetchedSeries>, FetchError> {
        let key = &seg_ref.data_object_key;
        let expected = expected_identity(tenant_hash, seg_ref);
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(key, &expected).await?;
        let selected = self
            .decode_selected(
                key,
                &footer,
                total_size,
                &suffix_etag,
                &mut regions,
                matchers,
            )
            .await?;
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let planned = self
            .fetch_scalar_pages(key, &footer, &selected, &suffix_etag, &mut regions)
            .await?;

        let mut scratch = Vec::new();
        let mut out = Vec::with_capacity(selected.len());
        for entry in &selected {
            if entry.entry.value_kind == ValueKind::Histogram {
                continue;
            }
            let mut samples = Vec::new();
            for (run_index, run) in entry.runs.iter().enumerate() {
                let plan = find_run_plan(&planned, &entry.entry.series_id, run_index)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                let ts_bytes = regions
                    .slice(plan.ts_range.0, plan.ts_range.1)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                let val_bytes = regions
                    .slice(plan.val_range.0, plan.val_range.1)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                let mut timestamps = Vec::new();
                let mut values = Vec::new();
                decode_run_pages_soa(
                    &entry.entry.series_id,
                    run,
                    &ts_bytes,
                    &val_bytes,
                    self.limits,
                    &mut scratch,
                    &mut timestamps,
                    &mut values,
                )
                .map_err(|source| corrupt(key, source))?;
                samples.extend(
                    timestamps
                        .into_iter()
                        .zip(values)
                        .map(|(ts_ns, value)| Sample { ts_ns, value }),
                );
            }
            out.push(FetchedSeries {
                series_id: entry.entry.series_id,
                labels: entry.entry.labels.clone(),
                samples,
                created_unix_ns: seg_ref.created_unix_ns,
                writer_epoch: seg_ref.writer_epoch,
                writer_seq: seg_ref.writer_seq,
            });
        }
        Ok(out)
    }

    /// SoA counterpart to `fetch` (docs/arrow-datafusion-plan.md ticket
    /// A1a): decodes the same selected scalar series but returns timestamps
    /// and values as separate vecs per series, plus page-kind stats. Reuses
    /// one decompression scratch buffer across every run in the segment.
    pub async fn fetch_soa(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<FetchedSeriesSoa>, FetchStats), FetchError> {
        let key = &seg_ref.data_object_key;
        let expected = expected_identity(tenant_hash, seg_ref);
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(key, &expected).await?;
        let selected = self
            .decode_selected(
                key,
                &footer,
                total_size,
                &suffix_etag,
                &mut regions,
                matchers,
            )
            .await?;
        if selected.is_empty() {
            return Ok((Vec::new(), FetchStats::default()));
        }
        let planned = self
            .fetch_scalar_pages(key, &footer, &selected, &suffix_etag, &mut regions)
            .await?;

        let mut stats = FetchStats::default();
        let mut scratch = Vec::new();
        let mut out = Vec::with_capacity(selected.len());
        for entry in &selected {
            if entry.entry.value_kind == ValueKind::Histogram {
                continue;
            }
            let mut timestamps = Vec::new();
            let mut values = Vec::new();
            for (run_index, run) in entry.runs.iter().enumerate() {
                let plan = find_run_plan(&planned, &entry.entry.series_id, run_index)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                let ts_bytes = regions
                    .slice(plan.ts_range.0, plan.ts_range.1)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                let val_bytes = regions
                    .slice(plan.val_range.0, plan.val_range.1)
                    .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
                let mut run_ts = Vec::new();
                let mut run_vals = Vec::new();
                let val_kind = decode_run_pages_soa(
                    &entry.entry.series_id,
                    run,
                    &ts_bytes,
                    &val_bytes,
                    self.limits,
                    &mut scratch,
                    &mut run_ts,
                    &mut run_vals,
                )
                .map_err(|source| corrupt(key, source))?;
                stats.record_val_page(val_kind, val_bytes.len());
                timestamps.append(&mut run_ts);
                values.append(&mut run_vals);
            }
            out.push(FetchedSeriesSoa {
                series_id: entry.entry.series_id,
                labels: entry.entry.labels.clone(),
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
