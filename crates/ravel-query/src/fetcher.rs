//! SegmentFetcher: footer-first suffix reads, identity verification, matcher
//! pruning, and coalesced byte-range page fetches over one segment
//! (docs/query-engine.md "Flow", docs/segment-format.md reader protocol).

use bytes::Bytes;
use futures::future::join_all;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_object_store::{Etag, GetOutcome, GetRange, ObjectStoreBackend, StoreError};
use ravel_promql::{LabelMatcher, matches_series};
use ravel_segment::{
    ExpectedIdentity, Footer, FooterOutcome, HistogramValue, ReaderLimits, RunEntry, SeriesEntry,
    SeriesEntryV4, ValPageKind, ValueKind, check_identity, decode_catalog_v4, decode_catalog_v5,
    decode_run_histogram_pages, decode_run_pages_soa, open_from_suffix, plan_ranges_v4,
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
/// Default size at or below which the first GET fetches the whole object in
/// one request instead of a footer suffix (#278 item 5). The commit record
/// carries the exact object size, so a small object is read whole up front:
/// its footer, catalog sections, and page bytes then all resolve from that
/// one buffer with no second probe. Above the threshold the footer-suffix
/// path is kept, which reads only the tail and coalesces the page GETs it
/// actually needs. 512 KiB comfortably covers a typical ~30 KiB L0 flush and
/// the small end of the L1 part distribution while never whole-object-reading
/// a large compacted part just to reach its footer.
pub const DEFAULT_WHOLE_OBJECT_THRESHOLD: u64 = 512 * 1024;
/// Default bound on the number of byte-range GETs a single segment fetch
/// keeps in flight at once (#278 item 2). The fetcher clones share one
/// semaphore, so this also bounds the total in-flight GETs across every
/// concurrent segment fetch in a query, not just within one segment.
pub const DEFAULT_MAX_CONCURRENT_GETS: usize = 16;

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

/// Histogram counterpart to `FetchedSeriesSoa` (#218): the decoded
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
    /// Object size at or below which the first GET reads the whole object
    /// instead of a footer suffix (#278 item 5).
    whole_object_threshold: u64,
    limits: ReaderLimits,
    /// Bounds the byte-range GETs kept in flight. Shared across `clone`s (it
    /// is an `Arc`), so every concurrent segment fetch in one query draws
    /// from the same permit pool rather than each opening its own unbounded
    /// fan-out (#278 item 2). It is only ever held around a single leaf
    /// `store.get`, never across a scope that acquires another permit, so no
    /// fetch can deadlock waiting on permits it already holds.
    get_semaphore: std::sync::Arc<tokio::sync::Semaphore>,
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

    /// Sets the whole-object fetch threshold (#278 item 5). An object whose
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

    /// Sets the in-flight byte-range GET bound (#278 item 2). Shared across
    /// this fetcher's clones.
    #[must_use]
    pub fn with_max_concurrent_gets(mut self, n: usize) -> Self {
        self.get_semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(n.max(1)));
        self
    }

    /// One store GET, bounded by the shared in-flight semaphore (#278 item
    /// 2). The permit is released the moment the GET resolves; callers must
    /// never hold the returned future's permit across another
    /// `guarded_get`/`ensure_ranges` call, or a query whose in-flight GETs
    /// already fill the pool could wait on itself.
    async fn guarded_get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let _permit =
            self.get_semaphore.acquire().await.map_err(|_| {
                StoreError::Transient("fetch concurrency semaphore closed".to_string())
            })?;
        self.store.get(key, range).await
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
        // Fetch the coalesced ranges concurrently rather than one await at a
        // time (#278 item 2): a multi-page selection over a large segment
        // used to pay one round trip per coalesced range in series. The
        // shared semaphore inside `guarded_get` bounds the actual in-flight
        // GETs; `join_all` preserves input order, so the resulting
        // `regions` insert order is identical to the old sequential loop.
        let gets = join_all(coalesce_ranges(missing, self.coalesce_gap).into_iter().map(
            |(start, end)| async move {
                let got = self
                    .guarded_get(key, GetRange::Range(start, end))
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
                Ok::<(u64, Bytes), FetchError>((start, got.data))
            },
        ))
        .await;
        for got in gets {
            let (start, data) = got?;
            regions.insert(start, data);
        }
        Ok(())
    }

    /// Opens a segment: suffix-GET, chase `NeedRange` for the footer if
    /// necessary, and verify identity. Returns the object's `total_size`
    /// alongside the footer; ADR-0027 leaves v5 the only version, so
    /// `open_from_suffix` has already rejected anything else and there is no
    /// per-version dispatch left for callers to do.
    ///
    /// Identity verification is level-aware
    /// (docs/compaction-retention-plan.md §3.5). An L0 ref verifies the
    /// footer's writer identity against the commit record (ADR-0010 §7). An
    /// L1 part ref has no writer identity of its own, so the footer's
    /// tenant/shard/ingest_hour/input_set_hash/part_index (and `level == 1`)
    /// are verified against the compaction record's fields the ref carries.
    async fn open_segment(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
    ) -> Result<(Footer, u64, Etag, FetchedRegions), FetchError> {
        let key = &seg_ref.data_object_key;
        // Size-aware first GET (#278 item 5): the commit record already
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
        let first =
            self.guarded_get(key, first_range)
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
                    .guarded_get(key, GetRange::Range(offset, offset + len))
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
    /// Returns the plan for every run of every scalar series.
    async fn fetch_scalar_pages(
        &self,
        key: &str,
        footer: &Footer,
        scalar: &[&SeriesEntryV4],
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
    ) -> Result<Vec<ravel_segment::PlannedRunRange>, FetchError> {
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
        self.ensure_ranges(key, suffix_etag, &page_ranges, regions)
            .await?;
        Ok(planned)
    }

    /// Histogram counterpart to [`fetch_scalar_pages`](Self::fetch_scalar_pages)
    /// (#218): coalesced TS/HIST page ranges for the histogram runs of
    /// `histogram`, fetched into `regions`. `plan_ranges_v4` fills each
    /// histogram run's `hist_range` (and leaves `val_range` a `(0, 0)`
    /// sentinel), so this fetches the TS and HIST byte ranges the histogram
    /// decode path reads.
    async fn fetch_histogram_pages(
        &self,
        key: &str,
        footer: &Footer,
        histogram: &[&SeriesEntryV4],
        suffix_etag: &Etag,
        regions: &mut FetchedRegions,
    ) -> Result<Vec<ravel_segment::PlannedRunRange>, FetchError> {
        let planned = plan_ranges_v4(footer, histogram).map_err(|source| corrupt(key, source))?;
        let page_ranges: Vec<(u64, u64)> = planned
            .iter()
            .flat_map(|p| {
                [
                    (p.ts_range.0, p.ts_range.0 + p.ts_range.1),
                    (p.hist_range.0, p.hist_range.0 + p.hist_range.1),
                ]
            })
            .collect();
        self.ensure_ranges(key, suffix_etag, &page_ranges, regions)
            .await?;
        Ok(planned)
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
    ) -> Result<ValPageKind, FetchError> {
        let ts_bytes = regions
            .slice(plan.ts_range.0, plan.ts_range.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let val_bytes = regions
            .slice(plan.val_range.0, plan.val_range.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
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
        Ok(kind)
    }

    /// Histogram counterpart to [`decode_run`](Self::decode_run) (#218):
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
    ) -> Result<(), FetchError> {
        let ts_bytes = regions
            .slice(plan.ts_range.0, plan.ts_range.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let hist_bytes = regions
            .slice(plan.hist_range.0, plan.hist_range.1)
            .ok_or_else(|| corrupt(key, ravel_segment::SegmentError::SectionOutOfBounds))?;
        let samples =
            decode_run_histogram_pages(series_id, run, &ts_bytes, &hist_bytes, self.limits)
                .map_err(|source| corrupt(key, source))?;
        for sample in samples {
            timestamps.push(sample.ts_ns);
            values.push(sample.value);
        }
        Ok(())
    }

    /// Core of `fetch`/`fetch_soa`: decodes every matched scalar series into
    /// one [`RunDecode`] per emitted unit, with resolved provenance keyed on
    /// the segment level (docs/compaction-retention-plan.md §3.5):
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
    /// [`fetch_histogram_runs`](Self::fetch_histogram_runs) instead (#218).
    async fn fetch_runs(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
        count_stats: bool,
    ) -> Result<(Vec<RunDecode>, FetchStats), FetchError> {
        let key = &seg_ref.data_object_key;
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref).await?;
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
        let scalar: Vec<&SeriesEntryV4> = selected
            .iter()
            .filter(|e| e.entry.value_kind == ValueKind::Scalar)
            .collect();
        if scalar.is_empty() {
            return Ok((Vec::new(), FetchStats::default()));
        }
        let planned = self
            .fetch_scalar_pages(key, &footer, &scalar, &suffix_etag, &mut regions)
            .await?;
        self.build_scalar_decodes(key, seg_ref, &scalar, &planned, &regions, count_stats)
    }

    /// Decodes the already-fetched scalar page bytes of `scalar` into one
    /// [`RunDecode`] per emitted unit (see [`fetch_runs`](Self::fetch_runs)
    /// for the per-level emission contract). Split out so the scalar-only
    /// [`fetch_runs`](Self::fetch_runs) and the combined
    /// [`fetch_runs_and_histograms`](Self::fetch_runs_and_histograms) share
    /// one decode body rather than drifting apart (#278 item 1).
    fn build_scalar_decodes(
        &self,
        key: &str,
        seg_ref: &SegmentRef,
        scalar: &[&SeriesEntryV4],
        planned: &[ravel_segment::PlannedRunRange],
        regions: &FetchedRegions,
        count_stats: bool,
    ) -> Result<(Vec<RunDecode>, FetchStats), FetchError> {
        let mut stats = FetchStats::default();
        let mut scratch = Vec::new();
        let mut out = Vec::with_capacity(scalar.len());
        for entry in scalar {
            match &seg_ref.level {
                SegmentLevel::L0 => {
                    // One unit per series: concatenate every run's samples in
                    // on-disk order, segment-level provenance. `decode_run`
                    // appends (its page decoders append, #283), so the shared
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
                        let kind = self.decode_run(
                            key,
                            &entry.entry.series_id,
                            run,
                            plan,
                            regions,
                            &mut scratch,
                            &mut timestamps,
                            &mut values,
                        )?;
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
                        let kind = self.decode_run(
                            key,
                            &entry.entry.series_id,
                            run,
                            plan,
                            regions,
                            &mut scratch,
                            &mut timestamps,
                            &mut values,
                        )?;
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
        Ok((out, stats))
    }

    /// Histogram counterpart to [`fetch_runs`](Self::fetch_runs) (#218):
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
    ) -> Result<Vec<RunHistogramDecode>, FetchError> {
        let key = &seg_ref.data_object_key;
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref).await?;
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
        let histogram: Vec<&SeriesEntryV4> = selected
            .iter()
            .filter(|e| e.entry.value_kind == ValueKind::Histogram)
            .collect();
        if histogram.is_empty() {
            return Ok(Vec::new());
        }
        let planned = self
            .fetch_histogram_pages(key, &footer, &histogram, &suffix_etag, &mut regions)
            .await?;
        self.build_histogram_decodes(key, seg_ref, &histogram, &planned, &regions)
    }

    /// Histogram counterpart to
    /// [`build_scalar_decodes`](Self::build_scalar_decodes): decodes the
    /// already-fetched histogram page bytes of `histogram` into one
    /// [`RunHistogramDecode`] per emitted unit. Shared by the histogram-only
    /// [`fetch_histogram_runs`](Self::fetch_histogram_runs) and the combined
    /// [`fetch_runs_and_histograms`](Self::fetch_runs_and_histograms) (#278
    /// item 1).
    fn build_histogram_decodes(
        &self,
        key: &str,
        seg_ref: &SegmentRef,
        histogram: &[&SeriesEntryV4],
        planned: &[ravel_segment::PlannedRunRange],
        regions: &FetchedRegions,
    ) -> Result<Vec<RunHistogramDecode>, FetchError> {
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
                        self.decode_histogram_run(
                            key,
                            &entry.entry.series_id,
                            run,
                            plan,
                            regions,
                            &mut timestamps,
                            &mut values,
                        )?;
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
                        self.decode_histogram_run(
                            key,
                            &entry.entry.series_id,
                            run,
                            plan,
                            regions,
                            &mut timestamps,
                            &mut values,
                        )?;
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
        Ok(out)
    }

    /// Opens the segment once and decodes both its matched scalar and matched
    /// histogram series in a single pass (#278 item 1). The scalar-only
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
    ) -> Result<(Vec<RunDecode>, FetchStats, Vec<RunHistogramDecode>), FetchError> {
        let key = &seg_ref.data_object_key;
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref).await?;
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
            self.fetch_scalar_pages(key, &footer, &scalar, &suffix_etag, &mut regions)
                .await?
        };
        let histogram_planned = if histogram.is_empty() {
            Vec::new()
        } else {
            self.fetch_histogram_pages(key, &footer, &histogram, &suffix_etag, &mut regions)
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
            )?
        };
        let histogram_out = if histogram.is_empty() {
            Vec::new()
        } else {
            self.build_histogram_decodes(key, seg_ref, &histogram, &histogram_planned, &regions)?
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
        let key = &seg_ref.data_object_key;
        let (footer, total_size, suffix_etag, mut regions) =
            self.open_segment(tenant_hash, seg_ref).await?;
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
    /// For an L1 part this emits one [`FetchedSeries`] per (series, run) with
    /// the run's provenance; for an L0 segment one per series with the
    /// segment's provenance (see [`fetch_runs`](Self::fetch_runs)).
    pub async fn fetch(
        &self,
        tenant_hash: TenantHash,
        seg_ref: &SegmentRef,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<FetchedSeries>, FetchError> {
        let (runs, _stats) = self
            .fetch_runs(tenant_hash, seg_ref, matchers, false)
            .await?;
        Ok(runs.into_iter().map(RunDecode::into_aos).collect())
    }

    /// SoA counterpart to `fetch` (docs/arrow-datafusion-plan.md ticket
    /// A1a): decodes the same selected scalar series but returns timestamps
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
        let (runs, stats) = self
            .fetch_runs(tenant_hash, seg_ref, matchers, true)
            .await?;
        Ok((runs.into_iter().map(RunDecode::into_soa).collect(), stats))
    }

    /// Histogram counterpart to [`fetch_soa`](Self::fetch_soa) (#218): fetches
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
        let runs = self
            .fetch_histogram_runs(tenant_hash, seg_ref, matchers)
            .await?;
        Ok(runs
            .into_iter()
            .map(RunHistogramDecode::into_fetched)
            .collect())
    }

    /// Single-open counterpart to calling [`fetch_soa`](Self::fetch_soa) and
    /// [`fetch_histograms`](Self::fetch_histograms) back to back on the same
    /// segment (#278 item 1): opens the segment once, decodes its catalog
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
        let (runs, stats, hist_runs) = self
            .fetch_runs_and_histograms(tenant_hash, seg_ref, matchers, true)
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

/// Histogram counterpart to [`RunDecode`] (#218): one decoded,
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
    fn into_fetched(self) -> FetchedHistogramSeries {
        FetchedHistogramSeries {
            series_id: self.series_id,
            labels: self.labels,
            timestamps: self.timestamps,
            values: self.values,
            created_unix_ns: self.created_unix_ns,
            writer_epoch: self.writer_epoch,
            writer_seq: self.writer_seq,
        }
    }
}

/// Verify an L1 part's v5 footer against the compaction record's identity
/// fields the [`SegmentRef`] carries (docs/compaction-retention-plan.md
/// §3.5: readers verify tenant/shard/ingest_hour/input_set_hash/part_index
/// against the record, the L1 analog of ADR-0010 §7). A part has no writer
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

    /// #278 item 1: opening a segment once with `fetch_soa_and_histograms`
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

    /// #278 item 5: below the whole-object threshold the first GET reads the
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
