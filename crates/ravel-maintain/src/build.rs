//! Whole-bucket catalog group-by and verbatim-page copy into RSEG v5 parts
//! (docs/compaction-retention-plan.md §3.3 steps 3-4, with the ADR-0026/0027
//! v5 writer substitution). Every input catalog's series is grouped into one
//! `BTreeMap` keyed by series id (all inputs' per-series metadata resident at
//! once), then iterated in id order; this is a group-by-then-iterate, not a
//! bounded-window k-way merge. Only the page bytes stream: every run of a
//! series is gathered from every input that carries it, its TS and VAL-or-HIST
//! pages are fetched by range and copied verbatim (never decoded), and parts
//! are split on series boundaries once accumulated page bytes reach
//! `max_l1_part_bytes`, so a series' runs never straddle a part and part
//! id-ranges are disjoint. Each finished part is a whole object written with a
//! single `CreateIfAbsent` PUT (no multipart; issue #243), and its encoded
//! bytes are retained in the returned `Vec` until publish so the
//! convergence-repair path can re-PUT a part a racing winner is missing.
//!
//! Page fetch mechanics (#279, perf epic #264): the pages for one part are
//! fetched as a batch, not one blocking GET per page. First the per-run TS and
//! VAL-or-HIST byte ranges of every series in the batch are grouped by input
//! object and coalesced (adjacent/near ranges merged into one GET, mirroring
//! `ravel-query`'s `SegmentFetcher::coalesce_ranges`), collapsing the ~2R
//! sequential GETs the naive path issued toward a couple of GETs per input.
//! The coalesced GETs then run concurrently under a bounded semaphore + a
//! `buffer_unordered` window, so a bucket's page fetch collapses from k RTT
//! toward ceil(k/parallelism) RTT on real object storage. Emission stays
//! deterministic: results are collected before any page is materialized, then
//! sliced (zero-copy `Bytes::slice`, no per-page `to_vec` off the wire) into
//! `RunInputV4` in the exact series-then-run order the sequential path used,
//! so the built part bytes are identical. Fetch buffering is bounded to one
//! part's worth of page bytes because a batch is exactly the series that fit
//! under `max_l1_part_bytes`; the encoded parts held until publish still
//! dominate peak memory (plan §3.3 memory bound).

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt, iter as stream_iter};
use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::CompactionPart;
use ravel_segment::{
    CompactionMetaV4, ExemplarInput, IngestBounds, RunInputV4, RunValuePageV4, SegmentIdentity,
    SegmentWriter, SeriesInputV4, ValueKind,
};
use ravel_types::{LabelSet, SeriesId};
use tokio::sync::Semaphore;

use crate::bucket::Bucket;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read::{InputCatalog, InputRecord, RunPlan, SeriesPlan};

/// Maximum concurrent page GETs in flight across a whole `build_parts` call.
/// A single global cap (one shared [`Semaphore`], the "one global semaphore"
/// docs/reviews/2026-07-30-performance-investigation.md §10 calls for) driving
/// a `buffer_unordered` window of the same width: high enough to hide
/// per-request latency on real object storage, low enough not to trip a
/// bucket's per-prefix request-rate throttle.
const FETCH_CONCURRENCY: usize = 16;

/// Largest gap between two planned byte ranges from the same object that still
/// get merged into one GET (mirrors `ravel-query`'s `DEFAULT_COALESCE_GAP`).
/// Consecutive runs' pages within one input's TS or VAL section are contiguous
/// (gap 0), so this mainly bridges the small alignment padding between them;
/// the gap bytes are the only over-fetch coalescing can cause, so it is kept
/// modest.
const COALESCE_GAP: u64 = 64 * 1024;

/// The current RSEG output version (ADR-0026, made the only version by
/// ADR-0027). Recorded in each part's `CompactionPart.segment_format_version`.
pub const OUTPUT_FORMAT_VERSION: u32 = ravel_segment::VERSION_V6 as u32;

/// One built (not yet published) L1 part: its content-addressed key, its
/// bytes, and the [`CompactionPart`] describing it for the record.
#[derive(Debug, Clone)]
pub struct BuiltPart {
    pub key: String,
    pub bytes: Bytes,
    pub part: CompactionPart,
}

/// Merge all inputs into size-capped v5 parts (plan §3.3). `catalogs` MUST be
/// aligned with `inputs` (both in canonical input order): the alignment is
/// what makes run tie-breaking by canonical input position deterministic.
///
/// `catalogs` is taken by value, and this call is their last use, so the
/// exemplar records are moved out of them (`std::mem::take` below) rather than
/// cloned: the retained records exist in exactly one place at a time, which is
/// what keeps them inside `read.rs`'s stated catalog-metadata memory bound
/// (issue #557). Only the borrowed page-range metadata (`&SeriesPlan`,
/// `object_key`) is read after the take.
pub async fn build_parts(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    inputs: &[InputRecord],
    mut catalogs: Vec<InputCatalog>,
    input_set_hash: &[u8; 32],
) -> Result<Vec<BuiltPart>> {
    if inputs.len() != catalogs.len() {
        return Err(MaintainError::Invariant(
            "inputs and catalogs length mismatch".to_string(),
        ));
    }
    let ingest_bounds = merged_ingest_bounds(inputs);
    let input_set_hash16 = hex::encode(&input_set_hash[..8]);

    // Every input's exemplars, grouped by the series they name, in canonical
    // input order then each object's own stored order (ADR-0047 decision 3).
    // The records are MOVED out of the catalogs (`std::mem::take`), not cloned:
    // `read.rs` argues the retained exemplars fit the catalog-metadata memory
    // bound, and that holds for one copy, not the originals plus a full clone
    // set live at once (issue #557). Grouping is only how a part collects the
    // exemplars of the series it carries: nothing here merges, deduplicates,
    // re-caps, or re-sorts them, and two inputs each carrying an exemplar for
    // the same (series, ts) both stay. `exemplar_total` is what the per-part
    // assignment below must conserve.
    let mut exemplars_by_series: BTreeMap<[u8; 16], Vec<ExemplarInput>> = BTreeMap::new();
    let mut exemplar_total = 0usize;
    for catalog in &mut catalogs {
        for e in std::mem::take(&mut catalog.exemplars) {
            exemplar_total += 1;
            exemplars_by_series
                .entry(e.series_id.0)
                .or_default()
                .push(e);
        }
    }

    // Group every series across every input by id, carrying the input index
    // so pages can be fetched from the right object. Inserting in canonical
    // input order means each id's contribution list is already in canonical
    // input order, which is the run tie-break rule (plan §3.3 step 3).
    let mut by_series: BTreeMap<[u8; 16], Vec<(usize, &SeriesPlan)>> = BTreeMap::new();
    for (idx, catalog) in catalogs.iter().enumerate() {
        for series in &catalog.series {
            by_series
                .entry(series.series_id.0)
                .or_default()
                .push((idx, series));
        }
    }
    let object_keys: Vec<&str> = catalogs.iter().map(|c| c.object_key.as_str()).collect();

    // Flatten the group-by into an ordered per-series build plan (metadata
    // only, no page bytes). This is the exact series-then-run emission order
    // the sequential path used, with each run's predicted page-byte size
    // (`ts_abs` len + `page_abs` len) precomputed from the catalog so part
    // boundaries are chosen without fetching a single page.
    let mut builds: Vec<SeriesBuild> = Vec::with_capacity(by_series.len());
    for (_id, contributions) in by_series {
        let mut runs: Vec<(&str, &RunPlan)> = Vec::new();
        let mut labels: Option<&LabelSet> = None;
        let mut kind: Option<ValueKind> = None;
        let mut series_id = None;
        let mut page_bytes: u64 = 0;

        for (input_idx, plan) in contributions {
            let object_key = object_keys[input_idx];
            if labels.is_none() {
                labels = Some(&plan.labels);
                kind = Some(plan.kind);
                series_id = Some(plan.series_id);
            } else if kind != Some(plan.kind) {
                return Err(MaintainError::Invariant(format!(
                    "series {} has mixed value kinds across inputs",
                    plan.series_id.to_hex()
                )));
            }
            for run in &plan.runs {
                page_bytes = page_bytes
                    .saturating_add(run.ts_abs.1)
                    .saturating_add(run.page_abs.1);
                runs.push((object_key, run));
            }
        }

        let (Some(labels), Some(series_id)) = (labels, series_id) else {
            continue;
        };
        builds.push(SeriesBuild {
            series_id,
            labels,
            runs,
            page_bytes,
        });
    }

    // One shared cap for every batch's fetches (batches run sequentially, so
    // this bounds total in-flight GETs at `FETCH_CONCURRENCY`).
    let semaphore = Semaphore::new(FETCH_CONCURRENCY);

    let mut parts = Vec::new();
    let mut part_index: u32 = 0;
    let mut batch_start = 0usize;
    let mut batch_bytes: u64 = 0;

    // Accumulate series into a batch until its predicted page bytes reach the
    // cap, exactly as the sequential path did (the last series pushes a batch
    // over the cap; a single series larger than the cap is its own part). Then
    // fetch that batch's pages (coalesced + concurrent), materialize them in
    // order, and flush the part.
    let mut exemplars_assigned = 0usize;
    for i in 0..builds.len() {
        batch_bytes = batch_bytes.saturating_add(builds[i].page_bytes);
        if batch_bytes >= config.max_l1_part_bytes {
            let batch = &builds[batch_start..=i];
            let batch_exemplars = take_batch_exemplars(&mut exemplars_by_series, batch);
            exemplars_assigned += batch_exemplars.len();
            let part = build_part_from_batch(
                store,
                &semaphore,
                bucket,
                config,
                &ingest_bounds,
                input_set_hash,
                &input_set_hash16,
                part_index,
                batch,
                batch_exemplars,
            )
            .await?;
            parts.push(part);
            part_index += 1;
            batch_start = i + 1;
            batch_bytes = 0;
        }
    }

    if batch_start < builds.len() {
        let batch = &builds[batch_start..];
        let batch_exemplars = take_batch_exemplars(&mut exemplars_by_series, batch);
        exemplars_assigned += batch_exemplars.len();
        let part = build_part_from_batch(
            store,
            &semaphore,
            bucket,
            config,
            &ingest_bounds,
            input_set_hash,
            &input_set_hash16,
            part_index,
            batch,
            batch_exemplars,
        )
        .await?;
        parts.push(part);
    }

    // Exemplar conservation, the ADR-0018 overlap-harmlessness rule applied to
    // this signal: every input exemplar reaches exactly one part. Anything left
    // in the map names a series no part carries, which would make the L1 output
    // less than the multiset of its inputs, so the run fails here rather than
    // publishing a lossy merge. `publish.rs`'s own conservation gate counts
    // samples (the only count the commit record carries), so it cannot see
    // this; the check belongs where the assignment happens.
    if exemplars_assigned != exemplar_total {
        let orphaned: Vec<String> = exemplars_by_series
            .keys()
            .map(|id| hex::encode(&id[..8]))
            .collect();
        return Err(MaintainError::Invariant(format!(
            "exemplar conservation violated: {exemplar_total} input exemplars, \
             {exemplars_assigned} assigned to parts; series with unassigned \
             exemplars: {}",
            orphaned.join(",")
        )));
    }

    Ok(parts)
}

/// Take the exemplars belonging to one batch's series out of the by-series map,
/// in the batch's own series order (ascending series id, the same order the
/// output's SERIES_IDS takes) and, within a series, in the order the inputs
/// carried them.
///
/// Removing rather than copying is what lets [`build_parts`] prove conservation:
/// a series is in exactly one batch, so after the last batch the map must be
/// empty. The output writer re-resolves each record's `series_index` against
/// its own SERIES_IDS and stable-sorts by `(series_index, ts_ns)`, so this
/// ordering is what breaks ties between two records sharing a key, and the
/// encoded section stays a function of the canonical input order alone.
fn take_batch_exemplars(
    by_series: &mut BTreeMap<[u8; 16], Vec<ExemplarInput>>,
    batch: &[SeriesBuild<'_>],
) -> Vec<ExemplarInput> {
    let mut out = Vec::new();
    for build in batch {
        if let Some(mut records) = by_series.remove(&build.series_id.0) {
            out.append(&mut records);
        }
    }
    out
}

/// One output series' merge plan without its page bytes: identity, borrowed
/// labels, and its ordered runs (each tagged with the input object to fetch
/// from), plus the total page bytes those runs will contribute to a part.
struct SeriesBuild<'a> {
    series_id: SeriesId,
    labels: &'a LabelSet,
    runs: Vec<(&'a str, &'a RunPlan)>,
    page_bytes: u64,
}

/// Fetch every page of one batch of series (coalesced per object, concurrent
/// under `semaphore`), materialize the runs in deterministic order, and build
/// the part. The fetch/materialize split keeps the buffered page bytes bounded
/// to this one part's worth and makes emission independent of GET completion
/// order.
#[allow(clippy::too_many_arguments)]
async fn build_part_from_batch(
    store: &dyn ObjectStoreBackend,
    semaphore: &Semaphore,
    bucket: &Bucket,
    config: &CompactorConfig,
    ingest_bounds: &IngestBounds,
    input_set_hash: &[u8; 32],
    input_set_hash16: &str,
    part_index: u32,
    builds: &[SeriesBuild<'_>],
    exemplars: Vec<ExemplarInput>,
) -> Result<BuiltPart> {
    let regions = fetch_batch_pages(store, semaphore, builds).await?;
    let batch = materialize_batch(builds, &regions)?;
    let part = flush_part(
        bucket,
        config,
        ingest_bounds,
        input_set_hash,
        input_set_hash16,
        part_index,
        batch,
        exemplars,
    )?;
    if !config.dry_run {
        put_part(store, &part).await?;
    }
    Ok(part)
}

/// The fetched, coalesced byte regions of one batch, keyed by input object
/// key. Each object's value is its list of `(absolute_start, bytes)` merged
/// GET results, from which any planned page range is later sliced zero-copy.
type BatchRegions<'a> = HashMap<&'a str, Vec<(u64, Bytes)>>;

/// Coalesce every run's TS and VAL-or-HIST byte range in the batch per input
/// object, then issue the merged GETs concurrently (bounded by `semaphore`,
/// windowed by `buffer_unordered`). Results are collected before returning, so
/// the caller materializes pages in a fixed order regardless of which GET
/// finished first.
async fn fetch_batch_pages<'a, 'f>(
    store: &'f dyn ObjectStoreBackend,
    semaphore: &'f Semaphore,
    builds: &[SeriesBuild<'a>],
) -> Result<BatchRegions<'a>>
where
    'a: 'f,
{
    // Every needed absolute half-open range, grouped by object.
    let mut ranges_by_object: BTreeMap<&'a str, Vec<(u64, u64)>> = BTreeMap::new();
    for build in builds {
        for (key, run) in &build.runs {
            let entry = ranges_by_object.entry(key).or_default();
            entry.push((run.ts_abs.0, run.ts_abs.0.saturating_add(run.ts_abs.1)));
            entry.push((
                run.page_abs.0,
                run.page_abs.0.saturating_add(run.page_abs.1),
            ));
        }
    }

    // Coalesce each object's ranges into the actual GET set.
    let mut gets: Vec<(&'a str, u64, u64)> = Vec::new();
    for (key, ranges) in ranges_by_object {
        for (start, end) in coalesce_ranges(ranges, COALESCE_GAP) {
            gets.push((key, start, end));
        }
    }

    // Box each GET future with an explicit `+ Send` bound before handing it to
    // `buffer_unordered`. A bare `async`/combinator chain that borrows the
    // `&dyn ObjectStoreBackend` trait object makes rustc infer a late-bound
    // lifetime whose `Send` it then cannot prove is general enough, which
    // surfaces as a "Send is not general enough" error where the maintain loop
    // is `tokio::spawn`ed (services/ravel-server). Naming the boxed future's
    // `Send` bound with an early-bound lifetime is the standard workaround.
    type GetFuture<'a, 'f> =
        Pin<Box<dyn Future<Output = Result<(&'a str, u64, Bytes)>> + Send + 'f>>;
    let futures: Vec<GetFuture<'a, 'f>> = gets
        .into_iter()
        .map(|(key, start, end)| {
            Box::pin(fetch_one_range(store, semaphore, key, start, end)) as GetFuture<'a, 'f>
        })
        .collect();
    let fetched: Vec<(&'a str, u64, Bytes)> = stream_iter(futures)
        .buffer_unordered(FETCH_CONCURRENCY)
        .try_collect()
        .await?;

    let mut regions: BatchRegions<'a> = HashMap::new();
    for (key, start, data) in fetched {
        regions.entry(key).or_default().push((start, data));
    }
    Ok(regions)
}

/// One coalesced ranged GET, gated on a shared concurrency permit. Named (not
/// an inline `async` block) so its future is `Send`-general over `'a`; see the
/// call site in [`fetch_batch_pages`].
async fn fetch_one_range<'a>(
    store: &dyn ObjectStoreBackend,
    semaphore: &Semaphore,
    key: &'a str,
    start: u64,
    end: u64,
) -> Result<(&'a str, u64, Bytes)> {
    let _permit = semaphore
        .acquire()
        .await
        .map_err(|_| MaintainError::Invariant("page-fetch semaphore closed".into()))?;
    let got = store.get(key, GetRange::Range(start, end)).await?;
    Ok((key, start, got.data))
}

/// Slice each run's TS and VAL-or-HIST page out of the fetched regions in the
/// batch's series-then-run order and pack them into [`SeriesInputV4`], stamping
/// each run's provenance from its commit record (plan §3.3 step 3). The page
/// bytes are sliced zero-copy from the coalesced GET buffers; the single
/// `to_vec` per page here is the copy `RunInputV4`'s owned `Vec<u8>` fields
/// require, not a redundant copy off the wire.
fn materialize_batch(
    builds: &[SeriesBuild<'_>],
    regions: &BatchRegions<'_>,
) -> Result<Vec<SeriesInputV4>> {
    let mut batch = Vec::with_capacity(builds.len());
    for build in builds {
        let mut runs = Vec::with_capacity(build.runs.len());
        for (key, run) in &build.runs {
            let object = regions.get(key).ok_or_else(|| {
                MaintainError::Invariant(format!("no fetched region for object {key}"))
            })?;
            let ts_page = slice_region(object, run.ts_abs).ok_or_else(|| {
                MaintainError::Invariant("coalesced fetch missing a TS page range".into())
            })?;
            let page = slice_region(object, run.page_abs).ok_or_else(|| {
                MaintainError::Invariant("coalesced fetch missing a value page range".into())
            })?;
            let value_page = match run.kind {
                ValueKind::Scalar => RunValuePageV4::Scalar(page.to_vec()),
                ValueKind::Histogram => RunValuePageV4::Histogram(page.to_vec()),
            };
            runs.push(RunInputV4 {
                created_unix_ns: run.created_unix_ns,
                writer_epoch: run.writer_epoch,
                writer_seq: run.writer_seq,
                min_ts_ns: run.min_ts_ns,
                max_ts_ns: run.max_ts_ns,
                sample_count: run.sample_count,
                ts_page: ts_page.to_vec(),
                value_page,
            });
        }
        batch.push(SeriesInputV4 {
            series_id: build.series_id,
            labels: build.labels.clone(),
            runs,
        });
    }
    Ok(batch)
}

/// Merge `(start, end)` half-open ranges into ordered, non-overlapping groups,
/// joining two consecutive ranges whose gap is at most `max_gap` into one
/// (the same shape as `ravel-query`'s `coalesce_ranges`; reimplemented locally
/// to avoid a `ravel-maintain` -> `ravel-query` dependency).
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

/// Zero-copy slice of one absolute page `(offset, len)` out of an object's
/// coalesced GET buffers. Coalescing guarantees each planned range falls fully
/// within exactly one merged buffer, so a linear scan finds it; the returned
/// `Bytes` shares the buffer's allocation (no copy). `None` means no buffer
/// covered the range, which is an internal invariant break.
fn slice_region(buffers: &[(u64, Bytes)], range: (u64, u64)) -> Option<Bytes> {
    let (offset, len) = range;
    let end = offset.checked_add(len)?;
    buffers.iter().find_map(|(start, buf)| {
        if *start <= offset && end <= start.saturating_add(buf.len() as u64) {
            let start_rel = usize::try_from(offset - start).ok()?;
            let end_rel = usize::try_from(end - start).ok()?;
            Some(buf.slice(start_rel..end_rel))
        } else {
            None
        }
    })
}

fn merged_ingest_bounds(inputs: &[InputRecord]) -> IngestBounds {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for i in inputs {
        min = min.min(i.record.min_ingest_ts_ns);
        max = max.max(i.record.max_ingest_ts_ns);
    }
    if inputs.is_empty() {
        min = 0;
        max = 0;
    }
    IngestBounds {
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
    }
}

#[allow(clippy::too_many_arguments)]
fn flush_part(
    bucket: &Bucket,
    config: &CompactorConfig,
    ingest_bounds: &IngestBounds,
    input_set_hash: &[u8; 32],
    input_set_hash16: &str,
    part_index: u32,
    batch: Vec<SeriesInputV4>,
    exemplars: Vec<ExemplarInput>,
) -> Result<BuiltPart> {
    let run_count: u64 = batch.iter().map(|s| s.runs.len() as u64).sum();
    let first_series_id = batch.iter().map(|s| s.series_id).min();
    let last_series_id = batch.iter().map(|s| s.series_id).max();

    let identity = SegmentIdentity {
        tenant_hash: bucket.tenant_hash.0,
        shard: bucket.shard,
        writer_id: config.compactor_writer_id.to_string(),
        writer_epoch: 0,
        writer_seq: 0,
    };
    let meta = CompactionMetaV4 {
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        input_set_hash: *input_set_hash,
        part_index,
        level: 1,
    };
    let ingest = IngestBounds {
        min_ingest_ts_ns: ingest_bounds.min_ingest_ts_ns,
        max_ingest_ts_ns: ingest_bounds.max_ingest_ts_ns,
    };
    // Exemplars ride along verbatim; the writer resolves each record's
    // `series_index` against this part's own sorted SERIES_IDS, which is the
    // only field the copy changes (ADR-0047 decision 3). An exemplar naming a
    // series this part does not carry is a writer error, not a silent drop, so
    // a mis-assignment above fails the run instead of shrinking the output.
    let written = SegmentWriter::write_v5_with_exemplars(batch, identity, ingest, meta, exemplars)?;
    let content_hash = written.summary.blake3;
    let hash16 = hex::encode(&content_hash[..8]);
    let key = keys::l1_part_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
        input_set_hash16,
        part_index,
        &hash16,
    )?;

    let part = CompactionPart {
        part_index,
        first_series_id: first_series_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        last_series_id: last_series_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        content_hash: content_hash.to_vec(),
        object_size: written.bytes.len() as u64,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        run_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: OUTPUT_FORMAT_VERSION,
    };
    Ok(BuiltPart {
        key,
        bytes: written.bytes,
        part,
    })
}

/// PUT one part `CreateIfAbsent`; `AlreadyExists` is idempotent success (the
/// key embeds the content hash, so the stored bytes are identical by
/// construction, plan §3.4 point 1).
pub async fn put_part(store: &dyn ObjectStoreBackend, part: &BuiltPart) -> Result<()> {
    use ravel_object_store::{PutOptions, StoreError, UploadChecksum};
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(&part.bytes));
    match store
        .put(
            &part.key,
            part.bytes.clone(),
            PutOptions::create_if_absent().with_checksum(checksum),
        )
        .await
    {
        Ok(_) | Err(StoreError::AlreadyExists) => Ok(()),
        Err(e) => Err(MaintainError::Store(e)),
    }
}
