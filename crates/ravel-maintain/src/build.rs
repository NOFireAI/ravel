//! Whole-bucket catalog group-by and run-merged rewrite into RSEG v7 parts.
//! Every input catalog's series is grouped into one `BTreeMap` keyed by series
//! id (all inputs' per-series metadata resident at once), then iterated in id
//! order; this is a group-by-then-iterate, not a bounded-window k-way merge.
//!
//! Since ADR-0092 decision 1, L1 compaction is a rewrite, not a re-layout. Per
//! series, every contributing run's pages are decoded, the samples are merged in
//! timestamp order, and the whole series is re-encoded into a SINGLE run
//! (`RunInputV7`) carrying each sample's dedup key (`created_unix_ns`,
//! `writer_epoch`, `writer_seq`, original in-page index) in the run-major
//! per-sample provenance columns, so a query over the merged part answers every
//! request identically to a query over the pre-compaction inputs
//! (`crates/ravel-query/tests/differential_compaction.rs`). A series that
//! appears in only one input already is one run: it is copied verbatim with no
//! provenance column (byte-identical to the L0 shape), which costs nothing.
//!
//! Parts are split on ENCODED OUTPUT bytes (ADR-0092 decision 3), not on input
//! catalog byte ranges: after run merging the output size is a function of the
//! data's shape through per-page codec selection, so the input-byte figure no
//! longer predicts it. The output figure is every section that grows with what
//! the part carries -- pages, SERIES_IDS, SERIES_META (including the per-sample
//! provenance columns), LABEL_DICT, EXEMPLARS -- so what the split rule counts
//! is the object it is about to write; see `PartSizeEstimate`. Input page bytes
//! are retained only as a fetch-buffer bound (the window below). A series' runs never straddle a part and part
//! id-ranges are disjoint, since series are emitted in ascending id order and a
//! part is a contiguous id range. Each finished part is a whole object written
//! with a single `CreateIfAbsent` PUT (no multipart), and its encoded bytes are
//! retained in the returned `Vec` until publish so the convergence-repair path
//! can re-PUT a part a racing winner is missing.
//!
//! Format-migration note (ADR-0066 decision 5): an input recorded *below* the
//! current output version (its commit record's `segment_format_version` is older
//! than [`OUTPUT_FORMAT_VERSION`]) cannot be copied verbatim, so its object key
//! is passed in `migrate_keys`. Under the run-merged rewrite this is largely
//! moot: a multi-run series is decoded and re-encoded regardless of version, and
//! a single-run series whose input is a migrate key takes the
//! decode-and-re-encode path ([`reencode_run_to_current_version`]) instead of
//! the verbatim copy. Under RSEG v7's single-version window `migrate_keys` is
//! empty in practice (a below-v7 input is unreadable), so the distinction only
//! affects the single-run verbatim fast path.
//!
//! Page fetch mechanics: pages are fetched in windows, not one blocking GET per
//! page. A fetch window is a run of consecutive series whose INPUT page bytes
//! stay under `max_l1_part_bytes` (the fetch-buffer bound). For each window the
//! per-run TS and VAL-or-HIST byte ranges are grouped by input object and
//! coalesced (adjacent/near ranges merged into one GET, mirroring
//! `ravel-query`'s `SegmentFetcher::coalesce_ranges`), then the coalesced GETs
//! run concurrently under a bounded semaphore + a `buffer_unordered` window, so
//! a bucket's page fetch collapses from k RTT toward ceil(k/parallelism) RTT on
//! real object storage. Each window's series are materialized (decoded, merged,
//! re-encoded) in ascending id order and the window's fetched buffers are then
//! dropped, so peak fetch buffering is one window's worth. A part accumulates
//! re-encoded series across windows until its estimated STORED bytes reach
//! `max_l1_part_bytes`, checked between series; the encoded parts held until
//! publish, plus one series' decoded samples at a time, dominate peak memory.
//! Neither of those two terms is sized by a config knob: the retained parts grow
//! with the bucket's output, and one series' decoded samples grow with that
//! series. `l1_part_memory_target_bytes` governs the RLOG and RSPAN merges and
//! is not read on this path.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt, iter as stream_iter};
use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::CompactionPart;
use ravel_segment::{
    CompactionMetaV4, ExemplarInput, IngestBounds, ReaderLimits, RunEntry, RunInputV4, RunInputV7,
    RunValuePageV4, SampleProvenance, SegmentIdentity, SegmentWriter, SeriesInputV7, SeriesValues,
    ValueKind, decode_run_histogram_pages, decode_run_pages_soa, encode_run_v4,
};
use ravel_types::{LabelSet, Sample, SeriesId};
use tokio::sync::Semaphore;

use crate::bucket::Bucket;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read::{InputCatalog, InputRecord, RunPlan, SeriesPlan};

/// Maximum concurrent page GETs in flight across a whole `build_parts` call.
/// A single global cap (one shared [`Semaphore`]) driving
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

/// The current RSEG output version: the newest version RSEG's single-sourced
/// supported-version window writes (ADR-0066 decision 1, slice A). Recorded in
/// each part's `CompactionPart.segment_format_version`. Read from
/// `ravel_segment::SUPPORTED_VERSIONS.newest()` rather than a mirrored literal
/// so a future format bump moves the writer, the reader gate, `audit-versions`,
/// and this compactor constant together (the sixteen-hand-edited-sites hazard
/// ADR-0049 measured). Today the window is `single(VERSION_V7)`, so this is 7.
pub const OUTPUT_FORMAT_VERSION: u32 = ravel_segment::SUPPORTED_VERSIONS.newest() as u32;

/// One built (not yet published) L1 part: its content-addressed key, its
/// bytes, and the [`CompactionPart`] describing it for the record.
///
/// `bytes` is `Option<Bytes>` (ADR-0979 decision 3): a caller that retains the
/// encoded bytes for its own publish path (the erasure rewrite, and for now the
/// RSEG/RSPAN codecs) carries `Some`; the bounded RLOG compaction path releases
/// them the moment the part's PUT succeeds and carries `None`, so the
/// retained-parts memory term goes to zero instead of scaling with the bucket's
/// whole L1 output. A `None`-bytes part cannot be re-PUT by the convergence
/// repair path, which is sound on the compaction path because every part is PUT
/// (content-addressed) before the record is; see
/// [`crate::publish::resolve_already_exists`].
///
/// `put_already_existed` records that this part's `CreateIfAbsent` PUT answered
/// `AlreadyExists` (the content-addressed key was already present from an
/// abandoned run). Such a part's stored `last_modified` is the abandoned run's,
/// so it can be inside the unreferenced-part sweep's age gate and a racing
/// tenant tombstone can delete it between our PUT and our record PUT. The
/// bounded compaction path drops its bytes anyway (retaining them for every
/// `AlreadyExists` part would recreate the whole-output term D3 exists to kill,
/// in exactly the abandoned-run-retry case) and instead HEAD-verifies these
/// parts after the record PUT succeeds
/// ([`crate::publish::verify_already_existed_parts`]). The flag is only set on
/// the bounded RLOG compaction path; every other constructor leaves it `false`.
#[derive(Debug, Clone)]
pub struct BuiltPart {
    pub key: String,
    pub bytes: Option<Bytes>,
    pub part: CompactionPart,
    pub put_already_existed: bool,
}

/// The outcome of a part's `CreateIfAbsent` PUT: whether it created the object
/// or found the content-addressed key already present (an abandoned run's
/// byte-identical part). [`put_part`] returns it so the bounded compaction path
/// can flag an `AlreadyExists` part for post-publish HEAD verification (ADR-0979
/// decision 3); most callers discard it (they retain bytes and repair from
/// them, so the distinction does not change their behaviour).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartPut {
    Created,
    AlreadyExisted,
}

/// Merge all inputs into size-capped run-merged RSEG v7 parts. `catalogs` MUST
/// be aligned with `inputs` (both in canonical input order): the alignment is
/// what makes run tie-breaking by canonical input position deterministic, which
/// in turn fixes each merged sample's provenance in-page index.
///
/// `catalogs` is taken by value, and this call is their last use, so the
/// exemplar records are moved out of them (`std::mem::take` below) rather than
/// cloned: the retained records exist in exactly one place at a time, which is
/// what keeps them inside `read.rs`'s stated catalog-metadata memory bound.
/// Only the borrowed page-range metadata (`&SeriesPlan`,
/// `object_key`) is read after the take.
///
/// `migrate_keys` holds the object keys of any input recorded below the current
/// output version (ADR-0066 decision 5): a run whose input key is in this set is
/// decoded and re-encoded at the current version rather than page-copied
/// verbatim (see the module doc and [`materialize_series`]). It is empty for
/// ordinary compaction, where every input is already at the current version, and
/// the single-run verbatim fast path then runs unchanged.
pub async fn build_parts(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    inputs: &[InputRecord],
    mut catalogs: Vec<InputCatalog>,
    migrate_keys: &HashSet<String>,
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
    // set live at once. Grouping is only how a part collects the
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
    // input order, which is the run tie-break rule.
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

    // One shared cap for every window's fetches (windows run sequentially, so
    // this bounds total in-flight GETs at `FETCH_CONCURRENCY`).
    let semaphore = Semaphore::new(FETCH_CONCURRENCY);
    let limits = ReaderLimits::default();

    let mut parts = Vec::new();
    let mut part_index: u32 = 0;

    // The rolling output part: series already decoded, merged, and re-encoded
    // whose accumulated ENCODED STORED bytes have not yet reached
    // `max_l1_part_bytes`. Part boundaries are chosen on output bytes (ADR-0092
    // decision 3), which after run merging no longer track input bytes, so a
    // part may span several fetch windows and a window may finish several parts.
    //
    // On this RSEG path `max_l1_part_bytes` is used for exactly what its name
    // (the stored-size target, issue #872) means: the bytes of the object about
    // to be written. `PartSizeEstimate` charges every section that grows with
    // the data, not the pages alone.
    //
    // `l1_part_memory_target_bytes` is not read here at all, so no configured
    // number sizes this builder's heap. What it holds at once is one fetch
    // window's raw pages (the stored-size target applied to INPUT page bytes),
    // one series' decoded samples while `materialize_series` decodes, merges,
    // and re-encodes its runs (bounded by that series' own size and by nothing
    // configurable), and every finished part's encoded bytes, which are retained
    // until publish so convergence repair can re-PUT one. The pending output
    // part is the only term `max_l1_part_bytes` sizes, and it is checked between
    // series, so that term runs one whole series past the target.
    let mut pending: Vec<SeriesInputV7> = Vec::new();
    let mut pending_exemplars: Vec<ExemplarInput> = Vec::new();
    let mut pending_estimate = PartSizeEstimate::new();
    let mut exemplars_assigned = 0usize;

    // Accumulate consecutive series into a fetch window until their INPUT page
    // bytes reach the cap (the fetch-buffer bound; the last series closes the
    // final window). Fetch that window's pages (coalesced + concurrent),
    // materialize each series in ascending id order into a run-merged
    // `SeriesInputV7`, and flush a part whenever the pending part's stored-size
    // estimate reaches the target. A single series whose own stored bytes exceed
    // it becomes its own part. Series stay in ascending id order throughout, so every part is a
    // contiguous, disjoint id range.
    let mut window_start = 0usize;
    let mut window_input_bytes: u64 = 0;
    for i in 0..builds.len() {
        window_input_bytes = window_input_bytes.saturating_add(builds[i].page_bytes);
        let last = i + 1 == builds.len();
        if window_input_bytes < config.max_l1_part_bytes && !last {
            continue;
        }
        let window = &builds[window_start..=i];
        let regions = fetch_batch_pages(store, &semaphore, window).await?;
        for build in window {
            let series_v7 = materialize_series(build, &regions, migrate_keys, limits)?;
            pending_estimate.push_series(&series_v7);
            pending.push(series_v7);
            if let Some(mut records) = exemplars_by_series.remove(&build.series_id.0) {
                exemplars_assigned += records.len();
                pending_estimate.push_exemplars(&records);
                pending_exemplars.append(&mut records);
            }
            if pending_estimate.bytes() >= config.max_l1_part_bytes {
                let part = flush_part(
                    bucket,
                    config,
                    &ingest_bounds,
                    input_set_hash,
                    &input_set_hash16,
                    part_index,
                    std::mem::take(&mut pending),
                    std::mem::take(&mut pending_exemplars),
                )?;
                if !config.dry_run {
                    put_part(store, &part).await?;
                }
                parts.push(part);
                part_index += 1;
                pending_estimate.reset();
            }
        }
        window_start = i + 1;
        window_input_bytes = 0;
    }

    // The tail part: whatever series remain below the stored-size target.
    if !pending.is_empty() {
        let part = flush_part(
            bucket,
            config,
            &ingest_bounds,
            input_set_hash,
            &input_set_hash16,
            part_index,
            pending,
            pending_exemplars,
        )?;
        if !config.dry_run {
            put_part(store, &part).await?;
        }
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

/// One output series' merge plan without its page bytes: identity, borrowed
/// labels, and its ordered runs (each tagged with the input object to fetch
/// from), plus the total INPUT page bytes those runs contribute to a fetch
/// window (the fetch-buffer bound; part sizing is on the output's stored-size
/// estimate).
struct SeriesBuild<'a> {
    series_id: SeriesId,
    labels: &'a LabelSet,
    runs: Vec<(&'a str, &'a RunPlan)>,
    page_bytes: u64,
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

/// Decode, merge, and re-encode one series into a single run-merged
/// [`SeriesInputV7`]. What it contributes to the part's stored-size estimate
/// (the figure `build_parts` splits parts on, ADR-0092 decision 3) is charged
/// by [`PartSizeEstimate::push_series`] from the returned value.
///
/// A series with exactly one contributing run is already one run: it is copied
/// verbatim (no per-sample provenance column, so the merged object stays
/// byte-identical to the L0 shape for that series), unless its input is a
/// migrate key, in which case it is decoded and re-encoded at the current
/// version. A series with two or more runs is fully decoded across every input,
/// its samples merged in timestamp order, and re-encoded into one run carrying
/// each sample's dedup key -- `(created_unix_ns, writer_epoch, writer_seq,
/// original in-page index)` -- in the per-sample provenance column, so a query
/// over the merged run reproduces the same candidate multiset with the same
/// priorities the unmerged runs produced (ADR-0092 decision 2).
fn materialize_series(
    build: &SeriesBuild<'_>,
    regions: &BatchRegions<'_>,
    migrate_keys: &HashSet<String>,
    limits: ReaderLimits,
) -> Result<SeriesInputV7> {
    let series_id = build.series_id;
    let kind = match build.runs.first() {
        Some((_, run)) => run.kind,
        // A series with no runs cannot reach here: `build_parts` only builds a
        // `SeriesBuild` from a non-empty contribution list.
        None => {
            return Err(MaintainError::Invariant(format!(
                "series {} has no runs to materialize",
                series_id.to_hex()
            )));
        }
    };

    // Single-run fast path: no decode/merge, no provenance column.
    if build.runs.len() == 1 {
        let (key, run) = build.runs[0];
        let object = regions.get(key).ok_or_else(|| {
            MaintainError::Invariant(format!("no fetched region for object {key}"))
        })?;
        let ts_page = slice_region(object, run.ts_abs).ok_or_else(|| {
            MaintainError::Invariant("coalesced fetch missing a TS page range".into())
        })?;
        let page = slice_region(object, run.page_abs).ok_or_else(|| {
            MaintainError::Invariant("coalesced fetch missing a value page range".into())
        })?;
        let run_v4 = if migrate_keys.contains(key) {
            reencode_run_to_current_version(
                &series_id,
                run,
                ts_page.as_ref(),
                page.as_ref(),
                limits,
            )?
        } else {
            let value_page = match run.kind {
                ValueKind::Scalar => RunValuePageV4::Scalar(page.to_vec()),
                ValueKind::Histogram => RunValuePageV4::Histogram(page.to_vec()),
            };
            RunInputV4 {
                created_unix_ns: run.created_unix_ns,
                writer_epoch: run.writer_epoch,
                writer_seq: run.writer_seq,
                min_ts_ns: run.min_ts_ns,
                max_ts_ns: run.max_ts_ns,
                sample_count: run.sample_count,
                ts_page: ts_page.to_vec(),
                value_page,
            }
        };
        return Ok(SeriesInputV7 {
            series_id,
            labels: build.labels.clone(),
            runs: vec![RunInputV7 {
                run: run_v4,
                provenance: None,
            }],
        });
    }

    // Multi-run merge: decode every run, tagging each sample with its run's
    // dedup key and its original in-page index, merge in timestamp order (stable
    // by insertion order for equal timestamps, which is canonical input order
    // then run order then in-page order -- deterministic), and re-encode into
    // one run carrying the per-sample provenance column.
    let (run_v4, provenance) = match kind {
        ValueKind::Scalar => merge_scalar_runs(&series_id, build, regions, limits)?,
        ValueKind::Histogram => merge_histogram_runs(&series_id, build, regions, limits)?,
    };
    Ok(SeriesInputV7 {
        series_id,
        labels: build.labels.clone(),
        runs: vec![RunInputV7 {
            run: run_v4,
            provenance: Some(provenance),
        }],
    })
}

/// The framed TS and VAL-or-HIST page bytes one run contributes to a part: the
/// page sections' share of the stored object, and the counterpart of the input
/// `page_bytes` figure a fetch window is bounded by. Each page's `enc`/`comp`/
/// `crc32c` header is already inside these buffers, which hold the page exactly
/// as it lands in the object. The catalog, dictionary, provenance and exemplar
/// sections are charged separately by [`PartSizeEstimate`].
fn run_page_bytes(run: &RunInputV4) -> u64 {
    let value_len = match &run.value_page {
        RunValuePageV4::Scalar(p) => p.len(),
        RunValuePageV4::Histogram(p) => p.len(),
    };
    run.ts_page.len() as u64 + value_len as u64
}

/// Fixed encoded cost every part pays before it carries anything: the
/// FooterProto (identity, event and ingest bounds, compaction provenance, and
/// one `Section` entry per section), the 16-byte trailer, and the zero padding
/// the writer inserts to 8-byte-align VAL_PAGES (docs/segment-format.md). A
/// deliberately flat figure at the top of that range, in the same spirit as the
/// RLOG merge's `STORED_RECORD_FIXED_BYTES`: it does not grow with what the
/// part carries, so it needs no per-item term.
const STORED_PART_FIXED_BYTES: u64 = 512;

/// Fixed encoded cost one series adds beyond its label strings and its runs:
/// its 16-byte SERIES_IDS entry (never zstd-compressed, BLAKE3 ids being
/// incompressible), its series-major SERIES_META cells (`schema_ref`, the
/// schema's value ordinals, `value_kind`, `run_count`), and, at or above the
/// sparse-emission threshold, its SERIES_IDX entry.
const STORED_SERIES_FIXED_BYTES: u64 = 48;

/// Fixed encoded cost one run adds: the twelve run-major SERIES_META varints
/// (blocks 5-16 of docs/segment-format.md), every one of them a delta, a count
/// or a page length. A flat figure at the top of the range those varints
/// occupy.
const STORED_RUN_FIXED_BYTES: u64 = 24;

/// Encoded cost per sample of a run that carries the optional per-sample dedup
/// provenance columns (blocks 17-21). Charged only for a run-merged run: a
/// single-run series copies through with `provenance: None` and the columns
/// cost it nothing, which is the format's canonical "no provenance" form.
///
/// A flat figure at the top of the range those four codec-encoded columns cost
/// per sample before the section's zstd. `prov_created_delta`, `prov_epoch` and
/// `prov_seq` are constant across all the samples one source write
/// contributed, so `ravel_codec::encode_i64` picks a run-length or delta form
/// and they cost well under a byte per sample; `prov_in_page_index` walks the
/// source pages' positions, so it delta-encodes to about one.
const STORED_PROVENANCE_SAMPLE_BYTES: u64 = 4;

/// Fixed encoded cost of one LABEL_DICT entry beyond the string's own bytes:
/// its length varint.
const STORED_DICT_ENTRY_BYTES: u64 = 2;

/// Fixed encoded cost of one EXEMPLARS record beyond its attribute ordinals:
/// the `series_index` and `ts_delta` varints, the 8-byte value, the 16-byte
/// trace id, the 8-byte span id, and `attr_count`.
const STORED_EXEMPLAR_FIXED_BYTES: u64 = 40;

/// Encoded cost of one exemplar attribute pair: its two LABEL_DICT ordinal
/// varints. The strings themselves intern into the object's LABEL_DICT and are
/// charged there, once per part, exactly as a series label is.
const STORED_EXEMPLAR_ATTR_BYTES: u64 = 4;

/// The estimated stored size of the in-progress part, the figure compared
/// against [`CompactorConfig::max_l1_part_bytes`].
///
/// The knob is named for on-object bytes, so this counts every RSEG section
/// that grows with what the part carries: the TS/VAL/HIST pages
/// ([`run_page_bytes`]), each series' SERIES_IDS entry and series-major
/// SERIES_META cells, each run's run-major cells, the per-sample provenance
/// columns a run-merged run adds, each distinct LABEL_DICT string, and the
/// EXEMPLARS records. Charging the pages alone left the provenance columns,
/// the dictionary and the exemplars out, and all three grow with the data, so
/// the stored object could pass the target while the estimate stayed under it
/// and the excess accumulated from one series to the next.
///
/// Like the RLOG estimate ([`crate::rlog::estimate_stored_record`]) this is a
/// pre-compression proxy: LABEL_DICT and SERIES_META are zstd-compressed
/// sections, so what is charged for them is an upper bound on what they store,
/// which is the conservative direction for a geometry knob. And like it, the
/// estimate only decides where parts split, never correctness: the writer
/// produces the same bytes for a given series set however the series were
/// partitioned across parts.
struct PartSizeEstimate {
    bytes: u64,
    /// LABEL_DICT interns each distinct string once per object, so a string is
    /// charged once per part rather than once per series or exemplar that
    /// names it. A part that closes and reopens on the same label pays for it
    /// in both, because both objects carry the entry.
    charged_dict: HashSet<String>,
}

impl PartSizeEstimate {
    fn new() -> Self {
        Self {
            bytes: STORED_PART_FIXED_BYTES,
            charged_dict: HashSet::new(),
        }
    }

    /// Start the next part: the fixed footer cost again, and an empty
    /// dictionary, since the new object interns its own strings from scratch.
    fn reset(&mut self) {
        self.bytes = STORED_PART_FIXED_BYTES;
        self.charged_dict.clear();
    }

    fn bytes(&self) -> u64 {
        self.bytes
    }

    fn charge_dict_string(&mut self, s: &str) {
        if !self.charged_dict.contains(s) {
            self.charged_dict.insert(s.to_string());
            self.bytes = self
                .bytes
                .saturating_add(s.len() as u64)
                .saturating_add(STORED_DICT_ENTRY_BYTES);
        }
    }

    fn push_series(&mut self, series: &SeriesInputV7) {
        self.bytes = self.bytes.saturating_add(STORED_SERIES_FIXED_BYTES);
        for label in series.labels.iter() {
            self.charge_dict_string(&label.name);
            self.charge_dict_string(&label.value);
        }
        for run in &series.runs {
            self.bytes = self
                .bytes
                .saturating_add(STORED_RUN_FIXED_BYTES)
                .saturating_add(run_page_bytes(&run.run));
            if let Some(provenance) = &run.provenance {
                self.bytes = self.bytes.saturating_add(
                    (provenance.len() as u64).saturating_mul(STORED_PROVENANCE_SAMPLE_BYTES),
                );
            }
        }
    }

    fn push_exemplars(&mut self, records: &[ExemplarInput]) {
        for record in records {
            self.bytes = self
                .bytes
                .saturating_add(STORED_EXEMPLAR_FIXED_BYTES)
                .saturating_add(
                    (record.attrs.len() as u64).saturating_mul(STORED_EXEMPLAR_ATTR_BYTES),
                );
            for (name, value) in &record.attrs {
                self.charge_dict_string(name);
                self.charge_dict_string(value);
            }
        }
    }
}

/// A sample's full ADR-0010 §5 dedup key, the ordering key within one
/// timestamp: `(created_unix_ns, writer_epoch, writer_seq, in_page_index)`.
fn provenance_key(p: &SampleProvenance) -> (i64, u64, u64, u32) {
    (
        p.created_unix_ns,
        p.writer_epoch,
        p.writer_seq,
        p.in_page_index,
    )
}

/// Order a merged run's scalar samples by `(ts_ns, dedup key, value bits)`
/// ascending. The primary key keeps the run ascending-by-ts, as every RSEG page
/// must be. The secondary keys matter because a query over a run-merged run may
/// fall back to run-wide-plus-position provenance (the fourth dedup element from
/// on-disk position) as well as read the explicit per-sample column: ordering
/// same-timestamp samples by ascending dedup priority makes on-disk position a
/// monotone proxy for that priority, so the two representations pick the same
/// winner at a duplicate timestamp. The tie-break on value bits keeps equal-key
/// duplicates (a re-sent identical sample) deterministic.
fn sort_merged_scalar(merged: &mut [(Sample, SampleProvenance)]) {
    merged.sort_by(|(sa, pa), (sb, pb)| {
        sa.ts_ns
            .cmp(&sb.ts_ns)
            .then_with(|| provenance_key(pa).cmp(&provenance_key(pb)))
            .then_with(|| sa.value.to_bits().cmp(&sb.value.to_bits()))
    });
}

/// The run-wide `(created_unix_ns, writer_epoch, writer_seq)` a merged run
/// carries: the lexicographic minimum over its samples' dedup keys. A merged
/// run's dedup source is its per-sample provenance column, not this triple, so
/// the value is not query-observable; the minimum keeps the SERIES_META
/// `created_delta` column (relative to the footer base) compact and makes the
/// choice a deterministic function of the samples.
fn merged_run_prefix(provenance: &[SampleProvenance]) -> (i64, u64, u64) {
    provenance
        .iter()
        .map(|p| (p.created_unix_ns, p.writer_epoch, p.writer_seq))
        .min()
        .unwrap_or((0, 0, 0))
}

/// Decode and merge every scalar run of one multi-run series into a single
/// re-encoded run plus its per-sample provenance column.
fn merge_scalar_runs(
    series_id: &SeriesId,
    build: &SeriesBuild<'_>,
    regions: &BatchRegions<'_>,
    limits: ReaderLimits,
) -> Result<(RunInputV4, Vec<SampleProvenance>)> {
    let mut merged: Vec<(Sample, SampleProvenance)> = Vec::new();
    let mut scratch = Vec::new();
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for (key, run) in &build.runs {
        let (ts_page, page) = slice_run_pages(regions, key, run)?;
        let entry = run_entry_for_decode(run);
        timestamps.clear();
        values.clear();
        decode_run_pages_soa(
            series_id,
            &entry,
            ts_page.as_ref(),
            page.as_ref(),
            limits,
            &mut scratch,
            &mut timestamps,
            &mut values,
        )?;
        for (in_page_index, (&ts_ns, &value)) in timestamps.iter().zip(&values).enumerate() {
            merged.push((
                Sample { ts_ns, value },
                sample_provenance(run, in_page_index)?,
            ));
        }
    }
    sort_merged_scalar(&mut merged);

    let (samples, provenance): (Vec<Sample>, Vec<SampleProvenance>) = merged.into_iter().unzip();
    let (created, epoch, seq) = merged_run_prefix(&provenance);
    let run_v4 = encode_run_v4(
        series_id,
        created,
        epoch,
        seq,
        &SeriesValues::Scalar(samples),
    )?;
    Ok((run_v4, provenance))
}

/// Histogram counterpart of [`merge_scalar_runs`].
fn merge_histogram_runs(
    series_id: &SeriesId,
    build: &SeriesBuild<'_>,
    regions: &BatchRegions<'_>,
    limits: ReaderLimits,
) -> Result<(RunInputV4, Vec<SampleProvenance>)> {
    let mut merged: Vec<(ravel_segment::HistogramSample, SampleProvenance)> = Vec::new();
    for (key, run) in &build.runs {
        let (ts_page, page) = slice_run_pages(regions, key, run)?;
        let entry = run_entry_for_decode(run);
        let samples =
            decode_run_histogram_pages(series_id, &entry, ts_page.as_ref(), page.as_ref(), limits)?;
        for (in_page_index, sample) in samples.into_iter().enumerate() {
            merged.push((sample, sample_provenance(run, in_page_index)?));
        }
    }
    // Order by `(ts_ns, dedup key)` ascending, the histogram counterpart of the
    // scalar sort: primary ts keeps the run ascending, and ordering same-ts
    // samples by ascending dedup priority keeps on-disk position a monotone
    // proxy for priority (there is no bit-pattern value tie-break for a
    // histogram; the structural order is decided by the priority column).
    merged.sort_by(|(sa, pa), (sb, pb)| {
        sa.ts_ns
            .cmp(&sb.ts_ns)
            .then_with(|| provenance_key(pa).cmp(&provenance_key(pb)))
    });

    let (samples, provenance): (Vec<ravel_segment::HistogramSample>, Vec<SampleProvenance>) =
        merged.into_iter().unzip();
    let (created, epoch, seq) = merged_run_prefix(&provenance);
    let run_v4 = encode_run_v4(
        series_id,
        created,
        epoch,
        seq,
        &SeriesValues::Histogram(samples),
    )?;
    Ok((run_v4, provenance))
}

/// This sample's dedup key: its run's `(created_unix_ns, writer_epoch,
/// writer_seq)` plus the sample's original position in that run's page. The
/// fourth element is what the query engine reconstructs from array position for
/// an unmerged run (ADR-0018), made explicit here because merging destroys that
/// position.
fn sample_provenance(run: &RunPlan, in_page_index: usize) -> Result<SampleProvenance> {
    Ok(SampleProvenance {
        created_unix_ns: run.created_unix_ns,
        writer_epoch: run.writer_epoch,
        writer_seq: run.writer_seq,
        in_page_index: u32::try_from(in_page_index)
            .map_err(|_| MaintainError::Invariant("run sample index exceeds u32".into()))?,
    })
}

/// A [`RunEntry`] carrying only the fields the decode primitives read (run
/// provenance, sample count, event-time bounds); the page byte ranges are
/// supplied directly, so the `(0, 0)` section-range sentinels are correct
/// (mirrors the erasure-rewrite decode path).
fn run_entry_for_decode(run: &RunPlan) -> RunEntry {
    RunEntry {
        created_unix_ns: run.created_unix_ns,
        writer_epoch: run.writer_epoch,
        writer_seq: run.writer_seq,
        sample_count: run.sample_count,
        min_ts_ns: run.min_ts_ns,
        max_ts_ns: run.max_ts_ns,
        ts_page: (0, 0),
        val_page: (0, 0),
        hist_page: (0, 0),
    }
}

/// Slice one run's TS and VAL-or-HIST pages out of the fetched regions.
fn slice_run_pages<'a>(
    regions: &BatchRegions<'a>,
    key: &str,
    run: &RunPlan,
) -> Result<(Bytes, Bytes)> {
    let object = regions
        .get(key)
        .ok_or_else(|| MaintainError::Invariant(format!("no fetched region for object {key}")))?;
    let ts_page = slice_region(object, run.ts_abs).ok_or_else(|| {
        MaintainError::Invariant("coalesced fetch missing a TS page range".into())
    })?;
    let page = slice_region(object, run.page_abs).ok_or_else(|| {
        MaintainError::Invariant("coalesced fetch missing a value page range".into())
    })?;
    Ok((ts_page, page))
}

/// Decode one run's TS and VAL-or-HIST pages to samples and re-encode them at
/// the current output version (ADR-0066 decision 5, the RSEG decode-and-
/// re-encode rewrite primitive). This is the migration counterpart of the
/// verbatim page copy in [`materialize_batch`]: the same
/// [`ravel_segment::decode_run_pages_soa`] /
/// [`ravel_segment::decode_run_histogram_pages`] read primitives the query and
/// erasure paths use, then [`ravel_segment::encode_run_v4`] (the encode half the
/// erasure-rewrite pass already shares) to re-frame the survivors -- here, every
/// sample, since a migration drops nothing. The run's dedup-priority provenance
/// is passed through unchanged; only the on-object byte format moves forward.
fn reencode_run_to_current_version(
    series_id: &SeriesId,
    run: &RunPlan,
    ts_page: &[u8],
    value_page: &[u8],
    limits: ReaderLimits,
) -> Result<RunInputV4> {
    // `decode_run_pages_soa`/`decode_run_histogram_pages` read only the run's
    // provenance, sample count, and event-time bounds from the entry; the page
    // byte ranges are supplied directly, so the `(0, 0)` section-range sentinels
    // are correct (mirrors the erasure-rewrite decode path).
    let entry = RunEntry {
        created_unix_ns: run.created_unix_ns,
        writer_epoch: run.writer_epoch,
        writer_seq: run.writer_seq,
        sample_count: run.sample_count,
        min_ts_ns: run.min_ts_ns,
        max_ts_ns: run.max_ts_ns,
        ts_page: (0, 0),
        val_page: (0, 0),
        hist_page: (0, 0),
    };
    let values = match run.kind {
        ValueKind::Scalar => {
            let mut scratch = Vec::new();
            let mut timestamps = Vec::new();
            let mut vals = Vec::new();
            decode_run_pages_soa(
                series_id,
                &entry,
                ts_page,
                value_page,
                limits,
                &mut scratch,
                &mut timestamps,
                &mut vals,
            )?;
            let samples = timestamps
                .into_iter()
                .zip(vals)
                .map(|(ts_ns, value)| Sample { ts_ns, value })
                .collect();
            SeriesValues::Scalar(samples)
        }
        ValueKind::Histogram => {
            let samples =
                decode_run_histogram_pages(series_id, &entry, ts_page, value_page, limits)?;
            SeriesValues::Histogram(samples)
        }
    };
    Ok(encode_run_v4(
        series_id,
        run.created_unix_ns,
        run.writer_epoch,
        run.writer_seq,
        &values,
    )?)
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
    batch: Vec<SeriesInputV7>,
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
    let written =
        SegmentWriter::write_v7_with_provenance(batch, identity, ingest, meta, exemplars)?;
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

    let mut part = CompactionPart {
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
        declared_column_stats: Vec::new(),
    };
    // Metrics never carry declared typed columns -- they are a logs concept
    // (ADR-0873 decision 3), so this part has no eligible columns and stamps
    // nothing. Routed through the same validated commit-side path the logs
    // compactor uses, so "stamps nothing" is expressed the one way, not by a
    // second hand-built empty field.
    ravel_commit::declared_stats::stamp_compaction_part(&mut part, &[]);
    Ok(BuiltPart {
        key,
        bytes: Some(written.bytes),
        part,
        put_already_existed: false,
    })
}

/// PUT one part `CreateIfAbsent`; `AlreadyExists` is idempotent success (the
/// key embeds the content hash, so the stored bytes are identical by
/// construction). Returns [`PartPut`] naming which of the two happened, so the
/// bounded compaction path can flag an `AlreadyExists` part for post-publish
/// verification (ADR-0979 decision 3).
///
/// The part's bytes must still be present: this is only ever called at PUT
/// time, before any release, so a `None` here is an internal invariant breach
/// rather than a store fault.
pub async fn put_part(store: &dyn ObjectStoreBackend, part: &BuiltPart) -> Result<PartPut> {
    use ravel_object_store::{PutOptions, StoreError, UploadChecksum};
    let bytes = part.bytes.as_ref().ok_or_else(|| {
        MaintainError::Invariant(format!(
            "put_part called on part {} whose bytes were already released",
            part.key
        ))
    })?;
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(bytes));
    match store
        .put(
            &part.key,
            bytes.clone(),
            PutOptions::create_if_absent().with_checksum(checksum),
        )
        .await
    {
        Ok(_) => Ok(PartPut::Created),
        Err(StoreError::AlreadyExists) => Ok(PartPut::AlreadyExisted),
        Err(e) => Err(MaintainError::Store(e)),
    }
}
