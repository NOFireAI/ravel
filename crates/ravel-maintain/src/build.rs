//! Streaming k-way catalog merge and verbatim-page copy into RSEG v5 parts
//! (docs/compaction-retention-plan.md §3.3 steps 3-4, with the ADR-0026/0027
//! v5 writer substitution). Series are emitted in global id order; every run
//! of a series is gathered from every input that carries it, its TS and
//! VAL-or-HIST pages are fetched by range and copied verbatim (never
//! decoded), and parts are split on series boundaries once accumulated page
//! bytes reach `max_l1_part_bytes`, so a series' runs never straddle a part
//! and part id-ranges are disjoint.

use std::collections::BTreeMap;

use bytes::Bytes;
use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::CompactionPart;
use ravel_segment::{
    CompactionMetaV4, IngestBounds, RunInputV4, RunValuePageV4, SegmentIdentity, SegmentWriter,
    SeriesInputV4, ValueKind,
};

use crate::bucket::Bucket;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read::{InputCatalog, InputRecord, RunPlan, SeriesPlan};

/// The current RSEG output version (ADR-0026, made the only version by
/// ADR-0027). Recorded in each part's `CompactionPart.segment_format_version`.
pub const OUTPUT_FORMAT_VERSION: u32 = 5;

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
pub async fn build_parts(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    inputs: &[InputRecord],
    catalogs: &[InputCatalog],
    input_set_hash: &[u8; 32],
) -> Result<Vec<BuiltPart>> {
    if inputs.len() != catalogs.len() {
        return Err(MaintainError::Invariant(
            "inputs and catalogs length mismatch".to_string(),
        ));
    }
    let ingest_bounds = merged_ingest_bounds(inputs);
    let input_set_hash16 = hex::encode(&input_set_hash[..8]);

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

    let mut parts = Vec::new();
    let mut batch: Vec<SeriesInputV4> = Vec::new();
    let mut batch_bytes: u64 = 0;
    let mut part_index: u32 = 0;

    for (_id, contributions) in by_series {
        let mut runs: Vec<RunInputV4> = Vec::new();
        let mut labels = None;
        let mut kind: Option<ValueKind> = None;
        let mut series_id = None;
        let mut series_page_bytes: u64 = 0;

        for (input_idx, plan) in contributions {
            let object_key = object_keys[input_idx];
            if labels.is_none() {
                labels = Some(plan.labels.clone());
                kind = Some(plan.kind);
                series_id = Some(plan.series_id);
            } else if kind != Some(plan.kind) {
                return Err(MaintainError::Invariant(format!(
                    "series {} has mixed value kinds across inputs",
                    plan.series_id.to_hex()
                )));
            }
            for run in &plan.runs {
                let (run_input, bytes) = fetch_run(store, object_key, run).await?;
                series_page_bytes = series_page_bytes.saturating_add(bytes);
                runs.push(run_input);
            }
        }

        let (Some(labels), Some(series_id)) = (labels, series_id) else {
            continue;
        };
        batch.push(SeriesInputV4 {
            series_id,
            labels,
            runs,
        });
        batch_bytes = batch_bytes.saturating_add(series_page_bytes);

        if batch_bytes >= config.max_l1_part_bytes {
            let part = flush_part(
                bucket,
                config,
                &ingest_bounds,
                input_set_hash,
                &input_set_hash16,
                part_index,
                std::mem::take(&mut batch),
            )?;
            if !config.dry_run {
                put_part(store, &part).await?;
            }
            parts.push(part);
            part_index += 1;
            batch_bytes = 0;
        }
    }

    if !batch.is_empty() {
        let part = flush_part(
            bucket,
            config,
            &ingest_bounds,
            input_set_hash,
            &input_set_hash16,
            part_index,
            batch,
        )?;
        if !config.dry_run {
            put_part(store, &part).await?;
        }
        parts.push(part);
    }

    Ok(parts)
}

/// Fetch one run's verbatim TS and VAL-or-HIST page bytes by range and pack
/// them into a [`RunInputV4`] with provenance from the input's commit record.
/// Returns the run plus the total fetched byte count (for part sizing).
async fn fetch_run(
    store: &dyn ObjectStoreBackend,
    object_key: &str,
    run: &RunPlan,
) -> Result<(RunInputV4, u64)> {
    let ts_page = fetch_range(store, object_key, run.ts_abs).await?;
    let page = fetch_range(store, object_key, run.page_abs).await?;
    let total = (ts_page.len() + page.len()) as u64;
    let value_page = match run.kind {
        ValueKind::Scalar => RunValuePageV4::Scalar(page),
        ValueKind::Histogram => RunValuePageV4::Histogram(page),
    };
    Ok((
        RunInputV4 {
            created_unix_ns: run.created_unix_ns,
            writer_epoch: run.writer_epoch,
            writer_seq: run.writer_seq,
            min_ts_ns: run.min_ts_ns,
            max_ts_ns: run.max_ts_ns,
            sample_count: run.sample_count,
            ts_page,
            value_page,
        },
        total,
    ))
}

async fn fetch_range(
    store: &dyn ObjectStoreBackend,
    key: &str,
    range: (u64, u64),
) -> Result<Vec<u8>> {
    let (off, len) = range;
    let got = store
        .get(key, GetRange::Range(off, off.saturating_add(len)))
        .await?;
    Ok(got.data.to_vec())
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
    let written = SegmentWriter::write_v5(batch, identity, ingest, meta)?;
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
