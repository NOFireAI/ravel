//! Streaming k-way catalog merge of decoded L0 inputs into RSEG v4 parts,
//! series-boundary split at `max_l1_part_bytes` (plan section 3.3 steps
//! 2-5). Builds each part fully in memory via `write_v4`: the plan's own
//! named risk (section 3.3, section 9) is that this needs an incremental
//! or multipart writer eventually; no such surface exists on
//! `ravel-segment` or `ravel-object-store` today, so this crate builds a
//! part's bytes in memory before it is ever PUT. Flagged as a deliberate
//! scope deviation, not a silent workaround.

use ravel_proto::segment::v1::SectionKind;
use ravel_segment::{
    CompactionMetaV4, IngestBounds, RunInputV4, RunValuePageV4, SegmentIdentity, SegmentWriter,
    SeriesEntry, SeriesInputV4, ValueKind, WrittenSegment,
};
use ravel_types::{LabelSet, SeriesId};
use uuid::Uuid;

use crate::error::MaintainError;
use crate::segread::{DecodedInput, section_slice};

/// Tenant/shard identity shared by every part of one compaction run.
pub struct BuildIdentity {
    pub tenant_hash: [u8; 16],
    pub shard: u32,
}

/// One built L1 part plus the bookkeeping the publish step (plan section
/// 3.4) needs to fill in a `CompactionPart` record entry: `write_v4`'s
/// `WrittenSegment` alone does not carry `first_series_id`/
/// `last_series_id`/`run_count`.
pub struct BuiltPart {
    pub part_index: u32,
    pub first_series_id: SeriesId,
    pub last_series_id: SeriesId,
    pub run_count: u64,
    pub written: WrittenSegment,
}

/// Merge `inputs` (already read via [`crate::segread::read_input`], in the
/// same canonical `(writer_id, epoch, seq)` order used for
/// `input_set_hash`) into one or more RSEG v4 parts.
///
/// The plan (section 4) says L1 footer `writer_id` "records the compactor
/// process uuid and is informational only, never part of dedup priority."
/// Taken literally that implies a fresh random id per run, which would
/// make the ticket's required determinism test (same inputs -> same
/// bytes, same keys) fail on every second build. Since writer_id truly is
/// excluded from dedup priority (run-level `created_unix_ns`/epoch/seq
/// copied from each input's commit record carries that instead), this
/// derives it deterministically from `input_set_hash` -- content-derived
/// rather than process-derived, but still informational-only and it
/// actually serves the plan's own stated goal (section 3.4: "identical
/// builds usually converge on identical part keys") better than a random
/// id would. Flagged as a resolved spec conflict, not silently picked.
pub fn merge_and_build(
    inputs: &[DecodedInput],
    identity: &BuildIdentity,
    ingest_hour_bucket: u32,
    input_set_hash: [u8; 32],
    max_l1_part_bytes: u64,
) -> Result<Vec<BuiltPart>, MaintainError> {
    let compactor_writer_id = deterministic_writer_id(&input_set_hash);
    let (min_ingest_ts_ns, max_ingest_ts_ns) = ingest_bounds(inputs);

    let mut pos = vec![0usize; inputs.len()];
    let mut parts = Vec::new();
    let mut part_index: u32 = 0;
    let mut pending: Vec<SeriesInputV4> = Vec::new();
    let mut pending_bytes: u64 = 0;
    let mut pending_runs: u64 = 0;

    while let Some(min_id) = next_series_id(inputs, &pos) {
        let mut runs = Vec::new();
        let mut labels: Option<LabelSet> = None;
        let mut value_kind: Option<ValueKind> = None;
        for (i, input) in inputs.iter().enumerate() {
            if pos[i] >= input.entries.len() {
                continue;
            }
            let entry = &input.entries[pos[i]];
            if entry.series_id != min_id {
                continue;
            }
            if labels.is_none() {
                labels = Some(entry.labels.clone());
            }
            match value_kind {
                None => value_kind = Some(entry.value_kind),
                Some(vk) if vk == entry.value_kind => {}
                Some(_) => {
                    return Err(MaintainError::MixedValueKindAcrossInputs {
                        series_id: hex::encode(min_id.0),
                    });
                }
            }
            runs.push(build_run(input, entry)?);
            pos[i] += 1;
        }

        let series_runs = runs.len() as u64;
        let series_bytes: u64 = runs
            .iter()
            .map(|r| {
                let value_len = match &r.value_page {
                    RunValuePageV4::Scalar(v) => v.len(),
                    RunValuePageV4::Histogram(v) => v.len(),
                };
                (r.ts_page.len() + value_len) as u64
            })
            .sum();

        if !pending.is_empty() && pending_bytes + series_bytes > max_l1_part_bytes {
            parts.push(flush_part(
                &mut pending,
                part_index,
                pending_runs,
                identity,
                compactor_writer_id,
                ingest_hour_bucket,
                input_set_hash,
                min_ingest_ts_ns,
                max_ingest_ts_ns,
            )?);
            part_index += 1;
            pending_bytes = 0;
            pending_runs = 0;
        }

        let labels = labels.ok_or(MaintainError::MergeInvariant(
            "series id selected as merge front but no input matched it",
        ))?;
        pending.push(SeriesInputV4 {
            series_id: min_id,
            labels,
            runs,
        });
        pending_bytes += series_bytes;
        pending_runs += series_runs;
    }

    if !pending.is_empty() {
        parts.push(flush_part(
            &mut pending,
            part_index,
            pending_runs,
            identity,
            compactor_writer_id,
            ingest_hour_bucket,
            input_set_hash,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        )?);
    }

    Ok(parts)
}

/// The smallest `series_id` among every input's current cursor position,
/// or `None` once every input is exhausted (merge done).
fn next_series_id(inputs: &[DecodedInput], pos: &[usize]) -> Option<SeriesId> {
    inputs
        .iter()
        .enumerate()
        .filter_map(|(i, input)| input.entries.get(pos[i]))
        .map(|entry| entry.series_id)
        .min()
}

fn build_run(input: &DecodedInput, entry: &SeriesEntry) -> Result<RunInputV4, MaintainError> {
    let ts_bytes = section_slice(&input.footer, SectionKind::TsPages, &input.bytes)?;
    let ts_page = slice_range(ts_bytes, entry.ts_page)?.to_vec();

    let value_page = match entry.value_kind {
        ValueKind::Scalar => {
            let val_bytes = section_slice(&input.footer, SectionKind::ValPages, &input.bytes)?;
            RunValuePageV4::Scalar(slice_range(val_bytes, entry.val_page)?.to_vec())
        }
        ValueKind::Histogram => {
            let hist_bytes = section_slice(&input.footer, SectionKind::HistPages, &input.bytes)?;
            RunValuePageV4::Histogram(slice_range(hist_bytes, entry.hist_page)?.to_vec())
        }
    };

    Ok(RunInputV4 {
        created_unix_ns: input.created_unix_ns,
        writer_epoch: input.writer_epoch,
        writer_seq: input.writer_seq,
        min_ts_ns: entry.min_ts_ns,
        max_ts_ns: entry.max_ts_ns,
        sample_count: entry.sample_count,
        ts_page,
        value_page,
    })
}

fn slice_range(bytes: &[u8], range: (u64, u64)) -> Result<&[u8], MaintainError> {
    let start = usize::try_from(range.0)
        .map_err(|_| MaintainError::Segment(ravel_segment::SegmentError::SectionOutOfBounds))?;
    let len = usize::try_from(range.1)
        .map_err(|_| MaintainError::Segment(ravel_segment::SegmentError::SectionOutOfBounds))?;
    let end = start.checked_add(len).ok_or(MaintainError::Segment(
        ravel_segment::SegmentError::SectionOutOfBounds,
    ))?;
    bytes.get(start..end).ok_or(MaintainError::Segment(
        ravel_segment::SegmentError::SectionOutOfBounds,
    ))
}

#[allow(clippy::too_many_arguments)]
fn flush_part(
    pending: &mut Vec<SeriesInputV4>,
    part_index: u32,
    run_count: u64,
    identity: &BuildIdentity,
    compactor_writer_id: Uuid,
    ingest_hour_bucket: u32,
    input_set_hash: [u8; 32],
    min_ingest_ts_ns: i64,
    max_ingest_ts_ns: i64,
) -> Result<BuiltPart, MaintainError> {
    let series = std::mem::take(pending);
    let first_series_id = series
        .first()
        .ok_or(MaintainError::MergeInvariant(
            "flush_part called on empty pending",
        ))?
        .series_id;
    let last_series_id = series
        .last()
        .ok_or(MaintainError::MergeInvariant(
            "flush_part called on empty pending",
        ))?
        .series_id;

    let segment_identity = SegmentIdentity {
        tenant_hash: identity.tenant_hash,
        shard: identity.shard,
        writer_id: compactor_writer_id.to_string(),
        writer_epoch: 0,
        writer_seq: u64::from(part_index),
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns,
        max_ingest_ts_ns,
    };
    let meta = CompactionMetaV4 {
        ingest_hour_bucket,
        input_set_hash,
        part_index,
        level: 1,
    };

    let written = SegmentWriter::write_v4(series, segment_identity, bounds, meta)?;

    Ok(BuiltPart {
        part_index,
        first_series_id,
        last_series_id,
        run_count,
        written,
    })
}

fn deterministic_writer_id(input_set_hash: &[u8; 32]) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&input_set_hash[..16]);
    Uuid::from_bytes(bytes)
}

fn ingest_bounds(inputs: &[DecodedInput]) -> (i64, i64) {
    if inputs.is_empty() {
        return (0, 0);
    }
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for input in inputs {
        min = min.min(input.min_ingest_ts_ns);
        max = max.max(input.max_ingest_ts_ns);
    }
    (min, max)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_commit::record::{NewCommitRecord, build};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutMode, PutOptions};
    use ravel_segment::{IngestBounds as V1IngestBounds, SegmentIdentity as V1Identity};
    use ravel_types::{Label, Sample, Signal, TenantHash, TenantId};

    async fn put_v1_series(
        store: &MemoryStore,
        tenant_hash: &TenantHash,
        shard: u32,
        writer_id: Uuid,
        metric: &str,
        samples: Vec<Sample>,
    ) -> crate::input::CompactionInput {
        let labels = LabelSet::new(vec![Label {
            name: "job".into(),
            value: "test".into(),
        }])
        .expect("labels");
        let series_id = SeriesId::compute(&TenantId::new("t"), metric, &labels).expect("series id");
        let series = vec![ravel_segment::SeriesInput {
            series_id,
            labels,
            samples: samples.clone(),
        }];
        let identity = V1Identity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let min_ts = samples.iter().map(|s| s.ts_ns).min().expect("nonempty");
        let max_ts = samples.iter().map(|s| s.ts_ns).max().expect("nonempty");
        let bounds = V1IngestBounds {
            min_ingest_ts_ns: min_ts,
            max_ingest_ts_ns: max_ts,
        };
        let written =
            ravel_segment::SegmentWriter::write(series, identity, bounds).expect("write v1");

        let record = build(NewCommitRecord {
            tenant_hash: *tenant_hash,
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: min_ts,
            max_ingest_ts_ns: max_ts,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        })
        .expect("valid record");

        store
            .put(
                &record.object_key,
                written.bytes,
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put segment object");
        let key = ravel_commit::keys::commit_key_for_record(&record).expect("commit key");
        store
            .put(
                &key,
                ravel_commit::record::encode(&record),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put commit record");

        crate::input::CompactionInput { key, record }
    }

    async fn decode_all(
        store: &MemoryStore,
        inputs: &[crate::input::CompactionInput],
    ) -> Vec<DecodedInput> {
        let mut out = Vec::new();
        for input in inputs {
            out.push(
                crate::segread::read_input(store, input)
                    .await
                    .expect("read input"),
            );
        }
        out
    }

    fn tenant_hash() -> TenantHash {
        TenantHash([0x44; 16])
    }

    #[tokio::test]
    async fn merges_two_disjoint_series_into_one_part() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let a = put_v1_series(
            &store,
            &th,
            1,
            Uuid::new_v4(),
            "metric_a",
            vec![Sample {
                ts_ns: 1_000,
                value: 1.0,
            }],
        )
        .await;
        let b = put_v1_series(
            &store,
            &th,
            1,
            Uuid::new_v4(),
            "metric_b",
            vec![Sample {
                ts_ns: 2_000,
                value: 2.0,
            }],
        )
        .await;
        let sorted = crate::input::sort_inputs_canonically(vec![a, b]).expect("sort");
        let hash = crate::input::input_set_hash(&sorted).expect("hash");
        let decoded = decode_all(&store, &sorted).await;

        let identity = BuildIdentity {
            tenant_hash: th.0,
            shard: 1,
        };
        let parts = merge_and_build(&decoded, &identity, 0, hash, 256 << 20).expect("merge");

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].written.summary.series_count, 2);
        assert_eq!(parts[0].written.summary.sample_count, 2);
        assert_eq!(parts[0].run_count, 2);
        assert!(parts[0].first_series_id <= parts[0].last_series_id);
    }

    #[tokio::test]
    async fn same_series_across_inputs_merges_into_multiple_runs() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let a = put_v1_series(
            &store,
            &th,
            1,
            Uuid::new_v4(),
            "metric_a",
            vec![Sample {
                ts_ns: 1_000,
                value: 1.0,
            }],
        )
        .await;
        let b = put_v1_series(
            &store,
            &th,
            1,
            Uuid::new_v4(),
            "metric_a",
            vec![Sample {
                ts_ns: 2_000,
                value: 2.0,
            }],
        )
        .await;
        let sorted = crate::input::sort_inputs_canonically(vec![a, b]).expect("sort");
        let hash = crate::input::input_set_hash(&sorted).expect("hash");
        let decoded = decode_all(&store, &sorted).await;

        let identity = BuildIdentity {
            tenant_hash: th.0,
            shard: 1,
        };
        let parts = merge_and_build(&decoded, &identity, 0, hash, 256 << 20).expect("merge");

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].written.summary.series_count, 1);
        assert_eq!(parts[0].written.summary.sample_count, 2);
        assert_eq!(parts[0].run_count, 2);
    }

    #[tokio::test]
    async fn tiny_max_part_bytes_splits_into_multiple_parts() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let a = put_v1_series(
            &store,
            &th,
            1,
            Uuid::new_v4(),
            "metric_a",
            vec![Sample {
                ts_ns: 1_000,
                value: 1.0,
            }],
        )
        .await;
        let b = put_v1_series(
            &store,
            &th,
            1,
            Uuid::new_v4(),
            "metric_b",
            vec![Sample {
                ts_ns: 2_000,
                value: 2.0,
            }],
        )
        .await;
        let sorted = crate::input::sort_inputs_canonically(vec![a, b]).expect("sort");
        let hash = crate::input::input_set_hash(&sorted).expect("hash");
        let decoded = decode_all(&store, &sorted).await;

        let identity = BuildIdentity {
            tenant_hash: th.0,
            shard: 1,
        };
        // 1 byte forces a split after the first series regardless of its
        // actual page size.
        let parts = merge_and_build(&decoded, &identity, 0, hash, 1).expect("merge");

        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_index, 0);
        assert_eq!(parts[1].part_index, 1);
        assert_eq!(parts[0].written.summary.series_count, 1);
        assert_eq!(parts[1].written.summary.series_count, 1);
    }

    #[tokio::test]
    async fn merge_is_deterministic_across_runs() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let a = put_v1_series(
            &store,
            &th,
            1,
            Uuid::new_v4(),
            "metric_a",
            vec![Sample {
                ts_ns: 1_000,
                value: 1.0,
            }],
        )
        .await;
        let b = put_v1_series(
            &store,
            &th,
            1,
            Uuid::new_v4(),
            "metric_b",
            vec![Sample {
                ts_ns: 2_000,
                value: 2.0,
            }],
        )
        .await;
        let sorted = crate::input::sort_inputs_canonically(vec![a, b]).expect("sort");
        let hash = crate::input::input_set_hash(&sorted).expect("hash");
        let decoded = decode_all(&store, &sorted).await;

        let identity = BuildIdentity {
            tenant_hash: th.0,
            shard: 1,
        };
        let parts_1 = merge_and_build(&decoded, &identity, 0, hash, 256 << 20).expect("merge 1");
        let parts_2 = merge_and_build(&decoded, &identity, 0, hash, 256 << 20).expect("merge 2");

        assert_eq!(parts_1.len(), parts_2.len());
        for (p1, p2) in parts_1.iter().zip(parts_2.iter()) {
            assert_eq!(p1.written.bytes, p2.written.bytes);
            assert_eq!(p1.written.summary.blake3, p2.written.summary.blake3);
        }
    }
}
