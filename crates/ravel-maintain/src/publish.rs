//! Publish and idempotency for a compaction run (plan section 3.4).
//!
//! The record `CreateIfAbsent` is the single serialization point: parts are
//! content-addressed (their key embeds the object blake3) and harmless to
//! leave behind, so the only ordering that matters for correctness is
//! "record visible only after all its parts are durable". This module PUTs
//! every part first, checks the abandonment deadline immediately before the
//! record PUT (the writer-interlock mirror, plan section 3.4 step 4), then
//! PUTs the compaction record. On `AlreadyExists` it converges on the
//! winner: same `input_set_hash` means a racing or prior run already won,
//! so it HEAD-verifies (and re-PUTs any missing) part the winner references
//! and reports success; a different `input_set_hash` at the same key is an
//! invariant breach that alarms and stops without deleting anything.

use std::collections::HashMap;

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};
use ravel_proto::commit::v1::{CompactionInputIdentity, CompactionPart, CompactionRecord};
use ravel_types::{Signal, TenantHash};

use crate::clock::Clock;
use crate::config::MaintainConfig;
use crate::error::MaintainError;
use crate::input::{self, CompactionInput};
use crate::merge::BuiltPart;

/// Everything one compaction run needs to publish: the bucket identity, the
/// canonically sorted inputs it consumed (verbatim into the record's input
/// list, plan section 3.1), the `input_set_hash` those inputs produced, the
/// built parts from [`crate::merge::merge_and_build`], and the injected
/// clock time the run began (the abandonment deadline is measured from it).
pub struct PublishInput<'a> {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
    /// Inputs in the same canonical `(writer_id, epoch, seq)` order used to
    /// compute `input_set_hash` (see [`input::sort_inputs_canonically`]).
    pub inputs: &'a [CompactionInput],
    pub input_set_hash: [u8; 32],
    pub parts: &'a [BuiltPart],
    pub started_ns: i64,
}

/// Outcome of a publish attempt (both variants are success).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// This run wrote the compaction record itself.
    Published,
    /// A prior or racing run's record with the same `input_set_hash` already
    /// existed; this run HEAD-verified its parts (re-PUTting any it could
    /// supply) and reports success without writing a record.
    Converged,
}

/// Publish a compaction run per plan section 3.4.
///
/// Steps, in order: PUT every part `CreateIfAbsent` (`AlreadyExists` is
/// success, the key embeds the content hash); check the abandonment
/// deadline immediately before the record PUT and refuse to publish past it
/// (parts are content-addressed and harmless to leave behind); PUT the
/// compaction record `CreateIfAbsent`. On `AlreadyExists`, converge on the
/// winner (see [`converge`]).
pub async fn publish(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &MaintainConfig,
    input: &PublishInput<'_>,
) -> Result<PublishOutcome, MaintainError> {
    let input_set_hash16 = input::hash16(&input.input_set_hash);

    // Step 1: PUT each part, CreateIfAbsent. AlreadyExists = success.
    for part in input.parts {
        let key = part_key(input, &input_set_hash16, part)?;
        put_content_addressed(store, &key, part.written.bytes.clone()).await?;
    }

    // Step 4: abandonment mirror of the writer interlock, checked
    // immediately before the record PUT. A run that could not get here
    // within max_compaction_lifetime must never publish its record; its
    // already-written parts are content-addressed and swept as unreferenced
    // (plan section 5).
    let now_ns = clock.now_ns();
    if config.is_abandoned(input.started_ns, now_ns) {
        return Err(MaintainError::Abandoned {
            started_ns: input.started_ns,
            now_ns,
        });
    }

    // Step 2: PUT the compaction record, CreateIfAbsent.
    let record = build_record(input, now_ns);
    let record_key = ravel_commit::keys::compaction_record_key(
        &input.tenant_hash,
        input.signal,
        input.shard,
        input.ingest_hour_bucket,
        &input_set_hash16,
    )?;
    let payload = record.encode_to_vec();
    match store
        .put(&record_key, payload.into(), PutOptions::create_if_absent())
        .await
    {
        Ok(_) => Ok(PublishOutcome::Published),
        // Step 3: someone else's record is already there. Converge.
        Err(StoreError::AlreadyExists) => converge(store, &record_key, input).await,
        Err(err) => Err(err.into()),
    }
}

/// Converge on the record already at `record_key` (plan section 3.4 step 3).
///
/// Same `input_set_hash`: a racing or prior run won; HEAD every part it
/// references and re-PUT any missing one this run can supply (content
/// addressed, so a re-PUT is byte-identical or absent, never a divergent
/// overwrite). Different `input_set_hash`: a sealed bucket somehow yielded
/// two input sets, the writer-interlock invariant is breached; alarm,
/// delete nothing, stop.
async fn converge(
    store: &dyn ObjectStoreBackend,
    record_key: &str,
    input: &PublishInput<'_>,
) -> Result<PublishOutcome, MaintainError> {
    let existing = store.get(record_key, GetRange::Full).await?;
    let stored = CompactionRecord::decode(existing.data.as_ref())?;

    if stored.input_set_hash.as_slice() != input.input_set_hash.as_slice() {
        return Err(MaintainError::InputSetHashMismatch {
            ours: hex::encode(input.input_set_hash),
            stored: hex::encode(&stored.input_set_hash),
        });
    }

    // Same input set: HEAD-and-repair the winner's parts. Index our own
    // built parts by content hash so a missing winner part we happen to
    // have identical bytes for can be re-PUT under the winner's own key.
    let ours_by_hash: HashMap<&[u8], &BuiltPart> = input
        .parts
        .iter()
        .map(|p| (p.written.summary.blake3.as_slice(), p))
        .collect();

    for part in &stored.parts {
        let key = ravel_commit::keys::reconstruct_l1_part_key(&stored, part)?;
        match store.head(&key).await {
            Ok(_) => {}
            Err(StoreError::NotFound) => {
                if let Some(built) = ours_by_hash.get(part.content_hash.as_slice()) {
                    put_content_addressed(store, &key, built.written.bytes.clone()).await?;
                }
                // Else our build diverged from the winner's and we cannot
                // supply this part's bytes. It is content-addressed and
                // unreferenced-by-us; a re-run or the sweeper resolves it.
                // (The determinism the merge guarantees makes this
                // unreachable when both runs saw the same inputs.)
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(PublishOutcome::Converged)
}

/// PUT a content-addressed object `CreateIfAbsent`. `AlreadyExists` is
/// success: the key embeds the object's blake3, so the stored bytes are
/// identical to `bytes` by construction.
async fn put_content_addressed(
    store: &dyn ObjectStoreBackend,
    key: &str,
    bytes: bytes::Bytes,
) -> Result<(), MaintainError> {
    match store.put(key, bytes, PutOptions::create_if_absent()).await {
        Ok(_) | Err(StoreError::AlreadyExists) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn part_key(
    input: &PublishInput<'_>,
    input_set_hash16: &str,
    part: &BuiltPart,
) -> Result<String, MaintainError> {
    let hash16 = input::hash16(&part.written.summary.blake3);
    Ok(ravel_commit::keys::l1_part_key(
        &input.tenant_hash,
        input.signal,
        input.shard,
        input.ingest_hour_bucket,
        input_set_hash16,
        part.part_index,
        &hash16,
    )?)
}

fn build_record(input: &PublishInput<'_>, created_unix_ns: i64) -> CompactionRecord {
    let inputs = input
        .inputs
        .iter()
        .map(|i| CompactionInputIdentity {
            writer_id: i.record.writer_id.clone(),
            writer_epoch: i.record.writer_epoch,
            writer_seq: i.record.writer_seq,
        })
        .collect();
    let parts = input
        .parts
        .iter()
        .map(|p| CompactionPart {
            part_index: p.part_index,
            first_series_id: p.first_series_id.0.to_vec(),
            last_series_id: p.last_series_id.0.to_vec(),
            content_hash: p.written.summary.blake3.to_vec(),
            object_size: p.written.bytes.len() as u64,
            sample_count: p.written.summary.sample_count,
            series_count: p.written.summary.series_count,
            run_count: p.run_count,
            min_event_ts_ns: p.written.summary.min_event_ts_ns,
            max_event_ts_ns: p.written.summary.max_event_ts_ns,
            // RSEG v4: merge.rs writes write_v4 parts. The proto comment and
            // ticket text still say "v3"/"write_v3"; that mismatch is
            // resolved in merge.rs's own doc comment (v4 is the current
            // segment format, the ticket text is stale). Kept at 4 here to
            // match what the part bytes actually are.
            segment_format_version: 4,
        })
        .collect();

    CompactionRecord {
        format_version: 1,
        tenant_hash: input.tenant_hash.0.to_vec(),
        signal: ravel_commit::signal::to_proto(input.signal) as i32,
        shard: input.shard,
        ingest_hour_bucket: input.ingest_hour_bucket,
        level: 1,
        inputs,
        input_set_hash: input.input_set_hash.to_vec(),
        parts,
        created_unix_ns,
    }
}
