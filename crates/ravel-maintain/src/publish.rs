//! Publish protocol and idempotency (docs/compaction-retention-plan.md §3.4).
//! The compaction record's `CreateIfAbsent` PUT is the single serialization
//! point: correctness never depends on two compactors producing identical
//! bytes. On `AlreadyExists` a racing or prior run won; the loser verifies the
//! winner's parts and converges. Two records with different `input_set_hash`
//! in one sealed bucket is an invariant breach that alarms and stops.

use prost::Message;
use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum};
use ravel_proto::commit::v1::{CompactionInputIdentity, CompactionRecord};

use crate::bucket::Bucket;
use crate::build::BuiltPart;
use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read::InputRecord;

/// What publishing did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    /// This run's `CreateIfAbsent` landed the record; we are the winner.
    Published,
    /// A prior or racing run already published an equivalent record (same
    /// `input_set_hash`); we HEAD-and-repaired its parts and converged.
    Converged { parts_repaired: usize },
    /// The `max_compaction_lifetime` deadline passed before the record PUT;
    /// the run abandoned and did NOT publish (plan §3.4 point 4). Its parts
    /// age out as unreferenced (sweep rule 3) only once some compaction record
    /// already exists for the bucket; parts of a bucket that has never had a
    /// successful compaction record published are not collectable by rule 3
    /// (its standing precondition; docs/consistency-model.md, plan §5).
    Abandoned,
}

/// Assemble the compaction record from the sorted inputs and built parts, then
/// publish it per §3.4. `start_ns` is when this run began (for the
/// abandonment deadline); `created_unix_ns` on the record is stamped from the
/// clock at publish time (the supersession-horizon anchor, plan §5).
#[allow(clippy::too_many_arguments)]
pub async fn publish_record(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    clock: &dyn Clock,
    bucket: &Bucket,
    inputs: &[InputRecord],
    input_set_hash: &[u8; 32],
    parts: &[BuiltPart],
    start_ns: i64,
) -> Result<PublishOutcome> {
    // Abandonment mirror of the writer interlock: past the deadline, a run
    // must never publish, so the sweeper's unreferenced-part rule stays safe
    // (plan §3.4 point 4, §3.6 row 13).
    let now = clock.now_ns();
    if now.saturating_sub(start_ns) > config.max_compaction_lifetime_ns {
        tracing::warn!(
            elapsed_ns = now.saturating_sub(start_ns),
            "compaction run exceeded max_compaction_lifetime; abandoning without publish"
        );
        return Ok(PublishOutcome::Abandoned);
    }

    let signal = ravel_commit::signal::to_proto(bucket.signal) as i32;
    let identities: Vec<CompactionInputIdentity> = inputs
        .iter()
        .map(|i| CompactionInputIdentity {
            writer_id: i.record.writer_id.clone(),
            writer_epoch: i.record.writer_epoch,
            writer_seq: i.record.writer_seq,
        })
        .collect();
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        level: 1,
        inputs: identities,
        input_set_hash: input_set_hash.to_vec(),
        parts: parts.iter().map(|p| p.part.clone()).collect(),
        created_unix_ns: now,
    };

    let record_key = keys::compaction_record_key_for(&record)?;
    let payload = record.encode_to_vec();
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(&payload));
    let opts = PutOptions::create_if_absent().with_checksum(checksum);

    match store.put(&record_key, payload.into(), opts).await {
        Ok(_) => {
            tracing::info!(key = %record_key, parts = parts.len(), "compaction record published");
            Ok(PublishOutcome::Published)
        }
        Err(StoreError::AlreadyExists) => {
            resolve_already_exists(store, &record_key, input_set_hash, parts).await
        }
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// GET the record that beat us. Same `input_set_hash`: HEAD every part it
/// references and re-PUT any our-built part that is missing (content-addressed
/// keys make this safe), then report convergence. Different `input_set_hash`:
/// a sealed bucket cannot legitimately hold two input sets, so alarm and stop
/// without deleting anything (plan §3.4 point 3, §3.6 row 11).
async fn resolve_already_exists(
    store: &dyn ObjectStoreBackend,
    record_key: &str,
    our_hash: &[u8; 32],
    our_parts: &[BuiltPart],
) -> Result<PublishOutcome> {
    let existing = store.get(record_key, GetRange::Full).await?;
    let winner = CompactionRecord::decode(existing.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("winner record decode failed: {e}")))?;
    // The winner's key must reconstruct to the key we fetched it at.
    keys::verify_compaction_record_key(&winner, record_key)?;

    if winner.input_set_hash.as_slice() != our_hash.as_slice() {
        return Err(MaintainError::InputSetHashDivergence {
            observed_key: record_key.to_string(),
            ours: hex::encode(our_hash),
            theirs: hex::encode(&winner.input_set_hash),
        });
    }

    // Same input set: repair any missing winner part we can reproduce.
    let mut repaired = 0usize;
    for part in &winner.parts {
        let part_key = keys::reconstruct_l1_part_key(&winner, part)?;
        match store.head(&part_key).await {
            Ok(_) => {}
            Err(StoreError::NotFound) => {
                if let Some(ours) = our_parts.iter().find(|p| p.key == part_key) {
                    crate::build::put_part(store, ours).await?;
                    repaired += 1;
                } else {
                    tracing::warn!(
                        key = %part_key,
                        "winner references a part this run did not build; cannot repair"
                    );
                }
            }
            Err(e) => return Err(MaintainError::Store(e)),
        }
    }
    tracing::info!(
        parts_repaired = repaired,
        "converged on prior compaction record"
    );
    Ok(PublishOutcome::Converged {
        parts_repaired: repaired,
    })
}
