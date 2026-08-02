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
    /// are content-addressed and deterministic for the sealed bucket, so any
    /// later successful compaction over the same frozen input set republishes
    /// the identical keys and its record references them: an abandoned run's
    /// parts are never orphaned while the bucket can still be compacted. Sweep
    /// rule 3 collects them in exactly two cases: once some compaction record
    /// exists for the bucket (a leftover part no record names ages out as
    /// unreferenced), or once a retention tombstone exists (which makes any
    /// future compaction impossible, so every record-less part in the bucket
    /// is collectable; issue #273). A bucket with neither a compaction record
    /// nor a tombstone keeps its record-less parts, because a future
    /// compaction may still publish a record naming them
    /// (docs/consistency-model.md, plan §5).
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

    // Record-count conservation gate (ADR-0048 decision 6, review finding
    // S2-03): compaction is a verbatim page copy for every signal and never
    // dedups, so the built parts must carry exactly the records the inputs
    // carry. Publishing a lossy merge would be a permanent silent loss (the
    // resolver excludes the inputs the moment the record lands, and the
    // sweep removes them after the horizon), so a mismatch aborts before
    // anything is PUT: the L0 inputs stay live and queryable, and the
    // abandoned parts age out under sweep rule 3 like any abandoned run's.
    // This runs under dry_run too, so a dry run reports the violation.
    let input_sample_count = checked_sample_sum(inputs.iter().map(|i| i.record.sample_count))?;
    let part_sample_count = checked_sample_sum(parts.iter().map(|p| p.part.sample_count))?;
    if input_sample_count != part_sample_count {
        return Err(MaintainError::ConservationViolation {
            tenant_hash: hex::encode(bucket.tenant_hash.0),
            signal: bucket.signal.key_prefix().to_string(),
            shard: bucket.shard,
            ingest_hour_bucket: bucket.ingest_hour_bucket,
            input_sample_count,
            part_sample_count,
        });
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

    // Dry-run: the record and its key are assembled identically, but the
    // publishing PUT is skipped. A dry run only reaches here for a bucket with
    // no existing compaction record (compact_bucket returns AlreadyCompacted
    // before building anything otherwise), so the real run's outcome here is
    // always Published; the convergence/repair path is never dry-run reachable.
    if config.dry_run {
        return Ok(PublishOutcome::Published);
    }

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

/// Sum `sample_count`s in u64 with checked addition; an overflowing sum is
/// itself an invariant breach (real buckets are nowhere near 2^64 records),
/// never a silent wrap that could fake or mask a conservation mismatch.
fn checked_sample_sum(counts: impl Iterator<Item = u64>) -> Result<u64> {
    let mut sum: u64 = 0;
    for count in counts {
        sum = sum.checked_add(count).ok_or_else(|| {
            MaintainError::Invariant("sample_count sum overflowed u64".to_string())
        })?;
    }
    Ok(sum)
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use proptest::prelude::*;
    use ravel_object_store::list_all;
    use ravel_object_store::memory::MemoryStore;
    use ravel_proto::commit::v1::{CommitRecord, CompactionPart};
    use ravel_types::{Signal, TenantId};

    use super::*;
    use crate::clock::FixedClock;

    const SHARD: u32 = 7;
    const HOUR: u32 = 495_000;
    const ALL_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

    fn bucket(signal: Signal) -> Bucket {
        Bucket::new(TenantId::new("acme").hash(), signal, SHARD, HOUR)
    }

    /// A minimal input for the publish gate: only the identity fields the
    /// record assembly reads and the `sample_count` the gate sums matter.
    /// Key/bucket verification happens upstream in `load_inputs`, not here.
    fn input(seq: u64, sample_count: u64, bucket: &Bucket) -> InputRecord {
        let record = CommitRecord {
            format_version: 1,
            tenant_hash: bucket.tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(bucket.signal) as i32,
            shard: bucket.shard,
            writer_id: "00000000-0000-0000-0000-000000000001".to_string(),
            writer_epoch: 1,
            writer_seq: seq,
            sample_count,
            ingest_hour_bucket: bucket.ingest_hour_bucket,
            ..CommitRecord::default()
        };
        InputRecord {
            commit_key: format!("input/{seq}"),
            record,
        }
    }

    fn part(index: u32, sample_count: u64) -> BuiltPart {
        BuiltPart {
            key: format!("part/{index}"),
            bytes: bytes::Bytes::new(),
            part: CompactionPart {
                part_index: index,
                content_hash: vec![0u8; 32],
                sample_count,
                segment_format_version: crate::build::OUTPUT_FORMAT_VERSION,
                ..CompactionPart::default()
            },
        }
    }

    async fn publish(
        store: &MemoryStore,
        bucket: &Bucket,
        inputs: &[InputRecord],
        parts: &[BuiltPart],
    ) -> Result<PublishOutcome> {
        let clock = FixedClock::new(1);
        publish_record(
            store,
            &CompactorConfig::default(),
            &clock,
            bucket,
            inputs,
            &[7u8; 32],
            parts,
            1,
        )
        .await
    }

    /// The gate itself (ADR-0048 decision 6): output parts one record short
    /// of the input sum abort with the typed error carrying both sums and
    /// the full bucket identity, and nothing at all is written to the store.
    /// `publish_record` is the shared choke point for all three signals'
    /// pipelines, so all three are driven through the same assertion.
    #[tokio::test]
    async fn conservation_mismatch_aborts_publish() {
        for signal in ALL_SIGNALS {
            let store = MemoryStore::new();
            let bucket = bucket(signal);
            let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];
            let parts = vec![part(0, 16)]; // short by one record
            let err = publish(&store, &bucket, &inputs, &parts)
                .await
                .expect_err("lossy merge must not publish");
            match err {
                MaintainError::ConservationViolation {
                    tenant_hash,
                    signal: signal_prefix,
                    shard,
                    ingest_hour_bucket,
                    input_sample_count,
                    part_sample_count,
                } => {
                    assert_eq!(tenant_hash, hex::encode(bucket.tenant_hash.0));
                    assert_eq!(signal_prefix, signal.key_prefix());
                    assert_eq!(shard, SHARD);
                    assert_eq!(ingest_hour_bucket, HOUR);
                    assert_eq!(input_sample_count, 17);
                    assert_eq!(part_sample_count, 16);
                }
                other => panic!("expected ConservationViolation, got {other:?}"),
            }
            let listed = list_all(&store, "").await.expect("list");
            assert!(
                listed.is_empty(),
                "aborted publish must write nothing, found {listed:?}"
            );
        }
    }

    /// The complementary direction: a conserving publish (same sums, any
    /// part split) is not disturbed by the gate and lands exactly one
    /// record object, for every signal.
    #[tokio::test]
    async fn conserving_publish_writes_record() {
        for signal in ALL_SIGNALS {
            let store = MemoryStore::new();
            let bucket = bucket(signal);
            let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];
            let parts = vec![part(0, 12), part(1, 5)];
            let outcome = publish(&store, &bucket, &inputs, &parts)
                .await
                .expect("conserving publish");
            assert_eq!(outcome, PublishOutcome::Published);
            let listed = list_all(&store, "").await.expect("list");
            assert_eq!(listed.len(), 1, "exactly the record object: {listed:?}");
        }
    }

    /// Inputs invented records must abort the same as dropped ones: the gate
    /// is exact equality, not a one-sided bound.
    #[tokio::test]
    async fn conservation_surplus_also_aborts() {
        let store = MemoryStore::new();
        let bucket = bucket(Signal::Metrics);
        let inputs = vec![input(1, 10, &bucket)];
        let parts = vec![part(0, 11)]; // one invented record
        let err = publish(&store, &bucket, &inputs, &parts)
            .await
            .expect_err("surplus must not publish");
        assert!(matches!(err, MaintainError::ConservationViolation { .. }));
        assert!(list_all(&store, "").await.expect("list").is_empty());
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

        /// Over randomized multi-input buckets on every signal: any part
        /// split conserving the input sum publishes, and bumping any single
        /// part's count aborts with the typed error and writes nothing.
        #[test]
        fn conserving_merge_publishes_and_any_mutation_aborts(
            counts in prop::collection::vec(0u64..100_000, 1..8),
            part_count in 1usize..5,
            mutate_part in any::<prop::sample::Index>(),
            signal_idx in 0usize..3,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let bucket = bucket(ALL_SIGNALS[signal_idx]);
                let inputs: Vec<InputRecord> = counts
                    .iter()
                    .enumerate()
                    .map(|(i, &c)| input(i as u64, c, &bucket))
                    .collect();
                let total: u64 = counts.iter().sum();

                // Split the exact total across `part_count` parts.
                let base = total / part_count as u64;
                let mut parts: Vec<BuiltPart> = (0..part_count)
                    .map(|i| {
                        let c = if i == part_count - 1 {
                            total - base * (part_count as u64 - 1)
                        } else {
                            base
                        };
                        part(i as u32, c)
                    })
                    .collect();

                let store = MemoryStore::new();
                let outcome = publish(&store, &bucket, &inputs, &parts)
                    .await
                    .expect("conserving publish");
                prop_assert_eq!(outcome, PublishOutcome::Published);
                prop_assert_eq!(list_all(&store, "").await.unwrap().len(), 1);

                // Mutate one part's count; the merge no longer conserves.
                let idx = mutate_part.index(parts.len());
                parts[idx].part.sample_count += 1;
                let store = MemoryStore::new();
                let err = publish(&store, &bucket, &inputs, &parts).await;
                let aborted = matches!(err, Err(MaintainError::ConservationViolation { .. }));
                prop_assert!(aborted, "expected ConservationViolation, got {:?}", err);
                prop_assert!(list_all(&store, "").await.unwrap().is_empty());
                Ok(())
            })?;
        }
    }
}
