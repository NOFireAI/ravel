//! Publish protocol and idempotency.
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
    /// `input_set_hash`); we HEAD-verified its parts, re-PUT any we could,
    /// and every referenced part is present. A part that is missing and not
    /// repairable from this run is a typed
    /// [`crate::MaintainError::ConvergedWinnerPartMissing`], never this
    /// variant: `Converged` means whole, not merely resolved.
    Converged { parts_repaired: usize },
    /// The `max_compaction_lifetime` deadline passed before the record PUT;
    /// the run abandoned and did NOT publish. Its parts
    /// are content-addressed and deterministic for the sealed bucket, so any
    /// later successful compaction over the same frozen input set republishes
    /// the identical keys and its record references them: an abandoned run's
    /// parts are never orphaned while the bucket can still be compacted. Sweep
    /// rule 3 collects them in exactly two cases: once some compaction record
    /// exists for the bucket (a leftover part no record names ages out as
    /// unreferenced), or once a retention tombstone exists (which makes any
    /// future compaction impossible, so every record-less part in the bucket
    /// is collectable). A bucket with neither a compaction record
    /// nor a tombstone keeps its record-less parts, because a future
    /// compaction may still publish a record naming them
    /// (docs/consistency-model.md).
    Abandoned,
}

/// A record-count conservation predicate. Given the summed input record count
/// and the summed built-part record count, it returns `true` when the rewrite
/// conserved records as this variant requires. Compaction (and EM's
/// format-migration rewrite, which drops nothing) supplies [`conserve_exact`];
/// EJ's later erasure rewrite supplies "input equals output plus the erased
/// count" by capturing the erased count in the closure (ADR-0064 decision 3
/// point 4, ADR-0066 decision 5). The primitive never hardcodes exact match:
/// the check is always taken from here, so a non-exact caller cannot silently
/// fall back to it.
pub trait ConservationPredicate {
    /// Whether `part_sample_count` conserves `input_sample_count` per this
    /// variant's arithmetic.
    fn conserved(&self, input_sample_count: u64, part_sample_count: u64) -> bool;
}

impl<F: Fn(u64, u64) -> bool> ConservationPredicate for F {
    fn conserved(&self, input_sample_count: u64, part_sample_count: u64) -> bool {
        self(input_sample_count, part_sample_count)
    }
}

/// The exact-conservation predicate (ADR-0048 decision 6): the built parts
/// carry exactly the records the inputs carry. Used by compaction and by EM's
/// format-migration rewrite, neither of which drops a record.
pub fn conserve_exact() -> impl ConservationPredicate {
    |input: u64, output: u64| input == output
}

/// Assemble the compaction record from the sorted inputs and built parts, then
/// publish it per §3.4 with the exact-conservation gate. `start_ns` is when
/// this run began (for the abandonment deadline); `created_unix_ns` on the
/// record is stamped from the clock at publish time (the supersession-horizon
/// anchor). This is the compaction entry point; the shared rewrite
/// primitive ([`crate::rewrite`]) calls [`publish_record_with_conservation`]
/// with its own predicate.
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
    publish_record_with_conservation(
        store,
        config,
        clock,
        bucket,
        inputs,
        input_set_hash,
        parts,
        start_ns,
        conserve_exact(),
    )
    .await
}

/// Assemble and publish the record exactly as [`publish_record`] does, but
/// with the record-count conservation gate taken as a parameter rather than
/// hardcoded to exact match. Every durability property of the publish path is
/// unchanged: the abandonment deadline, the `CreateIfAbsent` single-winner
/// serialization, and the racing-loser convergence/repair all behave
/// identically; only the predicate that decides whether the built parts
/// conserve the inputs' record count is pluggable, so EM's compaction variant
/// (exact) and EJ's later erasure variant (exact minus the erased set) share
/// this one publish path (ADR-0066 decision 5).
#[allow(clippy::too_many_arguments)]
pub async fn publish_record_with_conservation(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    clock: &dyn Clock,
    bucket: &Bucket,
    inputs: &[InputRecord],
    input_set_hash: &[u8; 32],
    parts: &[BuiltPart],
    start_ns: i64,
    conservation: impl ConservationPredicate,
) -> Result<PublishOutcome> {
    // Abandonment mirror of the writer interlock: past the deadline, a run
    // must never publish, so the sweeper's unreferenced-part rule stays safe.
    let now = clock.now_ns();
    if now.saturating_sub(start_ns) > config.max_compaction_lifetime_ns {
        tracing::warn!(
            elapsed_ns = now.saturating_sub(start_ns),
            "compaction run exceeded max_compaction_lifetime; abandoning without publish"
        );
        return Ok(PublishOutcome::Abandoned);
    }

    // Record-count conservation gate (ADR-0048 decision 6):
    // compaction is a verbatim page copy for every signal and never
    // dedups, so the built parts must carry exactly the records the inputs
    // carry. A rewrite that deliberately drops records supplies a
    // predicate that accounts for the drop; the exact-match compaction and
    // format-migration paths supply [`conserve_exact`]. Publishing a merge the
    // predicate rejects would be a permanent silent loss or gain (the resolver
    // excludes the inputs the moment the record lands, and the sweep removes
    // them after the horizon), so a rejected count aborts before the record is
    // PUT: the L0 inputs stay live and queryable, and any parts already PUT
    // age out under sweep rule 3 like any abandoned run's. This runs under
    // dry_run too, so a dry run reports the violation.
    let input_sample_count = checked_sample_sum(inputs.iter().map(|i| i.record.sample_count))?;
    let part_sample_count = checked_sample_sum(parts.iter().map(|p| p.part.sample_count))?;
    if !conservation.conserved(input_sample_count, part_sample_count) {
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
    // Finish/publish phase (issue #977): the encoded record payload is the only
    // allocation this phase adds on top of the parts still retained in the
    // merge's `PartSink`. Recording it here lets a report show the publish phase
    // is small next to the retained-parts plateau, not the source of a spike.
    if let Some(t) = config.merge_memory_tracker.as_ref() {
        t.set_publish_record_bytes(payload.len() as u64);
    }
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
            // The tombstone race is closed by verification, not retention
            // (ADR-0979 decision 3): a part that answered `AlreadyExists` at PUT
            // carries an abandoned run's age and could have been swept between
            // our existence check and this record PUT. HEAD-verify exactly those
            // parts now that the record is durable; a missing one fails loud with
            // a re-runnable typed error rather than leaving the record pointing
            // at a part the bounded compactor can no longer repair.
            verify_already_existed_parts(store, parts).await?;
            tracing::info!(key = %record_key, parts = parts.len(), "compaction record published");
            Ok(PublishOutcome::Published)
        }
        Err(StoreError::AlreadyExists) => {
            resolve_already_exists(store, &record_key, input_set_hash, parts).await
        }
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// HEAD every part whose PUT answered `AlreadyExists` (ADR-0979 decision 3),
/// after the compaction record that references them has been published. A
/// fresh-PUT part is age-zero and unreachable by the unreferenced-part sweep
/// for the run's whole duration, so it needs no check; an `AlreadyExists` part
/// carries an abandoned run's `last_modified` and can be inside the sweep's age
/// gate, so a tenant tombstone landing mid-run can delete it. If one is missing,
/// the run failed to make the record whole and cannot repair from RAM (the
/// bytes were released), so it fails loud with
/// [`MaintainError::AlreadyExistsPartVanished`]; the re-run converges by
/// re-PUTting the byte-identical part before the record resolves.
async fn verify_already_existed_parts(
    store: &dyn ObjectStoreBackend,
    parts: &[BuiltPart],
) -> Result<()> {
    for part in parts.iter().filter(|p| p.put_already_existed) {
        match store.head(&part.key).await {
            Ok(_) => {}
            Err(StoreError::NotFound) => {
                return Err(MaintainError::AlreadyExistsPartVanished {
                    part_key: part.key.clone(),
                });
            }
            Err(e) => return Err(MaintainError::Store(e)),
        }
    }
    Ok(())
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
/// keys make this safe), then report convergence. A missing part this run
/// cannot re-PUT (bytes released at PUT, or never built here) is a typed
/// [`MaintainError::ConvergedWinnerPartMissing`], never a silent convergence:
/// the record would reference an absent object. Different `input_set_hash`:
/// a sealed bucket cannot legitimately hold two input sets, so alarm and stop
/// without deleting anything.
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
                // Re-PUT only a part whose bytes we still hold. A bounded
                // compaction loser released its bytes at PUT (ADR-0979 decision
                // 3, `bytes` is `None`), so it takes the cannot-repair arm; the
                // record is still the truth and re-running the compaction from
                // scratch rebuilds and re-PUTs the byte-identical part.
                match our_parts.iter().find(|p| p.key == part_key) {
                    Some(ours) if ours.bytes.is_some() => {
                        crate::build::put_part(store, ours).await?;
                        repaired += 1;
                    }
                    // Cannot repair (bytes released at PUT, or a part this run
                    // never built): fail closed. Returning Converged here would
                    // hand the driver a whole bucket over a record that
                    // references an absent object, and later L0 cleanup turns
                    // that hole into data loss. The typed error carries the
                    // remedy (re-run; the rerun's fresh PUT restores the key),
                    // matching the fresh-record arm's
                    // AlreadyExistsPartVanished loudness.
                    Some(_) | None => {
                        return Err(MaintainError::ConvergedWinnerPartMissing { part_key });
                    }
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
            bytes: Some(bytes::Bytes::new()),
            put_already_existed: false,
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
