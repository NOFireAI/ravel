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
use crate::request_ledger::{RequestLedger, RequestPhase, note_get, note_metadata, note_put};

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
/// primitive ([`crate::rewrite`]) calls `publish_identities_with_conservation`
/// and `publish_superseding_record` with its own predicate.
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
    publish_identities_with_conservation(
        store,
        config,
        clock,
        bucket,
        inputs,
        input_identities(inputs),
        input_set_hash,
        parts,
        start_ns,
        conservation,
    )
    .await
}

/// [`publish_record_with_conservation`] with the record's input identities
/// given: `identities` is what the record names, one per entry of `inputs`,
/// whose sample counts the parts must conserve. An `identities` whose length
/// differs from `inputs`' is refused with [`MaintainError::Invariant`] before
/// the conservation gate, and nothing is written.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn publish_identities_with_conservation(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    clock: &dyn Clock,
    bucket: &Bucket,
    inputs: &[InputRecord],
    identities: Vec<CompactionInputIdentity>,
    input_set_hash: &[u8; 32],
    parts: &[BuiltPart],
    start_ns: i64,
    conservation: impl ConservationPredicate,
) -> Result<PublishOutcome> {
    if identities.len() != inputs.len() {
        return Err(MaintainError::Invariant(format!(
            "compaction record over {} inputs would name {} input identities; \
             nothing written",
            inputs.len(),
            identities.len()
        )));
    }
    publish_compaction_record(
        store,
        config,
        clock,
        bucket,
        inputs.iter().map(|i| i.record.sample_count),
        identities,
        None,
        1,
        input_set_hash,
        parts,
        start_ns,
        conservation,
    )
    .await
}

/// Publish a version 2 compaction record (ADR-0066, the force 2 amendment)
/// that supersedes `predecessor`, the record at `predecessor_key`, with
/// `parts` as its re-encoded part set.
///
/// The record's inputs are copied verbatim from the predecessor, its
/// `superseded_record_key` names it, and its `input_set_hash` is the version 2
/// hash over both, which `input_set_hash` must equal: the parts were built
/// under it, since it is in their keys and footers. The conservation gate
/// compares the predecessor's part record counts with `parts`'. Its `level`
/// is the predecessor's, the level the re-encoded parts' footers carry.
/// Everything else is [`publish_record_with_conservation`]'s protocol unchanged: the
/// abandonment deadline, the `CreateIfAbsent` record PUT under the canonical
/// key for the stored hash, and the convergence on an equal winner.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn publish_superseding_record(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    clock: &dyn Clock,
    bucket: &Bucket,
    predecessor_key: &str,
    predecessor: &CompactionRecord,
    input_set_hash: &[u8; 32],
    parts: &[BuiltPart],
    start_ns: i64,
    conservation: impl ConservationPredicate,
) -> Result<PublishOutcome> {
    let expected = ravel_commit::erasure::compute_superseding_compaction_input_set_hash(
        &predecessor.inputs,
        predecessor_key,
    );
    if &expected != input_set_hash {
        return Err(MaintainError::Invariant(format!(
            "superseding record over {predecessor_key} was built under input_set_hash {} \
             but its version 2 hash is {}",
            hex::encode(input_set_hash),
            hex::encode(expected)
        )));
    }
    publish_compaction_record(
        store,
        config,
        clock,
        bucket,
        predecessor.parts.iter().map(|p| p.sample_count),
        predecessor.inputs.clone(),
        Some(predecessor_key),
        predecessor.level,
        input_set_hash,
        parts,
        start_ns,
        conservation,
    )
    .await
}

/// The publish protocol both record versions share. `input_sample_counts` is
/// what the record's parts must conserve; `superseded_record_key` is `Some`
/// for a version 2 record and `None` for version 1. `level` is the record's
/// level: 1 for a compaction of L0 inputs, the predecessor's for a version 2
/// record. Every compaction record today is at level 1, which is also what
/// the logs and spans codecs stamp in their part footers and what the query
/// path's L1 identity check accepts.
#[allow(clippy::too_many_arguments)]
async fn publish_compaction_record(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    clock: &dyn Clock,
    bucket: &Bucket,
    input_sample_counts: impl Iterator<Item = u64>,
    identities: Vec<CompactionInputIdentity>,
    superseded_record_key: Option<&str>,
    level: u32,
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
    let input_sample_count = checked_sample_sum(input_sample_counts)?;
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
    let format_version = match superseded_record_key {
        Some(_) => ravel_commit::record::COMPACTION_SUPERSEDING_FORMAT_VERSION,
        None => 1,
    };
    let record = CompactionRecord {
        format_version,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        level,
        inputs: identities,
        input_set_hash: input_set_hash.to_vec(),
        parts: parts.iter().map(|p| p.part.clone()).collect(),
        created_unix_ns: now,
        superseded_record_key: superseded_record_key.unwrap_or_default().to_string(),
    };

    let record_key = keys::compaction_record_key_for(&record)?;
    let payload = bytes::Bytes::from(record.encode_to_vec());
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
    // publishing PUT is skipped and the outcome reported is Published; the
    // convergence/repair path is never dry-run reachable. A version 1 record
    // reaches here only for a bucket with no compaction record
    // (compact_bucket returns AlreadyCompacted otherwise). A version 2 record
    // reaches here for a bucket that holds its predecessor, but only when the
    // planning listing held no record superseding that predecessor (the
    // selector would have excluded the predecessor and the run planned from
    // the superseding record instead), so no record existed at this key when
    // the run planned.
    if config.dry_run {
        return Ok(PublishOutcome::Published);
    }

    let ledger = config.request_ledger.as_ref();
    // Publish phase (ADR-0996 task 996-8): the record PUT is recorded before its
    // outcome is inspected, so the `AlreadyExists` convergence arm below counts
    // the request it spent exactly like the winning arm.
    let payload_len = payload.len() as u64;
    crate::compat_policy::before_put(config.compat_run.as_ref(), store).await?;
    let put = store.put(&record_key, payload.clone(), opts).await;
    note_put(ledger, RequestPhase::Publish, payload_len);
    match put {
        Ok(_) => {
            ravel_invariant::inv_reachable!(
                "catalog: compaction record published",
                std::collections::BTreeMap::from([("record_key", record_key.clone())])
            );
            assert_record_is_ours(store, &record_key, &payload, input_set_hash, ledger).await;
            // The tombstone race is closed by verification, not retention
            // (ADR-0979 decision 3): a part that answered `AlreadyExists` at PUT
            // carries an abandoned run's age and could have been swept between
            // our existence check and this record PUT. HEAD-verify exactly those
            // parts now that the record is durable; a missing one fails loud with
            // a re-runnable typed error rather than leaving the record pointing
            // at a part the bounded compactor can no longer repair.
            verify_already_existed_parts(store, parts, ledger).await?;
            tracing::info!(key = %record_key, parts = parts.len(), "compaction record published");
            Ok(PublishOutcome::Published)
        }
        Err(StoreError::AlreadyExists) => {
            resolve_already_exists(
                store,
                &record_key,
                &payload,
                input_set_hash,
                parts,
                ledger,
                config.compat_run.as_ref(),
            )
            .await
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
    ledger: Option<&RequestLedger>,
) -> Result<()> {
    for part in parts.iter().filter(|p| p.put_already_existed) {
        let head = store.head(&part.key).await;
        note_metadata(ledger, RequestPhase::Publish);
        match head {
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

/// Re-read the record a winning compaction publish just wrote and judge it
/// against the bytes this attempt PUT. The predicate is the record clause of
/// `QueryVisibleDataCorrectUnderDuplicateOwnership` (a present record's content
/// equals the first writer's), the clause the negative config
/// `ownership-as-publication-authority` breaks; through it this site carries the
/// `OwnershipIsNotPublicationAuthority` row of ADR-4326 decision 2.
///
/// The record key is a bucket prefix plus the input-set hash prefix, every
/// record PUT is `CreateIfAbsent`, and the payload carries `created_unix_ns`, so
/// a fresh GET returns bytes other than ours only if another publisher, owner or
/// not, overwrote the record, or if the record was deleted (a retention
/// `delete_all`, the superseded sweep) and a stalled publisher then created it
/// again. The re-read cannot tell an overwrite from a delete-then-recreate and
/// reports either as false. It observes such a write only when it lands between
/// this attempt's PUT and its re-read; a later overwrite is not seen here.
///
/// The verdict is `Some(true)` when the GET returns our payload, `Some(false)`
/// for any other bytes, and `None` on `NotFound` or any error (a record a later
/// sweep removed was not replaced, and a failed read cannot tell). The GET and
/// its ledger charge live inside the `inv_always_if_known!` condition, so the
/// default build, where `EVALUATES` is false, issues no request and evaluates
/// nothing.
async fn assert_record_is_ours(
    store: &dyn ObjectStoreBackend,
    record_key: &str,
    our_payload: &bytes::Bytes,
    our_hash: &[u8; 32],
    ledger: Option<&RequestLedger>,
) {
    let _ = (store, our_payload, ledger);
    if !ravel_invariant::EVALUATES {
        return;
    }
    let details = std::collections::BTreeMap::from([
        ("record_key", record_key.to_string()),
        ("our_input_set_hash", hex::encode(our_hash)),
    ]);
    let _ = &details;
    ravel_invariant::inv_always_if_known!(
        {
            let after = store.get(record_key, GetRange::Full).await;
            note_get(ledger, RequestPhase::Publish, &after);
            match after {
                Ok(got) => Some(got.data == *our_payload),
                Err(_) => None,
            }
        },
        "maintenance: the store holds the record a winning compaction publish wrote",
        details
    );
}

/// The identity of each input, in input order: what a version 1 record names
/// and its `input_set_hash` covers.
pub(crate) fn input_identities(inputs: &[InputRecord]) -> Vec<CompactionInputIdentity> {
    inputs
        .iter()
        .map(|i| CompactionInputIdentity {
            writer_id: i.record.writer_id.clone(),
            writer_epoch: i.record.writer_epoch,
            writer_seq: i.record.writer_seq,
        })
        .collect()
}

/// The `catalog/compaction-swaps-record` mutant: the last identity moves to
/// the max UUID, a writer id no writer is issued (`ravel_commit::rng` hands
/// writers version 4 UUIDs). It still sorts last, so the list stays in
/// canonical order.
pub(crate) fn swap_one_identity(identities: &mut [CompactionInputIdentity]) {
    if let Some(last) = identities.last_mut() {
        last.writer_id = uuid::Uuid::max().to_string();
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
/// keys make this safe), then report convergence. A missing part this run
/// cannot re-PUT (bytes released at PUT, or never built here) is a typed
/// [`MaintainError::ConvergedWinnerPartMissing`], never a silent convergence:
/// the record would reference an absent object. Different `input_set_hash`:
/// a sealed bucket cannot legitimately hold two input sets, so alarm and stop
/// without deleting anything.
#[allow(clippy::too_many_arguments)]
async fn resolve_already_exists(
    store: &dyn ObjectStoreBackend,
    record_key: &str,
    our_payload: &bytes::Bytes,
    our_hash: &[u8; 32],
    our_parts: &[BuiltPart],
    ledger: Option<&RequestLedger>,
    compat: Option<&crate::compat_policy::CompatRun>,
) -> Result<PublishOutcome> {
    let existing = store.get(record_key, GetRange::Full).await;
    note_get(ledger, RequestPhase::Publish, &existing);
    let existing = existing?;
    if ravel_invariant::inv_mutant!("catalog/compaction-loser-overwrites") {
        store
            .put(record_key, our_payload.clone(), PutOptions::default())
            .await?;
    }
    let outcome = resolve_against_winner(
        store,
        record_key,
        &existing.data,
        our_hash,
        our_parts,
        ledger,
        compat,
    )
    .await;
    // The mutant writes our payload, whose bytes differ from the winner's, so
    // it also trips "catalog: a published compaction record is never
    // replaced"; a same-bytes overwrite would keep its ETag on RustFS, and the
    // version check below could not see it.
    if matches!(outcome, Err(MaintainError::InputSetHashDivergence { .. }))
        && ravel_invariant::inv_mutant!("maintenance/mo-diverge-overwrites-record")
    {
        store
            .put(record_key, our_payload.clone(), PutOptions::default())
            .await?;
    }
    assert_loser_resolution(store, record_key, &existing, our_hash, &outcome, ledger).await;
    // The record the store holds after this attempt, not the one it fetched,
    // unless the fetched record is byte-equal to ours: nothing this attempt
    // writes can then change it. A record a later sweep removed was not
    // replaced, and a failed read cannot tell.
    ravel_invariant::inv_always!(
        existing.data == *our_payload || {
            let after = store.get(record_key, GetRange::Full).await;
            note_get(ledger, RequestPhase::Publish, &after);
            match after {
                Ok(after) => after.data == existing.data,
                Err(_) => true,
            }
        },
        "catalog: a published compaction record is never replaced",
        std::collections::BTreeMap::from([
            ("record_key", record_key.to_string()),
            ("our_input_set_hash", hex::encode(our_hash)),
        ])
    );
    outcome
}

/// DivergentInputSetNeverMutates for one losing publish (ADR-4326 decision
/// 1): against a winner record, as fetched, whose input-set hash differs from
/// ours, a HEAD after the attempt resolved finds the record at the version
/// fetched, so the loser wrote nothing to it. A record gone or a failed HEAD
/// cannot tell. Returns at once in the default build.
async fn assert_loser_resolution(
    store: &dyn ObjectStoreBackend,
    record_key: &str,
    existing: &ravel_object_store::GetOutcome,
    our_hash: &[u8; 32],
    outcome: &Result<PublishOutcome>,
    ledger: Option<&RequestLedger>,
) {
    let _ = (store, existing, ledger);
    if !ravel_invariant::EVALUATES {
        return;
    }
    let details = std::collections::BTreeMap::from([
        ("record_key", record_key.to_string()),
        ("our_input_set_hash", hex::encode(our_hash)),
        ("outcome", format!("{outcome:?}")),
    ]);
    let _ = &details;
    let winner = ravel_commit::record::decode_compaction(&existing.data).ok();
    let diverged = winner
        .as_ref()
        .is_some_and(|w| w.input_set_hash.as_slice() != our_hash.as_slice());
    if diverged {
        ravel_invariant::inv_reachable!(
            "maintenance: a loser observed a divergent input set and alarmed",
            details
        );
        ravel_invariant::inv_always_if_known!(
            {
                let after = store.head(record_key).await;
                note_metadata(ledger, RequestPhase::Publish);
                match after {
                    Ok(after) => Some(after.version == existing.version),
                    Err(_) => None,
                }
            },
            "maintenance: a loser observing a divergent input-set hash fails closed and never overwrites the record",
            details
        );
    }
}

/// [`resolve_already_exists`] against the winner's record bytes `existing`.
async fn resolve_against_winner(
    store: &dyn ObjectStoreBackend,
    record_key: &str,
    existing: &[u8],
    our_hash: &[u8; 32],
    our_parts: &[BuiltPart],
    ledger: Option<&RequestLedger>,
    compat: Option<&crate::compat_policy::CompatRun>,
) -> Result<PublishOutcome> {
    let winner = ravel_commit::record::decode_compaction(existing)
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
    // Winner parts the store answered absent to this attempt's HEAD and that
    // no acknowledged re-PUT restored.
    let mut absent: Vec<String> = Vec::new();
    for part in &winner.parts {
        let part_key = keys::reconstruct_l1_part_key(&winner, part)?;
        let head = store.head(&part_key).await;
        note_metadata(ledger, RequestPhase::Publish);
        if matches!(head, Err(StoreError::NotFound)) {
            absent.push(part_key.clone());
        }
        match head {
            Ok(_) => {}
            Err(StoreError::NotFound) => {
                // Re-PUT only a part whose bytes we still hold. A bounded
                // compaction loser released its bytes at PUT (ADR-0979 decision
                // 3, `bytes` is `None`), so it takes the cannot-repair arm; the
                // record is still the truth and re-running the compaction from
                // scratch rebuilds and re-PUTs the byte-identical part, provided
                // the rerun uses the winner's part-split settings: part
                // boundaries depend on them, and they are not part of the
                // record's identity (issue #2351).
                match our_parts.iter().find(|p| p.key == part_key) {
                    Some(ours) if ours.bytes.is_some() => {
                        // A repair re-PUT is a part PUT: it is attributed to
                        // `PartPut` by what the request writes, beside every
                        // other L1 part write, not to the phase that noticed
                        // the hole.
                        crate::build::put_part_with_ledger(store, ours, ledger, compat).await?;
                        absent.retain(|k| k != &part_key);
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
                        if ravel_invariant::inv_mutant!(
                            "maintenance/mo-missing-part-reports-converged"
                        ) {
                            continue;
                        }
                        return Err(MaintainError::ConvergedWinnerPartMissing { part_key });
                    }
                }
            }
            Err(e) => return Err(MaintainError::Store(e)),
        }
    }
    // Judged from the store's answers to this attempt's part HEADs and
    // re-PUTs: every winner part was answered present or re-PUT.
    ravel_invariant::inv_always!(
        absent.is_empty(),
        "maintenance: a loser reports Converged only when the winner part is observed present in the store",
        std::collections::BTreeMap::from([
            ("record_key", record_key.to_string()),
            ("our_input_set_hash", hex::encode(our_hash)),
            ("absent_parts", format!("{absent:?}")),
        ])
    );
    ravel_invariant::inv_reachable!(
        "maintenance: a loser converged on the winner record",
        std::collections::BTreeMap::from([
            ("record_key", record_key.to_string()),
            ("our_input_set_hash", hex::encode(our_hash)),
        ])
    );
    tracing::info!(
        parts_repaired = repaired,
        "converged on prior compaction record"
    );
    Ok(PublishOutcome::Converged {
        parts_repaired: repaired,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    use proptest::prelude::*;
    use ravel_object_store::list_all;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetOutcome, ObjectMeta, PutOutcome};
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

    /// A record whose identities do not pair one-to-one with the inputs whose
    /// sample counts the gate sums is refused with the typed error, before the
    /// conservation gate (the counts here conserve) and before any PUT, in both
    /// directions.
    #[tokio::test]
    async fn identities_not_one_per_input_refuse_publish() {
        let bucket = bucket(Signal::Metrics);
        let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];
        let parts = vec![part(0, 17)];
        let all = input_identities(&inputs);
        for identities in [all[..1].to_vec(), [all.clone(), all[..1].to_vec()].concat()] {
            let store = MemoryStore::new();
            let count = identities.len();
            let err = publish_identities_with_conservation(
                &store,
                &CompactorConfig::default(),
                &FixedClock::new(1),
                &bucket,
                &inputs,
                identities,
                &[7u8; 32],
                &parts,
                1,
                conserve_exact(),
            )
            .await
            .expect_err("a record not naming one identity per input must not publish");
            match err {
                MaintainError::Invariant(message) => assert!(
                    message.contains(&format!(
                        "over 2 inputs would name {count} input identities"
                    )),
                    "{message}"
                ),
                other => panic!("expected Invariant, got {other:?}"),
            }
            assert!(list_all(&store, "").await.expect("list").is_empty());
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

    /// Wave-5b (ADR-0873 decision 3): stamping a part's declared columns does
    /// not disturb the conservation gate. A part carrying a valid
    /// declared-column stamp but a `sample_count` one record short of the input
    /// sum still aborts with the typed error and writes nothing -- the gate sums
    /// `part.sample_count`, which the stamp lives beside and never changes.
    ///
    /// Prove-the-test: comment out the `if !conservation.conserved(..)` guard in
    /// `publish_record_with_conservation` and this rigged mismatch publishes, so
    /// the `expect_err` fails.
    #[tokio::test]
    async fn conservation_gate_still_fires_with_stamped_parts() {
        use ravel_types::declared_stats::{
            DeclaredColumnStat, DeclaredStatType, DeclaredStatValue,
        };

        let store = MemoryStore::new();
        let bucket = bucket(Signal::Logs);
        let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];
        let mut p = part(0, 16); // short by one record (input sum is 17)
        let stamp = DeclaredColumnStat::new(
            "http.status",
            DeclaredStatType::I64,
            Some(DeclaredStatValue::I64(200)),
            Some(DeclaredStatValue::I64(500)),
            0,
        )
        .expect("valid stamp");
        ravel_commit::declared_stats::stamp_compaction_part(&mut p.part, &[stamp]);
        assert!(
            !p.part.declared_column_stats.is_empty(),
            "the part must actually carry a stamp for this to test the interaction"
        );

        let err = publish(&store, &bucket, &inputs, &[p])
            .await
            .expect_err("a lossy merge must not publish, stamped or not");
        assert!(
            matches!(err, MaintainError::ConservationViolation { .. }),
            "expected ConservationViolation, got {err:?}"
        );
        assert!(
            list_all(&store, "").await.expect("list").is_empty(),
            "aborted publish must write nothing"
        );
    }

    /// True in the invariants lane, where assertion sites record evaluations
    /// (`ravel-invariant/capture`); false under `antithesis` alone, which
    /// evaluates the assertions and records nothing.
    pub(crate) fn capturing() -> bool {
        let on = std::cell::Cell::new(false);
        ravel_invariant::inv_sometimes!(
            {
                on.set(ravel_invariant::test_support::capturing());
                true
            },
            "ravel-maintain test: evaluations are captured",
            ""
        );
        on.get()
    }

    /// Selects mutant `name` on this thread until the returned guard drops;
    /// `None` outside the invariants lane, where no mutant is compiled in.
    pub(crate) fn scoped_mutant(name: &'static str) -> Option<Box<dyn std::any::Any>> {
        let guard: std::cell::RefCell<Option<Box<dyn std::any::Any>>> = Default::default();
        let _ = name;
        ravel_invariant::inv_sometimes!(
            {
                *guard.borrow_mut() = ravel_invariant::test_support::scoped_mutant(name)
                    .expect("a registered mutant");
                true
            },
            "ravel-maintain test: a mutant is scoped",
            ""
        );
        guard.into_inner()
    }

    /// `(condition, details)` of every captured evaluation of `message` whose
    /// details mention `needle`; empty outside the invariants lane.
    pub(crate) fn evaluations(message: &'static str, needle: &str) -> Vec<(bool, String)> {
        let found = std::cell::RefCell::new(Vec::new());
        let _ = (message, needle);
        ravel_invariant::inv_sometimes!(
            {
                *found.borrow_mut() =
                    ravel_invariant::test_support::evaluations(message, needle).unwrap_or_default();
                true
            },
            "ravel-maintain test: evaluations are read",
            ""
        );
        found.into_inner()
    }

    const RECORD_NEVER_REPLACED: &str = "catalog: a published compaction record is never replaced";

    /// Publishes 17 input records as parts of 12 and 5 under `input_set_hash`
    /// and puts the parts the record names, then publishes the same inputs as
    /// one part of 17: the second attempt loses the create to a record whose
    /// parts differ. Returns the first record's bytes and the second outcome.
    async fn publish_then_lose_with_other_parts(
        store: &MemoryStore,
        input_set_hash: &[u8; 32],
    ) -> (String, bytes::Bytes, Result<PublishOutcome>) {
        let bucket = bucket(Signal::Metrics);
        let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];
        let clock = FixedClock::new(1);
        let config = CompactorConfig::default();
        let first = publish_record(
            store,
            &config,
            &clock,
            &bucket,
            &inputs,
            input_set_hash,
            &[part(0, 12), part(1, 5)],
            1,
        )
        .await
        .expect("first publish");
        assert_eq!(first, PublishOutcome::Published);
        let listed = list_all(store, "").await.expect("list");
        assert_eq!(listed.len(), 1, "{listed:?}");
        let record_key = listed[0].key.clone();
        let winner = store
            .get(&record_key, GetRange::Full)
            .await
            .expect("record")
            .data;
        let record = ravel_commit::record::decode_compaction(&winner).expect("decode");
        for p in &record.parts {
            let key = keys::reconstruct_l1_part_key(&record, p).expect("part key");
            store
                .put(&key, bytes::Bytes::new(), PutOptions::create_if_absent())
                .await
                .expect("put part");
        }
        let second = publish_record(
            store,
            &config,
            &clock,
            &bucket,
            &inputs,
            input_set_hash,
            &[part(0, 17)],
            1,
        )
        .await;
        (record_key, winner, second)
    }

    async fn record_bytes(store: &MemoryStore, record_key: &str) -> bytes::Bytes {
        store
            .get(record_key, GetRange::Full)
            .await
            .expect("record")
            .data
    }

    #[tokio::test]
    async fn mutant_compaction_loser_overwrites_breaks_record_immutable() {
        let Some(_mutant) = scoped_mutant("catalog/compaction-loser-overwrites") else {
            return;
        };
        let hash = [0x41; 32];
        let store = MemoryStore::new();
        let (key, winner, _) = publish_then_lose_with_other_parts(&store, &hash).await;
        let found = evaluations(RECORD_NEVER_REPLACED, &hex::encode(hash));
        assert!(
            found.iter().any(|(held, _)| !held),
            "the property is evaluated false for the overwrite: {found:?}"
        );
        assert_ne!(
            record_bytes(&store, &key).await,
            winner,
            "the mutant replaced it"
        );
    }

    #[tokio::test]
    async fn a_compaction_loser_converges_without_replacing_the_record() {
        if !capturing() {
            return;
        }
        let hash = [0x42; 32];
        let store = MemoryStore::new();
        let (key, winner, second) = publish_then_lose_with_other_parts(&store, &hash).await;
        assert_eq!(
            second.expect("the loser converges"),
            PublishOutcome::Converged { parts_repaired: 0 }
        );
        assert_eq!(record_bytes(&store, &key).await, winner);
        let found = evaluations(RECORD_NEVER_REPLACED, &hex::encode(hash));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].0, "{found:?}");
        let published = evaluations("catalog: compaction record published", "");
        assert!(!published.is_empty());
    }

    const NEVER_MUTATES: &str = "maintenance: a loser observing a divergent input-set hash fails closed and never overwrites the record";
    const CONVERGES: &str = "maintenance: a loser reports Converged only when the winner part is observed present in the store";

    /// Publishes 17 input records as parts of 12 and 5 under `hash` and, when
    /// `put_parts`, puts the parts the record names. Returns the record key
    /// and bytes.
    async fn publish_winner(
        store: &MemoryStore,
        hash: &[u8; 32],
        put_parts: bool,
    ) -> (String, bytes::Bytes) {
        let bucket = bucket(Signal::Metrics);
        let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];
        let outcome = publish_record(
            store,
            &CompactorConfig::default(),
            &FixedClock::new(1),
            &bucket,
            &inputs,
            hash,
            &[part(0, 12), part(1, 5)],
            1,
        )
        .await
        .expect("winner publish");
        assert_eq!(outcome, PublishOutcome::Published);
        let listed = list_all(store, "").await.expect("list");
        assert_eq!(listed.len(), 1, "{listed:?}");
        let key = listed[0].key.clone();
        let bytes = record_bytes(store, &key).await;
        if put_parts {
            let record = ravel_commit::record::decode_compaction(&bytes).expect("decode");
            for p in &record.parts {
                let part_key = keys::reconstruct_l1_part_key(&record, p).expect("part key");
                store
                    .put(
                        &part_key,
                        bytes::Bytes::new(),
                        PutOptions::create_if_absent(),
                    )
                    .await
                    .expect("put part");
            }
        }
        (key, bytes)
    }

    /// Publishes the same inputs as one part of 17 under `hash`: a loser to
    /// any record at the same key.
    async fn publish_loser(store: &MemoryStore, hash: &[u8; 32]) -> Result<PublishOutcome> {
        let bucket = bucket(Signal::Metrics);
        let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];
        publish_record(
            store,
            &CompactorConfig::default(),
            &FixedClock::new(1),
            &bucket,
            &inputs,
            hash,
            &[part(0, 17)],
            1,
        )
        .await
    }

    /// A hash that shares `hash`'s first 8 bytes, so its record key is the
    /// same, and differs after them.
    fn diverging(hash: [u8; 32]) -> [u8; 32] {
        let mut other = hash;
        other[31] ^= 0xFF;
        other
    }

    #[tokio::test]
    async fn mutant_diverge_overwrites_record_breaks_never_mutates() {
        let Some(_mutant) = scoped_mutant("maintenance/mo-diverge-overwrites-record") else {
            return;
        };
        let ours = [0x51; 32];
        let store = MemoryStore::new();
        let (key, winner) = publish_winner(&store, &diverging(ours), true).await;
        let lost = publish_loser(&store, &ours).await;
        assert!(
            matches!(lost, Err(MaintainError::InputSetHashDivergence { .. })),
            "{lost:?}"
        );
        assert_ne!(
            record_bytes(&store, &key).await,
            winner,
            "the mutant overwrote"
        );
        if !capturing() {
            return;
        }
        let found = evaluations(NEVER_MUTATES, &hex::encode(ours));
        assert!(
            found.iter().any(|(held, _)| !held),
            "DivergentInputSetNeverMutates is evaluated false for the overwrite: {found:?}"
        );
        let replaced = evaluations(RECORD_NEVER_REPLACED, &hex::encode(ours));
        assert!(
            replaced.iter().any(|(held, _)| !held),
            "the overwrite writes different bytes, so CompactionRecordImmutable is false too: {replaced:?}"
        );
    }

    #[tokio::test]
    async fn a_divergent_loser_never_mutates_the_record() {
        if !capturing() {
            return;
        }
        let ours = [0x52; 32];
        let store = MemoryStore::new();
        let (key, winner) = publish_winner(&store, &diverging(ours), true).await;
        let lost = publish_loser(&store, &ours).await;
        assert!(
            matches!(lost, Err(MaintainError::InputSetHashDivergence { .. })),
            "{lost:?}"
        );
        assert_eq!(record_bytes(&store, &key).await, winner);
        let found = evaluations(NEVER_MUTATES, &hex::encode(ours));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].0, "{found:?}");
        let alarmed = evaluations(
            "maintenance: a loser observed a divergent input set and alarmed",
            &hex::encode(ours),
        );
        assert_eq!(alarmed.len(), 1, "{alarmed:?}");
        assert!(evaluations(CONVERGES, &hex::encode(ours)).is_empty());
    }

    #[tokio::test]
    async fn mutant_missing_part_reports_converged_breaks_convergence() {
        let Some(_mutant) = scoped_mutant("maintenance/mo-missing-part-reports-converged") else {
            return;
        };
        let hash = [0x53; 32];
        let store = MemoryStore::new();
        // The winner's parts are absent and the loser never built them, so
        // it cannot re-PUT them.
        publish_winner(&store, &hash, false).await;
        let outcome = publish_loser(&store, &hash).await;
        assert_eq!(
            outcome.expect("the mutant reports convergence"),
            PublishOutcome::Converged { parts_repaired: 0 }
        );
        if !capturing() {
            return;
        }
        let found = evaluations(CONVERGES, &hex::encode(hash));
        assert!(
            found.iter().any(|(held, _)| !held),
            "MergeAttemptsConverge is evaluated false for the missing part: {found:?}"
        );
    }

    #[tokio::test]
    async fn a_loser_that_re_puts_a_missing_winner_part_converges_holding() {
        if !capturing() {
            return;
        }
        let hash = [0x56; 32];
        let store = MemoryStore::new();
        // The winner's parts are absent; the loser built the same parts at
        // the winner's keys and still holds their bytes.
        let (record_key, winner) = publish_winner(&store, &hash, false).await;
        let record = ravel_commit::record::decode_compaction(&winner).expect("decode");
        let parts: Vec<BuiltPart> = record
            .parts
            .iter()
            .map(|p| {
                let mut built = part(p.part_index, p.sample_count);
                built.key = keys::reconstruct_l1_part_key(&record, p).expect("part key");
                built
            })
            .collect();
        let bucket = bucket(Signal::Metrics);
        let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];
        let outcome = publish_record(
            &store,
            &CompactorConfig::default(),
            &FixedClock::new(1),
            &bucket,
            &inputs,
            &hash,
            &parts,
            1,
        )
        .await
        .expect("converges");
        assert_eq!(outcome, PublishOutcome::Converged { parts_repaired: 2 });
        assert_eq!(record_bytes(&store, &record_key).await, winner);
        for p in &parts {
            store.head(&p.key).await.expect("re-PUT part present");
        }
        let found = evaluations(CONVERGES, &hex::encode(hash));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].0, "{found:?}");
    }

    #[tokio::test]
    async fn a_loser_converges_only_over_present_winner_parts() {
        if !capturing() {
            return;
        }
        let present = [0x54; 32];
        let store = MemoryStore::new();
        publish_winner(&store, &present, true).await;
        assert_eq!(
            publish_loser(&store, &present).await.expect("converges"),
            PublishOutcome::Converged { parts_repaired: 0 }
        );
        let found = evaluations(CONVERGES, &hex::encode(present));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].0, "{found:?}");
        let converged = evaluations(
            "maintenance: a loser converged on the winner record",
            &hex::encode(present),
        );
        assert_eq!(converged.len(), 1, "{converged:?}");

        // A missing, non-re-PUTtable winner part fails closed: no
        // convergence is reported, so nothing is evaluated.
        let missing = [0x55; 32];
        let store = MemoryStore::new();
        publish_winner(&store, &missing, false).await;
        let err = publish_loser(&store, &missing)
            .await
            .expect_err("fails closed");
        assert!(
            matches!(err, MaintainError::ConvergedWinnerPartMissing { .. }),
            "{err:?}"
        );
        assert!(evaluations(CONVERGES, &hex::encode(missing)).is_empty());
    }

    /// Publishes inputs of 10 and 7 records as parts of 12 and 5 and returns
    /// the inputs' identities with the published record.
    async fn publish_and_read_record() -> (Vec<CompactionInputIdentity>, CompactionRecord) {
        let store = MemoryStore::new();
        let bucket = bucket(Signal::Metrics);
        let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];
        let outcome = publish(&store, &bucket, &inputs, &[part(0, 12), part(1, 5)])
            .await
            .expect("publish");
        assert_eq!(outcome, PublishOutcome::Published);
        let listed = list_all(&store, "").await.expect("list");
        assert_eq!(listed.len(), 1, "{listed:?}");
        let record = ravel_commit::record::decode_compaction(
            &store
                .get(&listed[0].key, GetRange::Full)
                .await
                .expect("record")
                .data,
        )
        .expect("decode");
        let identities = inputs
            .iter()
            .map(|i| CompactionInputIdentity {
                writer_id: i.record.writer_id.clone(),
                writer_epoch: i.record.writer_epoch,
                writer_seq: i.record.writer_seq,
            })
            .collect();
        (identities, record)
    }

    #[tokio::test]
    async fn a_published_record_names_exactly_its_inputs() {
        let (identities, record) = publish_and_read_record().await;
        assert_eq!(record.inputs, identities);
    }

    const RECORD_IS_OURS: &str =
        "maintenance: the store holds the record a winning compaction publish wrote";

    type ReReadHook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

    /// A store that runs `hook` once, before answering the first GET of a key
    /// `keys::parse_compaction_record_key` accepts (an L1 compaction record, not
    /// an L0 commit record or a rewrite record). The winning
    /// publish's re-read of its own record is that GET, so the hook models a
    /// concurrent publisher writing between this attempt's record PUT and its
    /// re-read.
    struct FirstGetHook {
        inner: Arc<MemoryStore>,
        hook: Mutex<Option<ReReadHook>>,
    }

    impl FirstGetHook {
        fn new(inner: Arc<MemoryStore>, hook: ReReadHook) -> Self {
            FirstGetHook {
                inner,
                hook: Mutex::new(Some(hook)),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for FirstGetHook {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> std::result::Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> std::result::Result<GetOutcome, StoreError> {
            if keys::parse_compaction_record_key(key).is_ok() {
                let hook = self.hook.lock().expect("hook").take();
                if let Some(hook) = hook {
                    hook().await;
                }
            }
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> std::result::Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> std::result::Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> std::result::Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// The single compaction record key in `store`.
    async fn record_key_of(store: &MemoryStore) -> String {
        list_all(store, "")
            .await
            .expect("list")
            .into_iter()
            .find(|m| m.key.ends_with(&format!(".{}", keys::COMMIT_SUFFIX)))
            .expect("a compaction record")
            .key
    }

    /// One run of a winning publish contended at its re-read.
    struct ReReadRun {
        winner: Result<PublishOutcome>,
        loser: PublishOutcome,
        key: String,
        /// The record bytes the winner PUT, read before the loser ran.
        winner_bytes: bytes::Bytes,
        /// The record bytes after the loser ran (the loser's, under the mutant;
        /// the winner's otherwise).
        after_bytes: bytes::Bytes,
        store: Arc<MemoryStore>,
    }

    /// Publishes a winner through a [`FirstGetHook`] whose hook, firing on the
    /// winner's record re-read, puts the winner's part keys (so a loser can
    /// converge) and then runs a real losing publish against the inner store
    /// under `hash`. The loser's `CreateIfAbsent` loses to the winner's record;
    /// whether it overwrites the record is the `catalog/compaction-loser-overwrites`
    /// mutant's choice, scoped by the caller.
    async fn publish_with_concurrent_loser_on_reread(hash: [u8; 32]) -> ReReadRun {
        let inner = Arc::new(MemoryStore::new());
        let bucket = bucket(Signal::Metrics);
        let inputs = vec![input(1, 10, &bucket), input(2, 7, &bucket)];

        let winner_slot: Arc<Mutex<Option<bytes::Bytes>>> = Arc::new(Mutex::new(None));
        let after_slot: Arc<Mutex<Option<bytes::Bytes>>> = Arc::new(Mutex::new(None));
        let loser_slot: Arc<Mutex<Option<PublishOutcome>>> = Arc::new(Mutex::new(None));

        let hook_inner = Arc::clone(&inner);
        let hook_winner = Arc::clone(&winner_slot);
        let hook_after = Arc::clone(&after_slot);
        let hook_loser = Arc::clone(&loser_slot);
        let hook: ReReadHook = Box::new(move || {
            Box::pin(async move {
                let key = record_key_of(hook_inner.as_ref()).await;
                let winner_bytes = record_bytes(hook_inner.as_ref(), &key).await;
                *hook_winner.lock().expect("winner slot") = Some(winner_bytes.clone());
                let record =
                    ravel_commit::record::decode_compaction(&winner_bytes).expect("decode");
                for p in &record.parts {
                    let part_key = keys::reconstruct_l1_part_key(&record, p).expect("part key");
                    hook_inner
                        .put(
                            &part_key,
                            bytes::Bytes::new(),
                            PutOptions::create_if_absent(),
                        )
                        .await
                        .expect("put winner part");
                }
                let loser = publish_loser(hook_inner.as_ref(), &hash)
                    .await
                    .expect("the loser resolves");
                *hook_loser.lock().expect("loser slot") = Some(loser);
                let after = record_bytes(hook_inner.as_ref(), &key).await;
                *hook_after.lock().expect("after slot") = Some(after);
            })
        });

        let wrapper = FirstGetHook::new(Arc::clone(&inner), hook);
        let winner = publish_record(
            &wrapper,
            &CompactorConfig::default(),
            &FixedClock::new(1),
            &bucket,
            &inputs,
            &hash,
            &[part(0, 12), part(1, 5)],
            1,
        )
        .await;

        let key = record_key_of(inner.as_ref()).await;
        ReReadRun {
            winner,
            loser: loser_slot
                .lock()
                .expect("loser slot")
                .clone()
                .expect("the loser ran"),
            winner_bytes: winner_slot
                .lock()
                .expect("winner slot")
                .clone()
                .expect("the winner record was read"),
            after_bytes: after_slot
                .lock()
                .expect("after slot")
                .clone()
                .expect("the record was read after the loser"),
            key,
            store: inner,
        }
    }

    #[tokio::test]
    async fn mutant_loser_overwrite_breaks_winning_publish_record() {
        let Some(_mutant) = scoped_mutant("catalog/compaction-loser-overwrites") else {
            return;
        };
        let hash = [0x61; 32];
        let run = publish_with_concurrent_loser_on_reread(hash).await;
        assert_eq!(
            run.winner.expect("the winner published"),
            PublishOutcome::Published
        );
        let found = evaluations(RECORD_IS_OURS, &hex::encode(hash));
        assert!(
            found.iter().any(|(held, _)| !held),
            "the winner's re-read observed the loser's overwrite: {found:?}"
        );
        // The record the winner re-read holds the loser's bytes, not its own.
        assert_ne!(run.after_bytes, run.winner_bytes, "the mutant replaced it");
        let stored = record_bytes(run.store.as_ref(), &run.key).await;
        assert_eq!(stored, run.after_bytes, "the stored record is the loser's");
        let stored = ravel_commit::record::decode_compaction(&stored).expect("decode");
        let counts: Vec<u64> = stored.parts.iter().map(|p| p.sample_count).collect();
        assert_eq!(
            counts,
            vec![17],
            "the stored record names the loser's single part, not the winner's 12 and 5"
        );
    }

    #[tokio::test]
    async fn a_winning_publish_keeps_its_record_under_a_concurrent_loser() {
        if !capturing() {
            return;
        }
        let hash = [0x62; 32];
        let run = publish_with_concurrent_loser_on_reread(hash).await;
        assert_eq!(
            run.winner.expect("the winner published"),
            PublishOutcome::Published
        );
        assert_eq!(run.loser, PublishOutcome::Converged { parts_repaired: 0 });
        let found = evaluations(RECORD_IS_OURS, &hex::encode(hash));
        assert!(
            found.iter().any(|(held, _)| *held),
            "the winner's re-read observed its own record: {found:?}"
        );
        assert!(
            !found.iter().any(|(held, _)| !held),
            "no re-read observed anything else: {found:?}"
        );
        assert_eq!(
            record_bytes(run.store.as_ref(), &run.key).await,
            run.winner_bytes,
            "the record is still the winner's"
        );
    }

    #[tokio::test]
    async fn an_uncontended_publish_evaluates_the_record_once() {
        if !capturing() {
            return;
        }
        let hash = [0x63; 32];
        let store = MemoryStore::new();
        publish_winner(&store, &hash, false).await;
        let found = evaluations(RECORD_IS_OURS, &hex::encode(hash));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].0, "{found:?}");
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
