//! Age-based retention (ADR-0019, docs/compaction-retention-plan.md §6): the
//! second deletion trigger, and the first that destroys data rather than a
//! redundant copy of it.
//!
//! The flow per sealed bucket is the shape the consistency-model already
//! promises: a durable tombstone transaction, then bucket-wide exclusion from
//! new snapshots (the resolver's job, ravel-catalog), then a horizon-gated
//! physical sweep here.
//!
//! 1. **Expiry evaluation** decodes the bucket's already-listed commit and
//!    compaction records and takes `max(max_event_ts_ns)` across all of them
//!    (no footer reads). A bucket is expired when it is sealed and that
//!    maximum is `< now - R`, so no sample younger than `R` is ever excluded
//!    (ADR-0019 decision 1; the impossibility floor).
//! 2. **Tombstone** is written `CreateIfAbsent` at the fixed per-bucket key
//!    with an injected `retired_at_ns`. It is durable and irreversible:
//!    raising `R` later never resurrects a tombstoned bucket (ADR-0019
//!    decision 2).
//! 3. **Physical sweep** runs once `now >= retired_at_ns + protection_horizon`,
//!    deleting in the fixed order L0 commit records, compaction records, L0
//!    data objects, L1 parts, then the tombstone last, and only after a
//!    verifying LIST shows the bucket's commit prefix holds only the tombstone
//!    and its `l1/` prefix is empty. Any residue leaves the tombstone in place
//!    for the next pass (ADR-0019 decision 4).
//!
//! Retention runs before compaction ([`maintain_bucket`], ADR-0019 decision
//! 6): an expired bucket is tombstoned, never compacted first. That ordering
//! is the efficiency-preferred path, not the correctness guarantee. The
//! correctness guarantee is the tombstone's bucket-wide exclusion plus the
//! ordinary sweep: even if a racing compactor publishes into a
//! just-tombstoned bucket, the exclusion covers its record and parts and the
//! physical sweep deletes them. [`crate::compact::compact_bucket`] also
//! declines when it lists a tombstone, but ADR-0019 calls that "an efficiency
//! measure only": its absence would only waste work, never corrupt data.

use prost::Message;
use ravel_commit::keys;
use ravel_commit::record;
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum, list_all,
};
use ravel_proto::commit::v1::{CommitRecord, CompactionRecord, RetentionTombstone};

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::compact::{CompactionOutcome, compact_bucket};
use crate::config::{CompactorConfig, RetentionConfig};
use crate::error::{MaintainError, Result};
use crate::read::{BucketListing, list_bucket};
use crate::sweep::LeaseCheck;

/// The outcome of one retention pass over a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionOutcome {
    /// No retention window is configured for this tenant; nothing to do.
    NoPolicy,
    /// The bucket is not yet sealed, so retention cannot evaluate it.
    NotSealed,
    /// Sealed but not expired (or holding no records): nothing to do.
    NotExpired,
    /// Expired: a tombstone is present (written this pass or already there)
    /// and the protection horizon has not elapsed, so no bytes were deleted.
    Tombstoned,
    /// Tombstone present and horizon elapsed, but a verifying LIST still found
    /// residue (a delete lost to a lease or a concurrent write), so the
    /// tombstone was left in place for the next pass to finish.
    SweptPartial,
    /// Tombstone present, horizon elapsed, bucket verified empty, tombstone
    /// deleted last: the bucket is fully retired.
    Swept,
}

/// Run one retention pass over a single sealed bucket (ADR-0019, plan §6):
/// evaluate expiry, write the tombstone if newly expired, and run the
/// horizon-gated physical sweep if a tombstone is already present and its
/// horizon has elapsed. Stateless and idempotent: a crashed pass re-run from
/// scratch converges (the tombstone is `CreateIfAbsent`, every delete is a
/// no-op if the object is already gone).
pub async fn retention_sweep_bucket(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    retention: &RetentionConfig,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
) -> Result<RetentionOutcome> {
    let Some(window_ns) = retention.window_for(&bucket.tenant_hash) else {
        return Ok(RetentionOutcome::NoPolicy);
    };
    let now = clock.now_ns();
    if !bucket.is_sealed(now, config) {
        return Ok(RetentionOutcome::NotSealed);
    }

    let listing = list_bucket(store, bucket).await?;

    // Already tombstoned: the only remaining work is the horizon-gated physical
    // sweep. Anchored on the durable retired_at_ns, exactly as supersession
    // anchors on the compaction record's created_unix_ns.
    if let Some(tombstone_key) = &listing.tombstone_key {
        let tombstone = get_tombstone(store, tombstone_key).await?;
        if now
            >= tombstone
                .retired_at_ns
                .saturating_add(config.protection_horizon_ns)
        {
            return physical_sweep(store, lease, bucket, &listing, tombstone_key).await;
        }
        return Ok(RetentionOutcome::Tombstoned);
    }

    // Not tombstoned: evaluate expiry from the bucket's records (no footer
    // reads; plan §6).
    let mut commit_records = Vec::with_capacity(listing.commit_keys.len());
    for key in &listing.commit_keys {
        commit_records.push(load_commit_record(store, key).await?);
    }
    let mut compaction_records = Vec::with_capacity(listing.compaction_record_keys.len());
    for key in &listing.compaction_record_keys {
        compaction_records.push(get_compaction_record(store, key).await?);
    }
    let max_event = max_event_ts(&commit_records, &compaction_records);
    if !is_expired(max_event, now, window_ns) {
        return Ok(RetentionOutcome::NotExpired);
    }

    write_tombstone(store, bucket, now, window_ns, &listing).await?;
    Ok(RetentionOutcome::Tombstoned)
}

/// Run retention before compaction over one bucket (ADR-0019 decision 6): the
/// retention check runs first, so an expired bucket is tombstoned and never
/// compacted. Compaction runs only when retention leaves the bucket live
/// (no policy / not sealed / not expired). Returns the retention outcome and
/// the compaction outcome, if compaction ran.
///
/// This is the efficiency-preferred ordering, not the correctness guarantee
/// (see the module docs and [`crate::compact::compact_bucket`]'s own
/// tombstone check).
pub async fn maintain_bucket(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    retention: &RetentionConfig,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
) -> Result<(RetentionOutcome, Option<CompactionOutcome>)> {
    let outcome = retention_sweep_bucket(store, clock, config, retention, lease, bucket).await?;
    let compaction = match outcome {
        // The bucket is (or is being) retired: never compact it.
        RetentionOutcome::Tombstoned | RetentionOutcome::Swept | RetentionOutcome::SweptPartial => {
            None
        }
        RetentionOutcome::NoPolicy | RetentionOutcome::NotSealed | RetentionOutcome::NotExpired => {
            Some(compact_bucket(store, clock, config, bucket).await?)
        }
    };
    Ok((outcome, compaction))
}

/// The maximum `max_event_ts_ns` across a bucket's L0 commit records and
/// compaction-record parts (plan §6). `None` when the bucket holds no records.
pub fn max_event_ts(
    commit_records: &[CommitRecord],
    compaction_records: &[CompactionRecord],
) -> Option<i64> {
    let mut max: Option<i64> = None;
    let mut bump = |v: i64| max = Some(max.map_or(v, |m: i64| m.max(v)));
    for rec in commit_records {
        bump(rec.max_event_ts_ns);
    }
    for rec in compaction_records {
        for part in &rec.parts {
            bump(part.max_event_ts_ns);
        }
    }
    max
}

/// Whether a bucket is expired under retention window `R` at `now_ns`: its
/// newest event is strictly older than `now - R` (ADR-0019 decision 1). A
/// bucket with no records (`None`) is never expired: there is nothing to
/// retire. This is the impossibility floor: any sample younger than `R` (event
/// ts `> now - R`) forces `max_event_ts > now - R`, so the bucket is not
/// expired and is never excluded.
pub fn is_expired(max_event_ts: Option<i64>, now_ns: i64, retention_window_ns: i64) -> bool {
    match max_event_ts {
        Some(max) => max < now_ns.saturating_sub(retention_window_ns),
        None => false,
    }
}

/// Write the retention tombstone `CreateIfAbsent` (ADR-0019 decision 2). An
/// `AlreadyExists` means a concurrent pass won the race; either way the bucket
/// is now tombstoned, so it is not an error.
async fn write_tombstone(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    retired_at_ns: i64,
    window_ns: i64,
    listing: &BucketListing,
) -> Result<()> {
    let record_count = (listing.commit_keys.len() + listing.compaction_record_keys.len()) as u64;
    let tombstone = RetentionTombstone {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: ravel_commit::signal::to_proto(bucket.signal) as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        retired_at_ns,
        // Validated `>= floor > 0`, so the cast is lossless.
        retention_window_ns: window_ns as u64,
        record_count_observed: record_count,
    };
    let key = keys::retention_tombstone_key_for(&tombstone)?;
    let payload = tombstone.encode_to_vec();
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(&payload));
    let opts = PutOptions::create_if_absent().with_checksum(checksum);
    match store.put(&key, payload.into(), opts).await {
        Ok(_) | Err(StoreError::AlreadyExists) => Ok(()),
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// Horizon-gated physical sweep (ADR-0019 decision 4, plan §6). Deletes in the
/// fixed order L0 commit records, compaction records, L0 data objects, L1
/// parts, then the tombstone last, and only after a verifying LIST shows the
/// bucket's commit prefix holds only the tombstone and its `l1/` prefix is
/// empty. Any residue leaves the tombstone in place.
async fn physical_sweep(
    store: &dyn ObjectStoreBackend,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
    listing: &BucketListing,
    tombstone_key: &str,
) -> Result<RetentionOutcome> {
    // Resolve the derived data-object and part keys BEFORE deleting the
    // records that name them (records are deleted first).
    let mut l0_data_keys: Vec<String> = Vec::new();
    for key in &listing.commit_keys {
        match store.get(key, GetRange::Full).await {
            Ok(got) => {
                let record = record::decode(&got.data)?;
                l0_data_keys.push(keys::reconstruct_data_key(&record)?);
            }
            Err(StoreError::NotFound) => {}
            Err(e) => return Err(MaintainError::Store(e)),
        }
    }
    let mut l1_part_keys: Vec<String> = Vec::new();
    for key in &listing.compaction_record_keys {
        match store.get(key, GetRange::Full).await {
            Ok(got) => {
                let record = CompactionRecord::decode(got.data.as_ref()).map_err(|e| {
                    MaintainError::Invariant(format!("compaction record decode failed: {e}"))
                })?;
                keys::verify_compaction_record_key(&record, key)?;
                for part in &record.parts {
                    l1_part_keys.push(keys::reconstruct_l1_part_key(&record, part)?);
                }
            }
            Err(StoreError::NotFound) => {}
            Err(e) => return Err(MaintainError::Store(e)),
        }
    }

    // Deletion order (docs/consistency-model.md "Deletion and GC", ADR-0019
    // decision 4): records, then data objects, then L1 parts, tombstone last.
    delete_all(store, lease, &listing.commit_keys).await?;
    delete_all(store, lease, &listing.compaction_record_keys).await?;
    delete_all(store, lease, &l0_data_keys).await?;
    delete_all(store, lease, &l1_part_keys).await?;

    // Verify the bucket is empty before deleting the tombstone: the commit
    // prefix must contain only the tombstone, and the l1/ prefix must be empty.
    if !bucket_is_empty_but_tombstone(store, bucket).await? {
        return Ok(RetentionOutcome::SweptPartial);
    }
    if !lease.is_protected(tombstone_key) {
        store.delete(tombstone_key).await?;
    } else {
        return Ok(RetentionOutcome::SweptPartial);
    }
    Ok(RetentionOutcome::Swept)
}

/// Delete each key idempotently, skipping any the [`LeaseCheck`] protects
/// (a protected key becomes residue that the verifying LIST will catch, so the
/// tombstone stays for a later pass).
async fn delete_all(
    store: &dyn ObjectStoreBackend,
    lease: &dyn LeaseCheck,
    keys: &[String],
) -> Result<()> {
    for key in keys {
        if lease.is_protected(key) {
            continue;
        }
        store.delete(key).await?;
    }
    Ok(())
}

/// A fresh strongly consistent check that the bucket holds nothing but its
/// tombstone: the commit prefix contains only the tombstone entry, and the
/// `l1/` prefix for this bucket is empty (ADR-0019 decision 4's verifying
/// LIST).
async fn bucket_is_empty_but_tombstone(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
) -> Result<bool> {
    let commit_prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )?;
    for meta in list_all(store, &commit_prefix).await? {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(keys::BucketEntry::Tombstone(_)) => {}
            Ok(_) => return Ok(false),
            Err(keys::KeyError::UnknownBucketEntryShape(k)) => {
                return Err(MaintainError::UnknownBucketEntry(k));
            }
            Err(e) => return Err(MaintainError::Key(e)),
        }
    }
    let l1_prefix = l1_bucket_prefix(bucket);
    Ok(list_all(store, &l1_prefix).await?.is_empty())
}

/// `t/<tenant_hash_hex>/<signal>/l1/<shard>/<ingest_hour>/` -- the prefix
/// covering every L1 part of one bucket.
fn l1_bucket_prefix(bucket: &Bucket) -> String {
    format!(
        "t/{}/{}/{}/{:04}/{}/",
        bucket.tenant_hash.to_hex(),
        bucket.signal.key_prefix(),
        keys::L1_DIR,
        bucket.shard,
        keys::ingest_hour_string(bucket.ingest_hour_bucket),
    )
}

/// GET, decode, and validate one L0 commit record.
async fn load_commit_record(store: &dyn ObjectStoreBackend, key: &str) -> Result<CommitRecord> {
    let got = store.get(key, GetRange::Full).await?;
    Ok(record::decode(&got.data)?)
}

/// GET, decode, and key-verify one compaction record (ADR-0010 §7).
async fn get_compaction_record(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<CompactionRecord> {
    let got = store.get(key, GetRange::Full).await?;
    let record = CompactionRecord::decode(got.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("compaction record decode failed: {e}")))?;
    keys::verify_compaction_record_key(&record, key)?;
    Ok(record)
}

/// GET, decode, and key-verify one retention tombstone (ADR-0010 §7 discipline).
async fn get_tombstone(store: &dyn ObjectStoreBackend, key: &str) -> Result<RetentionTombstone> {
    let got = store.get(key, GetRange::Full).await?;
    let tombstone = RetentionTombstone::decode(got.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("tombstone decode failed: {e}")))?;
    keys::verify_retention_tombstone_key(&tombstone, key)?;
    Ok(tombstone)
}
