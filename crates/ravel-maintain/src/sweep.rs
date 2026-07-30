//! The sweeper: one component, three eligibility rules
//! (docs/compaction-retention-plan.md §5, docs/consistency-model.md "Deletion
//! and GC"). This is the first implementation of any deletion in Ravel.
//!
//! 1. **Orphan GC** (ADR-0010 §11): an `l0/` data object with no commit
//!    record, older than `grace + max_flush_lifetime`. The writer interlock
//!    (a writer abandons any flush older than `max_flush_lifetime` and never
//!    publishes it afterward) is what makes this safe: a record-less object
//!    that old can never gain a commit record later, so deleting it cannot
//!    orphan a future reader. Commit-record absence is re-verified with a
//!    fresh strongly consistent LIST immediately before each delete.
//! 2. **Superseded-input sweep** (ADR-0018): the L0 commit records and data
//!    objects a compaction record names in its input list, once
//!    `now >= record.created_unix_ns + protection_horizon`. Records are
//!    deleted before data objects, so a crash mid-sweep never leaves a commit
//!    record pointing at a deleted data object visible to a resolver.
//! 3. **Unreferenced-part cleanup**: an `l1/` object referenced by no
//!    compaction record in its bucket, once a compaction record exists for
//!    that bucket and the object is older than `grace +
//!    max_compaction_lifetime`. Non-reference is re-verified with a fresh
//!    strongly consistent LIST immediately before each delete.
//!
//! All three are **signal-generic**: they operate only on commit-record,
//! compaction-record, and object *keys* plus store `last_modified`, never on a
//! segment byte, so nothing here needs to know RSEG from RLOG. All three are
//! stateless per pass, restartable from zero, and every delete is idempotent
//! (the object-store contract makes deleting a missing key a success). The
//! clock is always injected; object age is read from `last_modified`, which
//! the object-store contract restricts to exactly GC age checks.
//!
//! The [`LeaseCheck`] hook is consulted before every delete in all three
//! rules. It ships as the no-op [`NoLeases`] ("nothing is ever protected"):
//! the consistency-model's "not lease-protected" precondition is then
//! vacuously satisfied everywhere. It is a seam for future slow-consumer work
//! (plan §5, Q3), not live logic; no lease machinery is built behind it.

use std::collections::{HashMap, HashSet};

use prost::Message;
use ravel_commit::keys::{self, BucketEntry, KeyError};
use ravel_commit::record;
use ravel_object_store::{GetRange, ObjectMeta, ObjectStoreBackend, StoreError, list_all};
use ravel_proto::commit::v1::CompactionRecord;
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read::verify_commit_key;

/// A hook the sweeper consults before every delete, in all three rules. The
/// only implementation today is [`NoLeases`] (nothing is ever protected); this
/// is a seam for future reader-lease / slow-consumer work (plan §5, Q3), never
/// a correctness dependency of the current design (the protection horizon and
/// the age gates are what protect in-flight readers).
pub trait LeaseCheck: Send + Sync {
    /// Return `true` if `key` is protected by an active reader lease and must
    /// not be deleted this pass.
    fn is_protected(&self, key: &str) -> bool;
}

/// The shipped [`LeaseCheck`]: nothing is ever protected, so the
/// consistency-model's "not lease-protected" GC precondition is vacuously
/// satisfied everywhere (plan §5).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoLeases;

impl LeaseCheck for NoLeases {
    fn is_protected(&self, _key: &str) -> bool {
        false
    }
}

/// What one sweep pass over a `(tenant, signal, shard)` deleted, per rule.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Rule 1: record-less `l0/` data objects deleted (orphan GC).
    pub orphans_deleted: usize,
    /// Rule 2: superseded L0 commit records deleted.
    pub superseded_records_deleted: usize,
    /// Rule 2: superseded L0 data objects deleted.
    pub superseded_data_deleted: usize,
    /// Rule 3: unreferenced `l1/` part objects deleted.
    pub unreferenced_parts_deleted: usize,
}

/// Run all three sweep rules over one `(tenant, signal, shard)` and report
/// what each deleted. Stateless and idempotent: a crashed pass re-run from
/// scratch converges (every delete is a no-op if the object is already gone).
///
/// Order: superseded, then unreferenced parts, then orphan GC last. Orphan GC
/// runs last so it mops up any record-less data object a crash left behind
/// mid-superseded-sweep (row 8), rather than racing the same object with the
/// superseded rule in one pass. The rules are independent, so the order only
/// affects which rule's counter claims a crash remnant, never correctness.
pub async fn sweep_shard(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<SweepReport> {
    let (superseded_records_deleted, superseded_data_deleted) =
        sweep_superseded(store, clock, config, lease, tenant, signal, shard).await?;
    let unreferenced_parts_deleted =
        sweep_unreferenced_parts(store, clock, config, lease, tenant, signal, shard).await?;
    let orphans_deleted = sweep_orphans(store, clock, config, lease, tenant, signal, shard).await?;
    Ok(SweepReport {
        orphans_deleted,
        superseded_records_deleted,
        superseded_data_deleted,
        unreferenced_parts_deleted,
    })
}

// --- Rule 1: orphan GC (ADR-0010 §11) --------------------------------------

/// Delete every record-less `l0/` data object older than the orphan age gate,
/// re-verifying commit-record absence with a fresh strongly consistent LIST
/// immediately before each delete. Returns the number deleted.
pub async fn sweep_orphans(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<usize> {
    let now = clock.now_ns();
    let gate = config.orphan_age_gate_ns();
    let referenced = referenced_l0_identities(store, tenant, signal, shard).await?;

    let prefix = l0_data_prefix(tenant, signal, shard)?;
    let objects = list_all(store, &prefix).await?;
    let mut deleted = 0usize;
    for meta in objects {
        let parsed = keys::parse_data_key(&meta.key)?;
        let identity = (parsed.writer_id, parsed.epoch, parsed.seq);
        if referenced.contains(&identity) {
            continue;
        }
        if object_age_ns(now, &meta) <= gate {
            continue;
        }
        if lease.is_protected(&meta.key) {
            continue;
        }
        // Re-verify absence immediately before the delete, via a fresh
        // strongly consistent LIST (ADR-0010 §11): a commit record may have
        // landed for this identity since the first listing.
        let fresh = referenced_l0_identities(store, tenant, signal, shard).await?;
        if fresh.contains(&identity) {
            continue;
        }
        store.delete(&meta.key).await?;
        deleted += 1;
    }
    Ok(deleted)
}

/// The set of L0 commit-record identities `(writer_id, epoch, seq)` present in
/// a shard, across every hour. Read from commit-record *keys* only (no GET):
/// an `l0/` data object whose identity is in this set is referenced. A data
/// object and its commit record share `(writer_id, epoch, seq)`; a leftover
/// with the same identity but a different content hash (a forbidden split
/// brain) is conservatively treated as referenced and never deleted.
async fn referenced_l0_identities(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<HashSet<(Uuid, u64, u64)>> {
    let prefix = keys::commit_shard_prefix(tenant, signal, shard)?;
    let metas = list_all(store, &prefix).await?;
    let mut out = HashSet::new();
    for meta in metas {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(BucketEntry::CommitRecord(pk)) => {
                out.insert((pk.writer_id, pk.epoch, pk.seq));
            }
            Ok(BucketEntry::CompactionRecord(_) | BucketEntry::Tombstone(_)) => {}
            Err(KeyError::UnknownBucketEntryShape(k)) => {
                return Err(MaintainError::UnknownBucketEntry(k));
            }
            Err(e) => return Err(MaintainError::Key(e)),
        }
    }
    Ok(out)
}

// --- Rule 2: superseded-input sweep (ADR-0018) -----------------------------

/// Delete the L0 commit records and data objects named in each horizon-passed
/// compaction record's input list, records before data objects. Returns
/// `(records_deleted, data_deleted)`.
pub async fn sweep_superseded(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<(usize, usize)> {
    let now = clock.now_ns();
    let entries = list_commit_entries(store, tenant, signal, shard).await?;

    let mut records_deleted = 0usize;
    let mut data_deleted = 0usize;
    for (key, entry) in &entries {
        if !matches!(entry, BucketEntry::CompactionRecord(_)) {
            continue;
        }
        let record = get_compaction_record(store, key).await?;
        // Horizon gate anchored on the durable created_unix_ns (plan §5).
        if now
            < record
                .created_unix_ns
                .saturating_add(config.protection_horizon_ns)
        {
            continue;
        }

        // Gather (commit record key, data object key) for every input still
        // present. The data key needs the record's content hash, so each input
        // record is read before it is deleted; an input already gone (a
        // crash-interrupted prior pass) is skipped and its data object, if
        // any, is collected by orphan GC (row 8).
        let mut record_keys: Vec<String> = Vec::new();
        let mut data_keys: Vec<String> = Vec::new();
        for input in &record.inputs {
            let writer_id = Uuid::parse_str(&input.writer_id).map_err(|_| {
                MaintainError::Key(KeyError::InvalidWriterId(input.writer_id.clone()))
            })?;
            let commit_key = keys::commit_key(
                tenant,
                signal,
                shard,
                record.ingest_hour_bucket,
                writer_id,
                input.writer_epoch,
                input.writer_seq,
            )?;
            match store.get(&commit_key, GetRange::Full).await {
                Ok(got) => {
                    let rec = record::decode(&got.data)?;
                    // The record's key must reconstruct to the key we fetched
                    // it at (ADR-0010 §7): a corrupted-but-decodable input
                    // record's own fields, which reconstruct_data_key trusts,
                    // must not name a data object outside the bucket this key
                    // implies (mirrors read::load_inputs).
                    verify_commit_key(&rec, &commit_key)?;
                    let data_key = keys::reconstruct_data_key(&rec)?;
                    record_keys.push(commit_key);
                    data_keys.push(data_key);
                }
                Err(StoreError::NotFound) => {}
                Err(e) => return Err(MaintainError::Store(e)),
            }
        }

        // Records first, then data objects (plan §5, docs/consistency-model.md):
        // a crash between the two phases leaves record-less data (orphan GC),
        // never a record pointing at a deleted object.
        for k in &record_keys {
            if lease.is_protected(k) {
                continue;
            }
            store.delete(k).await?;
            records_deleted += 1;
        }
        for k in &data_keys {
            if lease.is_protected(k) {
                continue;
            }
            store.delete(k).await?;
            data_deleted += 1;
        }
    }
    Ok((records_deleted, data_deleted))
}

// --- Rule 3: unreferenced-part cleanup -------------------------------------

/// Delete every `l1/` object in a bucket that already holds a compaction
/// record but is referenced by none of that bucket's records, once the object
/// is older than the unreferenced-part age gate. Non-reference is re-verified
/// with a fresh strongly consistent LIST immediately before each delete.
/// Returns the number deleted.
pub async fn sweep_unreferenced_parts(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<usize> {
    let now = clock.now_ns();
    let gate = config.unreferenced_part_age_gate_ns();
    let referenced = referenced_l1_parts(store, tenant, signal, shard).await?;

    let prefix = l1_prefix(tenant, signal, shard)?;
    let objects = list_all(store, &prefix).await?;
    let mut deleted = 0usize;
    for meta in objects {
        let parsed = keys::parse_l1_part_key(&meta.key)?;
        // Precondition: a compaction record must already exist for the bucket.
        // Only such buckets appear as keys in `referenced`.
        let Some(bucket_refs) = referenced.get(&parsed.ingest_hour_bucket) else {
            continue;
        };
        if bucket_refs.contains(&meta.key) {
            continue;
        }
        if object_age_ns(now, &meta) <= gate {
            continue;
        }
        if lease.is_protected(&meta.key) {
            continue;
        }
        // Re-verify non-reference immediately before the delete.
        let fresh = referenced_l1_parts(store, tenant, signal, shard).await?;
        match fresh.get(&parsed.ingest_hour_bucket) {
            Some(fs) if !fs.contains(&meta.key) => {}
            _ => continue,
        }
        store.delete(&meta.key).await?;
        deleted += 1;
    }
    Ok(deleted)
}

/// For each bucket (ingest hour) that holds at least one compaction record,
/// the set of L1 part keys those records reference. A bucket absent from the
/// map has no compaction record, so its `l1/` objects fail the
/// unreferenced-part precondition and are never swept by rule 3.
async fn referenced_l1_parts(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<HashMap<u32, HashSet<String>>> {
    let entries = list_commit_entries(store, tenant, signal, shard).await?;
    let mut out: HashMap<u32, HashSet<String>> = HashMap::new();
    for (key, entry) in &entries {
        if !matches!(entry, BucketEntry::CompactionRecord(_)) {
            continue;
        }
        let record = get_compaction_record(store, key).await?;
        let set = out.entry(record.ingest_hour_bucket).or_default();
        for part in &record.parts {
            set.insert(keys::reconstruct_l1_part_key(&record, part)?);
        }
    }
    Ok(out)
}

// --- shared helpers --------------------------------------------------------

/// List a shard's commit prefix and classify every key by shape, failing loud
/// on any unknown shape (plan §3.1). Returns `(key, entry)` pairs across all
/// hours of the shard.
async fn list_commit_entries(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<Vec<(String, BucketEntry)>> {
    let prefix = keys::commit_shard_prefix(tenant, signal, shard)?;
    let metas = list_all(store, &prefix).await?;
    let mut out = Vec::with_capacity(metas.len());
    for meta in metas {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(entry) => out.push((meta.key, entry)),
            Err(KeyError::UnknownBucketEntryShape(k)) => {
                return Err(MaintainError::UnknownBucketEntry(k));
            }
            Err(e) => return Err(MaintainError::Key(e)),
        }
    }
    Ok(out)
}

/// GET, decode, and key-verify a compaction record (ADR-0010 §7).
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

/// Age of an object in nanoseconds from its `last_modified` (ms), against an
/// injected `now_ns`. The object-store contract restricts `last_modified` to
/// exactly this use (GC age checks); it is never used to order commits.
fn object_age_ns(now_ns: i64, meta: &ObjectMeta) -> i64 {
    now_ns.saturating_sub(meta.last_modified_unix_ms.saturating_mul(1_000_000))
}

/// `t/<tenant_hash_hex>/<signal>/l0/<shard>/` -- the prefix covering every L0
/// data object for one `(tenant, signal, shard)`, across all ingest hours (L0
/// data keys are not hour-bucketed, ADR-0010 §1). No public builder exists in
/// ravel-commit for this prefix, so it is constructed here from the same
/// pieces `keys::data_key` uses.
fn l0_data_prefix(tenant: &TenantHash, signal: Signal, shard: u32) -> Result<String> {
    Ok(format!(
        "t/{}/{}/l0/{}/",
        tenant.to_hex(),
        signal.key_prefix(),
        format_shard(shard)?
    ))
}

/// `t/<tenant_hash_hex>/<signal>/l1/<shard>/` -- the prefix covering every L1
/// part object for one `(tenant, signal, shard)`, across all ingest hours.
fn l1_prefix(tenant: &TenantHash, signal: Signal, shard: u32) -> Result<String> {
    Ok(format!(
        "t/{}/{}/{}/{}/",
        tenant.to_hex(),
        signal.key_prefix(),
        keys::L1_DIR,
        format_shard(shard)?
    ))
}

/// The 4-digit shard segment used in every key shape (mirrors ravel-commit's
/// private `format_shard`). Rejects shards past the 4-digit width so a prefix
/// can never silently under-match.
fn format_shard(shard: u32) -> Result<String> {
    if shard > 9999 {
        return Err(MaintainError::Key(KeyError::ShardOutOfRange(shard)));
    }
    Ok(format!("{shard:04}"))
}
