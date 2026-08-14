//! Age-based retention (ADR-0019): the
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

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use ravel_catalog::{DecodedPart, PartLimits, decode_head, decode_part};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum, list_all,
};
use ravel_proto::catalog::v1::{SnapshotHead, SnapshotPartRef};
use ravel_proto::commit::v1::{CommitRecord, CompactionRecord, RetentionTombstone};
use ravel_types::{Signal, TenantHash};

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::compact::{CompactionOutcome, compact_bucket};
use crate::config::{CompactorConfig, RetentionConfig};
use crate::error::{MaintainError, Result};
use crate::read::{BucketListing, list_bucket, verify_commit_key};
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
    /// Tombstone present and horizon elapsed, but the physical sweep was
    /// blocked before deleting anything because the live catalog HEAD snapshot
    /// still reaches this bucket (ADR-0020: "GC must treat reachability from
    /// HEAD-referenced snapshots (within the protection horizon) as a delete
    /// blocker"). Nothing was deleted and the tombstone was left in place, so
    /// bucket-wide exclusion still holds and a later sweep finishes the job
    /// once the fold has dropped the bucket from the snapshot (the fold's
    /// retention-frontier reconcile, crates/ravel-catalog). The [`SnapshotBlock`]
    /// says why the block fired.
    BlockedBySnapshot(SnapshotBlock),
}

/// Why the physical sweep was blocked by HEAD reachability (ADR-0020
/// delete-blocker). Both variants leave the tombstone in place and delete
/// nothing; they are distinguished so each is separately observable in the
/// maintain counters (a persistent [`SnapshotBlock::Unreadable`] is an
/// operator signal that HEAD or a part cannot be read, not the ordinary
/// lagging-fold case).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotBlock {
    /// A decoded snapshot entry names an object physically inside this bucket
    /// (same shard and ingest hour). The ordinary case: the fold has not yet
    /// dropped the tombstoned bucket from the snapshot.
    Named,
    /// HEAD, or a snapshot part covering this bucket's hour, was present but
    /// could not be read (undecodable, checksum/hash mismatch, unsupported
    /// version, or a HEAD-named part that is missing). Blocked fail-closed:
    /// non-reachability cannot be proven from data that cannot be read, and a
    /// wrongly-permitted delete is unrecoverable while a delayed one is not
    /// (deliverable 3).
    Unreadable,
}

/// Per-sweep-pass cache of the catalog HEAD and the snapshot parts it names,
/// so a retention pass that walks many buckets of one `(tenant, signal)` reads
/// HEAD at most once and each covering part at most once, rather than once per
/// bucket (a per-bucket HEAD GET would be an S3-request-cost regression against
/// the live cost-reduction epic, ADR-0076). Created once per
/// [`crate::scan::scan_and_maintain_with_memo`] call and threaded through
/// [`maintain_bucket_with_reach`] into [`physical_sweep`]; the public
/// [`maintain_bucket`] / [`retention_sweep_bucket`] entry points create a fresh
/// one per call.
///
/// The cache is read-once-per-pass, not a durable cache: it is safe precisely
/// because a retention tombstone is irreversible (ADR-0019 decision 2), so a
/// bucket the fold has already dropped from HEAD is never re-added, and a HEAD
/// read at the start of a pass that does NOT name a bucket cannot later start
/// naming it. A HEAD read that DOES name a bucket only ever delays a delete by
/// one pass, the fail-safe direction.
#[derive(Default)]
pub struct SnapshotReachability {
    head: Option<HeadLoad>,
    /// Decoded snapshot parts by object key. `None` = present but unreadable
    /// (fail-closed); `Some` = decoded and usable.
    parts: HashMap<String, Option<Arc<DecodedPart>>>,
}

/// The catalog HEAD as read once for a sweep pass.
enum HeadLoad {
    /// HEAD is absent: no snapshot names anything, so no bucket is blocked
    /// (ADR-0020: the index is a pure optimization; a missing HEAD degrades to
    /// listing).
    Absent,
    /// HEAD is present but undecodable (or a newer format this build cannot
    /// read): fail-closed, every bucket is blocked.
    Unreadable,
    /// HEAD is present and decoded.
    Present(SnapshotHead),
}

/// The result of gating one bucket on HEAD reachability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotGate {
    /// No snapshot entry names an object in this bucket: the sweep may proceed.
    Clear,
    /// The sweep is blocked; the reason distinguishes the counters.
    Blocked(SnapshotBlock),
}

impl SnapshotReachability {
    /// A fresh, empty cache for one sweep pass.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the live HEAD snapshot still reaches an object inside `bucket`
    /// (ADR-0020 delete-blocker). HEAD and each covering part are loaded at
    /// most once per pass and cached. HEAD absent -> [`SnapshotGate::Clear`];
    /// HEAD or any covering part unreadable -> fail-closed
    /// [`SnapshotBlock::Unreadable`]; a decoded entry naming this bucket's
    /// shard+hour -> [`SnapshotBlock::Named`].
    async fn bucket_gate(
        &mut self,
        store: &dyn ObjectStoreBackend,
        bucket: &Bucket,
    ) -> Result<SnapshotGate> {
        // Load HEAD once, then take the covering part refs out (owned clones)
        // so the borrow of `self.head` is released before the part loads below
        // borrow `self` mutably.
        let covering: Vec<SnapshotPartRef> = match self
            .ensure_head(store, &bucket.tenant_hash, bucket.signal)
            .await?
        {
            HeadStatus::Absent => return Ok(SnapshotGate::Clear),
            HeadStatus::Unreadable => {
                return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable));
            }
            HeadStatus::Present => match &self.head {
                Some(HeadLoad::Present(head)) => head
                    .parts
                    .iter()
                    .filter(|p| {
                        p.min_hour <= bucket.ingest_hour_bucket
                            && bucket.ingest_hour_bucket <= p.watermark_hour
                    })
                    .cloned()
                    .collect(),
                // Unreachable after `ensure_head` returned `Present`; block
                // fail-closed rather than panic (no unwrap/expect on a
                // production path).
                _ => return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable)),
            },
        };

        for part_ref in &covering {
            match self.ensure_part(store, part_ref).await? {
                // A covering part could not be read: cannot prove
                // non-reachability, fail closed (deliverable 3).
                None => return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable)),
                Some(part) => {
                    // An object is physically inside this bucket iff its entry's
                    // shard and ingest hour match the bucket. Any such entry
                    // means the snapshot still names an object the sweep would
                    // delete.
                    if part.entries.iter().any(|e| {
                        e.shard == bucket.shard && e.ingest_hour_bucket == bucket.ingest_hour_bucket
                    }) {
                        return Ok(SnapshotGate::Blocked(SnapshotBlock::Named));
                    }
                }
            }
        }
        Ok(SnapshotGate::Clear)
    }

    /// Load HEAD once for the pass, caching the result. Returns a lightweight
    /// status; the decoded HEAD itself stays in `self.head` for the covering
    /// part-ref extraction in [`Self::bucket_gate`].
    async fn ensure_head(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
    ) -> Result<HeadStatus> {
        if self.head.is_none() {
            let head_key = catalog_head_key(tenant, signal);
            let load = match store.get(&head_key, GetRange::Full).await {
                Ok(got) => match decode_head(got.data.as_ref()) {
                    Ok(head) => HeadLoad::Present(head),
                    Err(err) => {
                        // Present but undecodable/newer: fail-closed. Cannot
                        // prove non-reachability from a HEAD we cannot read.
                        tracing::warn!(
                            error = %err,
                            key = %head_key,
                            "retention sweep: catalog HEAD failed to decode; blocking deletes \
                             fail-closed this pass rather than proving non-reachability from an \
                             unreadable HEAD"
                        );
                        HeadLoad::Unreadable
                    }
                },
                Err(StoreError::NotFound) => HeadLoad::Absent,
                Err(err) => return Err(MaintainError::Store(err)),
            };
            self.head = Some(load);
        }
        Ok(match &self.head {
            Some(HeadLoad::Absent) => HeadStatus::Absent,
            Some(HeadLoad::Unreadable) => HeadStatus::Unreadable,
            Some(HeadLoad::Present(_)) => HeadStatus::Present,
            None => HeadStatus::Unreadable,
        })
    }

    /// Load, verify (blake3 against the HEAD ref), and decode one snapshot part
    /// once per pass, caching the result. `Ok(None)` is the fail-closed
    /// "present but unreadable" case (a missing HEAD-named part, a hash
    /// mismatch, or a decode failure); a transient store fault propagates.
    async fn ensure_part(
        &mut self,
        store: &dyn ObjectStoreBackend,
        part_ref: &SnapshotPartRef,
    ) -> Result<Option<Arc<DecodedPart>>> {
        if let Some(cached) = self.parts.get(&part_ref.key) {
            return Ok(cached.clone());
        }
        let load: Option<Arc<DecodedPart>> = match store.get(&part_ref.key, GetRange::Full).await {
            Ok(got) => {
                let data = got.data;
                if blake3::hash(&data).as_bytes().as_slice() != part_ref.blake3.as_slice() {
                    tracing::warn!(
                        key = %part_ref.key,
                        "retention sweep: snapshot part hash mismatch; blocking deletes fail-closed"
                    );
                    None
                } else {
                    let limits = PartLimits {
                        max_snapshot_part_bytes: ravel_catalog::DEFAULT_MAX_SNAPSHOT_PART_BYTES,
                    };
                    match decode_part(data.as_ref(), &limits) {
                        Ok(part) => Some(Arc::new(part)),
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                key = %part_ref.key,
                                "retention sweep: snapshot part failed to decode; blocking deletes \
                                 fail-closed"
                            );
                            None
                        }
                    }
                }
            }
            // HEAD names a part that is not present. Anomalous (a HEAD-named
            // part is only deleted after HEAD stops naming it plus the
            // horizon): cannot read its entries, so fail closed.
            Err(StoreError::NotFound) => {
                tracing::warn!(
                    key = %part_ref.key,
                    "retention sweep: HEAD-named snapshot part is missing; blocking deletes \
                     fail-closed"
                );
                None
            }
            Err(err) => return Err(MaintainError::Store(err)),
        };
        self.parts.insert(part_ref.key.clone(), load.clone());
        Ok(load)
    }
}

/// Lightweight status returned by [`SnapshotReachability::ensure_head`], so the
/// decoded HEAD can stay owned in the cache while the caller decides how to
/// proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadStatus {
    Absent,
    Unreadable,
    Present,
}

/// `t/<tenant_hash_hex>/catalog/<signal>/HEAD` -- the mutable head pointer for
/// one `(tenant, signal)` (docs/catalog-and-mvcc.md key layout, a frozen
/// contract). No public builder is exported from ravel-catalog, so it is
/// constructed here from the same pieces, matching `sweep.rs`'s
/// `catalog_head_key` precedent.
fn catalog_head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

/// Run one retention pass over a single sealed bucket (ADR-0019):
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
    let mut reach = SnapshotReachability::new();
    retention_sweep_bucket_with_reach(&mut reach, store, clock, config, retention, lease, bucket)
        .await
}

/// [`retention_sweep_bucket`] with a caller-owned [`SnapshotReachability`]
/// cache persisted across the buckets of one sweep pass, so the HEAD-
/// reachability gate reads HEAD at most once and each covering snapshot part at
/// most once per pass rather than once per bucket (ADR-0076 request cost). The
/// public [`retention_sweep_bucket`] wraps this with a fresh per-call cache;
/// [`crate::scan::scan_and_maintain_with_memo`] owns one cache for the whole
/// per-(tenant, signal, shard) pass and calls this directly.
#[allow(clippy::too_many_arguments)]
pub async fn retention_sweep_bucket_with_reach(
    reach: &mut SnapshotReachability,
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
            return physical_sweep(
                reach,
                store,
                lease,
                bucket,
                &listing,
                tombstone_key,
                config.dry_run,
            )
            .await;
        }
        return Ok(RetentionOutcome::Tombstoned);
    }

    // Not tombstoned: evaluate expiry from the bucket's records (no footer
    // reads).
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

    write_tombstone(store, bucket, now, window_ns, &listing, config.dry_run).await?;
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
    let mut reach = SnapshotReachability::new();
    maintain_bucket_with_reach(&mut reach, store, clock, config, retention, lease, bucket).await
}

/// [`maintain_bucket`] with a caller-owned [`SnapshotReachability`] cache
/// shared across the buckets of one sweep pass (ADR-0076 request cost, see
/// [`retention_sweep_bucket_with_reach`]). The public [`maintain_bucket`] wraps
/// this with a fresh per-call cache.
#[allow(clippy::too_many_arguments)]
pub async fn maintain_bucket_with_reach(
    reach: &mut SnapshotReachability,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    retention: &RetentionConfig,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
) -> Result<(RetentionOutcome, Option<CompactionOutcome>)> {
    let outcome =
        retention_sweep_bucket_with_reach(reach, store, clock, config, retention, lease, bucket)
            .await?;
    let compaction = match outcome {
        // The bucket is (or is being) retired, or its delete is blocked by a
        // still-reaching snapshot: never compact it.
        RetentionOutcome::Tombstoned
        | RetentionOutcome::Swept
        | RetentionOutcome::SweptPartial
        | RetentionOutcome::BlockedBySnapshot(_) => None,
        RetentionOutcome::NoPolicy | RetentionOutcome::NotSealed | RetentionOutcome::NotExpired => {
            Some(compact_bucket(store, clock, config, bucket).await?)
        }
    };
    Ok((outcome, compaction))
}

/// The maximum `max_event_ts_ns` across a bucket's L0 commit records and
/// compaction-record parts. `None` when the bucket holds no records.
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
    dry_run: bool,
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
    // Dry-run: the tombstone and its key are assembled identically, but the
    // durable PUT that would tombstone the bucket is skipped.
    if dry_run {
        return Ok(());
    }
    match store.put(&key, payload.into(), opts).await {
        Ok(_) | Err(StoreError::AlreadyExists) => Ok(()),
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// Horizon-gated physical sweep (ADR-0019 decision 4). Deletes in the
/// fixed order L0 commit records, compaction records, L0 data objects, L1
/// parts, then the tombstone last, and only after a verifying LIST shows the
/// bucket's commit prefix holds only the tombstone and its `l1/` prefix is
/// empty. Any residue leaves the tombstone in place.
async fn physical_sweep(
    reach: &mut SnapshotReachability,
    store: &dyn ObjectStoreBackend,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
    listing: &BucketListing,
    tombstone_key: &str,
    dry_run: bool,
) -> Result<RetentionOutcome> {
    // HEAD-reachability gate (ADR-0020 delete-blocker): before deleting
    // anything, refuse if the live catalog HEAD snapshot still names an object
    // inside this bucket, or if HEAD/a covering part cannot be read (fail
    // closed). This is the safety net for a lagging or stopped folder; the
    // fold's retention-frontier reconcile (crates/ravel-catalog) is what makes
    // the block clear on its own by dropping the tombstoned bucket promptly.
    // HEAD absent is NOT a block: no snapshot names anything, so the sweep
    // proceeds (ADR-0020: the index is a pure optimization). The tombstone is
    // left in place on a block, so bucket-wide exclusion holds and a later
    // pass finishes once the fold has caught up.
    match reach.bucket_gate(store, bucket).await? {
        SnapshotGate::Clear => {}
        SnapshotGate::Blocked(reason) => {
            return Ok(RetentionOutcome::BlockedBySnapshot(reason));
        }
    }

    // Resolve the L0 data-object keys BEFORE deleting the commit records that
    // name them (records are deleted first).
    let mut l0_data_keys: Vec<String> = Vec::new();
    for key in &listing.commit_keys {
        match store.get(key, GetRange::Full).await {
            Ok(got) => {
                let record = record::decode(&got.data)?;
                // The record's key must reconstruct to the key we listed it at
                // (ADR-0010 §7): a corrupted-but-decodable record's own fields,
                // which reconstruct_data_key trusts, must not name an object
                // outside the bucket this key implies.
                verify_commit_key(&record, key)?;
                l0_data_keys.push(keys::reconstruct_data_key(&record)?);
            }
            Err(StoreError::NotFound) => {}
            Err(e) => return Err(MaintainError::Store(e)),
        }
    }
    // Discover L1 parts by a fresh LIST of the bucket's own l1/ prefix (the
    // same LIST bucket_is_empty_but_tombstone uses), not by reconstructing
    // keys from compaction records. This makes the L1 delete independent of
    // whether the compaction record that named those parts still exists, so a
    // crash between the compaction-record delete and the L1-part delete still
    // converges: the next pass re-lists whatever l1/ objects are physically
    // present and deletes them (ADR-0019 decision 4; docs/consistency-model.md
    // targets "everything in a tombstoned bucket", not "the parts its records
    // name").
    let l1_prefix = l1_bucket_prefix(bucket);
    let l1_part_keys: Vec<String> = list_all(store, &l1_prefix)
        .await?
        .into_iter()
        .map(|meta| meta.key)
        .collect();

    // Deletion order (docs/consistency-model.md "Deletion and GC", ADR-0019
    // decision 4): records, then data objects, then L1 parts, tombstone last.
    delete_all(store, lease, &listing.commit_keys, dry_run).await?;
    delete_all(store, lease, &listing.compaction_record_keys, dry_run).await?;
    delete_all(store, lease, &l0_data_keys, dry_run).await?;
    delete_all(store, lease, &l1_part_keys, dry_run).await?;

    // Verify the bucket is empty before deleting the tombstone: the commit
    // prefix must contain only the tombstone, and the l1/ prefix must be empty.
    // Under dry-run every delete above was skipped, so the bucket still holds
    // its objects and this check reports SweptPartial: the honest dry-run
    // answer is "the horizon has elapsed and a real run would sweep now", not
    // "the bucket is empty". No CLI path dry-runs retention (the service
    // always runs with dry_run == false); this guard exists so config.dry_run
    // is honored everywhere it is threaded.
    if !bucket_is_empty_but_tombstone(store, bucket).await? {
        return Ok(RetentionOutcome::SweptPartial);
    }
    if !lease.is_protected(tombstone_key) {
        if !dry_run {
            store.delete(tombstone_key).await?;
        }
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
    dry_run: bool,
) -> Result<()> {
    for key in keys {
        if lease.is_protected(key) {
            continue;
        }
        if !dry_run {
            store.delete(key).await?;
        }
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
    let record = record::decode(&got.data)?;
    // The record's key must reconstruct to the key we listed it at (ADR-0010
    // §7): the same identity discipline load_inputs applies to every input.
    verify_commit_key(&record, key)?;
    Ok(record)
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
