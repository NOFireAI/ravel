//! The sweeper: one component, four eligibility rules
//! (docs/compaction-retention-plan.md §5, docs/consistency-model.md "Deletion
//! and GC"). This is the first implementation of any deletion in Ravel.
//!
//! 1. **Orphan GC** (ADR-0010 §11): an `l0/` data object with no commit
//!    record, older than `grace + max_flush_lifetime`. The writer interlock
//!    (a writer abandons any flush older than `max_flush_lifetime` and never
//!    publishes it afterward) is what makes this safe: a record-less object
//!    that old can never gain a commit record later, so deleting it cannot
//!    orphan a future reader. Commit-record absence is re-verified with one
//!    fresh strongly consistent LIST shared by every candidate in the pass
//!    (ADR-0048 decision 5), then gated by a mass-orphan circuit breaker
//!    (ADR-0048 decision 4): a pass that would delete at least
//!    `orphan_breaker_min_count` candidates AND more than
//!    `orphan_breaker_max_ratio` of the shard's listed L0 objects deletes
//!    nothing and halts, because that shape is the signature of an
//!    out-of-band commit-record loss, not routine cleanup.
//! 2. **Superseded-input sweep** (ADR-0018): the L0 commit records and data
//!    objects a compaction record names in its input list, once
//!    `now >= record.created_unix_ns + protection_horizon`. Records are
//!    deleted before data objects, so a crash mid-sweep never leaves a commit
//!    record pointing at a deleted data object visible to a resolver.
//! 3. **Unreferenced-part cleanup**: an `l1/` object referenced by no
//!    compaction record in its bucket, once the object is older than `grace +
//!    max_compaction_lifetime` and one of two branch conditions holds:
//!    (a) a compaction record already exists for the bucket, so any object no
//!    record names is a leftover; or (b) a retention tombstone exists for the
//!    bucket and no compaction record does, which makes any future compaction
//!    impossible (`compact_bucket`'s tombstone gate returns before building or
//!    publishing, ADR-0019), so every record-less `l1/` object in the bucket
//!    can never be re-referenced by a legal future publish (issue #273). A
//!    bucket with neither a record nor a tombstone keeps its record-less
//!    parts: a future compaction over the same sealed, content-addressed input
//!    set will republish the identical keys and name them. The exact branch
//!    condition is re-verified with a fresh strongly consistent LIST
//!    immediately before each delete.
//! 4. **Idempotency marker sweep** (ADR-0051 §5): a `t/<tenant_hash>/<signal>/
//!    idem/` marker (logs and spans only, ravel-ingest's post-flush dedup
//!    cache) whose `<ingest_hour>` -- encoded in the key name itself, not read
//!    from `last_modified` -- is more than `idem_dedup_window_hours` behind
//!    the clock's current ingest-hour bucket. Stateless and signal-generic
//!    like the other three, but its age signal is the pinned ingest hour a
//!    retry could still land in, not object age, because a marker's whole
//!    purpose is keyed to that hour, not to when it happened to get written.
//!    A key under the prefix that fails to parse as
//!    `<keyhash32>.<ingest_hour>.idm` is logged and skipped, never deleted and
//!    never fatal: the prefix is additive, so the `c/`-prefix fail-loud
//!    unknown-shape rule (rules 1-3) does not apply to it.
//!
//! All four are **signal-generic**: rules 1-3 operate only on commit-record,
//! compaction-record, and object *keys* plus store `last_modified`, and rule 4
//! only on marker keys, never on a segment byte, so nothing here needs to know
//! RSEG from RLOG. All four are stateless per pass, restartable from zero, and
//! every delete is idempotent (the object-store contract makes deleting a
//! missing key a success). The clock is always injected; rules 1-3 read object
//! age from `last_modified` (which the object-store contract restricts to
//! exactly GC age checks), while rule 4 reads it from the ingest hour encoded
//! in the marker's own key.
//!
//! The [`LeaseCheck`] hook is consulted before every delete in all four
//! rules. It ships as the no-op [`NoLeases`] ("nothing is ever protected"):
//! the consistency-model's "not lease-protected" precondition is then
//! vacuously satisfied everywhere. It is a seam for future slow-consumer work
//! (plan §5, Q3), not live logic; no lease machinery is built behind it.

use std::collections::{HashMap, HashSet};

use prost::Message;
use ravel_commit::keys::{self, BucketEntry, KeyError, parse_ingest_hour_string};
use ravel_commit::record;
use ravel_object_store::{GetRange, ObjectMeta, ObjectStoreBackend, StoreError, list_all};
use ravel_proto::commit::v1::{CompactionRecord, RewriteRecord};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::{CompactorConfig, NS_PER_HOUR};
use crate::error::{MaintainError, Result};
use crate::read::verify_commit_key;

use ravel_ingest::{IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS, MARKER_SUFFIX};

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
    /// Rule 1's mass-orphan circuit breaker tripped this pass (ADR-0048
    /// decision 4): `orphans_deleted` is `0` and `orphans_withheld` carries
    /// what would have been deleted. Rules 2 and 3 above are unaffected and
    /// still ran, since they are anchored on durable records an operator or
    /// compactor deliberately wrote, never on record absence.
    pub orphan_breaker_tripped: bool,
    /// Orphan candidates withheld by a tripped breaker this pass. Always `0`
    /// when `orphan_breaker_tripped` is `false`.
    pub orphans_withheld: usize,
    /// This pass deleted orphans despite exceeding the breaker's threshold,
    /// because `CompactorConfig::force_orphan_gc` overrode it (ADR-0048
    /// decision 4's one-shot operator override). Always `false` when
    /// `orphan_breaker_tripped` is `true`.
    pub orphan_breaker_overridden: bool,
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
    let (orphans_deleted, orphan_breaker_tripped, orphans_withheld, orphan_breaker_overridden) =
        match sweep_orphans(store, clock, config, lease, tenant, signal, shard).await {
            Ok(outcome) => (outcome.deleted, false, 0, outcome.breaker_overridden),
            Err(MaintainError::OrphanBreakerTripped { candidates, .. }) => {
                (0, true, candidates, false)
            }
            Err(e) => return Err(e),
        };
    Ok(SweepReport {
        orphans_deleted,
        superseded_records_deleted,
        superseded_data_deleted,
        unreferenced_parts_deleted,
        orphan_breaker_tripped,
        orphans_withheld,
        orphan_breaker_overridden,
    })
}

// --- Rule 1: orphan GC (ADR-0010 §11) --------------------------------------

/// What one orphan-GC pass did (ADR-0048 decisions 4 and 5). A pass that
/// trips the mass-orphan breaker without an override returns
/// [`MaintainError::OrphanBreakerTripped`] instead of this type, and deletes
/// nothing; [`sweep_shard`] folds that error into [`SweepReport`]'s breaker
/// fields for callers that run the whole shard.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrphanSweepOutcome {
    /// Record-less `l0/` data objects deleted this pass.
    pub deleted: usize,
    /// This pass exceeded the breaker's threshold but proceeded anyway
    /// because [`CompactorConfig::force_orphan_gc`] was set (ADR-0048
    /// decision 4's one-shot operator override).
    pub breaker_overridden: bool,
}

/// Delete every record-less `l0/` data object older than the orphan age
/// gate. Three phases (ADR-0048 decisions 4 and 5): (a) candidate selection
/// over one listing of the shard's L0 data objects, filtered by the
/// commit-record identities already present, the age gate, and lease
/// protection; (b) one fresh strongly consistent LIST of the commit prefix,
/// shared by every candidate, dropping any whose identity now appears
/// (replacing the old per-candidate full-shard LIST, the dominant request
/// cost of a sweep); (c) the mass-orphan circuit breaker gate; (d) delete.
/// A tripped, non-overridden breaker returns
/// [`MaintainError::OrphanBreakerTripped`] before phase (d), so the pass is
/// all-or-nothing: either every surviving candidate is deleted, or none are.
pub async fn sweep_orphans(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<OrphanSweepOutcome> {
    let now = clock.now_ns();
    let gate = config.orphan_age_gate_ns();

    // Phase (a): candidate selection over one listing of the shard's L0 data
    // objects, checked against the commit-record identities from one initial
    // commit-prefix LIST.
    let prefix = l0_data_prefix(tenant, signal, shard)?;
    let objects = list_all(store, &prefix).await?;
    let l0_objects_listed = objects.len();
    let referenced = referenced_l0_identities(store, tenant, signal, shard).await?;

    let mut candidates: Vec<(ObjectMeta, (Uuid, u64, u64))> = Vec::new();
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
        candidates.push((meta, identity));
    }

    // Phase (b): one fresh, batched re-verify LIST of the commit prefix,
    // shared by every candidate this pass (ADR-0048 decision 5): a commit
    // record may have landed for a candidate's identity since the first
    // listing. Skipped when there is nothing to re-verify.
    if !candidates.is_empty() {
        let fresh = referenced_l0_identities(store, tenant, signal, shard).await?;
        candidates.retain(|(_, identity)| !fresh.contains(identity));
    }

    // Phase (c): the mass-orphan circuit breaker (ADR-0048 decision 4). Both
    // conditions must hold: a tiny shard's small orphan count never trips on
    // ratio alone, and any genuinely mass orphan population trips regardless
    // of shard size.
    let candidate_count = candidates.len();
    let would_trip = candidate_count >= config.orphan_breaker_min_count
        && (candidate_count as f64) > config.orphan_breaker_max_ratio * (l0_objects_listed as f64);

    if would_trip && !config.force_orphan_gc {
        return Err(MaintainError::OrphanBreakerTripped {
            tenant_hash: tenant.to_hex(),
            signal: signal.key_prefix().to_string(),
            shard,
            candidates: candidate_count,
            l0_objects_listed,
            min_count: config.orphan_breaker_min_count,
            max_ratio: config.orphan_breaker_max_ratio,
        });
    }

    // Phase (d): delete. All-or-nothing: phase (c) already returned if the
    // pass should delete zero.
    for (meta, _) in &candidates {
        if !config.dry_run {
            store.delete(&meta.key).await?;
        }
    }

    Ok(OrphanSweepOutcome {
        deleted: candidate_count,
        breaker_overridden: would_trip && config.force_orphan_gc,
    })
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
            // Only L0 commit identities are collected here; a compaction,
            // rewrite (ADR-0064), or tombstone record contributes none.
            Ok(
                BucketEntry::CompactionRecord(_)
                | BucketEntry::RewriteRecord(_)
                | BucketEntry::Tombstone(_),
            ) => {}
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
            if !config.dry_run {
                store.delete(k).await?;
            }
            records_deleted += 1;
        }
        for k in &data_keys {
            if lease.is_protected(k) {
                continue;
            }
            if !config.dry_run {
                store.delete(k).await?;
            }
            data_deleted += 1;
        }
    }
    Ok((records_deleted, data_deleted))
}

// --- Rule 3: unreferenced-part cleanup -------------------------------------

/// Delete every `l1/` object older than the unreferenced-part age gate that a
/// legal future publish can never name, re-verifying the exact branch
/// condition with a fresh strongly consistent LIST immediately before each
/// delete. Two branches make an object collectable (see [`PartBranch`]): its
/// bucket holds a compaction record and no record references it, or its bucket
/// holds a retention tombstone and no compaction record (issue #273). A bucket
/// with neither is left alone: its record-less parts belong to a future
/// compaction that will republish the identical content-addressed keys.
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
    let (referenced, tombstoned) = bucket_reference_map(store, tenant, signal, shard).await?;

    let prefix = l1_prefix(tenant, signal, shard)?;
    let objects = list_all(store, &prefix).await?;
    let mut deleted = 0usize;
    for meta in objects {
        let parsed = keys::parse_l1_part_key(&meta.key)?;
        let bucket = parsed.ingest_hour_bucket;
        let Some(branch) = classify_part(&meta.key, bucket, &referenced, &tombstoned) else {
            continue;
        };
        if object_age_ns(now, &meta) <= gate {
            continue;
        }
        if lease.is_protected(&meta.key) {
            continue;
        }
        // Re-verify the exact branch condition immediately before the delete,
        // via a fresh strongly consistent LIST. Requiring the same branch (not
        // merely "still collectable") preserves the record-present rule's old
        // skip-when-the-bucket-is-absent-from-the-fresh-map behavior: if the
        // bucket's compaction record vanished between the two listings, the
        // fresh classification is no longer `UnreferencedWithRecord`, so the
        // delete is skipped.
        let (fresh_ref, fresh_tomb) = bucket_reference_map(store, tenant, signal, shard).await?;
        if classify_part(&meta.key, bucket, &fresh_ref, &fresh_tomb) != Some(branch) {
            continue;
        }
        if !config.dry_run {
            store.delete(&meta.key).await?;
        }
        deleted += 1;
    }
    Ok(deleted)
}

/// Why an `l1/` object is a rule-3 deletion candidate. The pre-delete
/// re-verify re-checks the exact condition of the branch that first admitted
/// the object, never a weaker "still collectable somehow" test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartBranch {
    /// The bucket holds at least one compaction record and none of them names
    /// this object: a leftover (a losing/superseded build's part, or a part
    /// whose split boundary changed). Safe because a sealed bucket's input set
    /// is frozen, so no record will ever start naming it.
    UnreferencedWithRecord,
    /// The bucket holds a retention tombstone and no compaction record. The
    /// tombstone makes any future compaction impossible (`compact_bucket`
    /// returns `Tombstoned` before it builds or publishes, ADR-0019), so no
    /// legal future publish can name this object (issue #273).
    TombstonedRecordless,
}

/// Classify one `l1/` object for rule 3, or `None` if it must not be swept.
/// A bucket with a compaction record protects the parts its records name and
/// exposes the rest ([`PartBranch::UnreferencedWithRecord`]); a bucket with a
/// tombstone but no record exposes every part ([`PartBranch::TombstonedRecordless`]);
/// a bucket with neither exposes nothing (a future compaction may still name
/// its record-less parts).
fn classify_part(
    key: &str,
    bucket: u32,
    referenced: &HashMap<u32, HashSet<String>>,
    tombstoned: &HashSet<u32>,
) -> Option<PartBranch> {
    match referenced.get(&bucket) {
        Some(refs) if refs.contains(key) => None,
        Some(_) => Some(PartBranch::UnreferencedWithRecord),
        None if tombstoned.contains(&bucket) => Some(PartBranch::TombstonedRecordless),
        None => None,
    }
}

/// One LIST of the shard's commit prefix, reduced to what rule 3 needs: for
/// each bucket that holds at least one compaction record, the set of L1 part
/// keys those records reference; and the set of buckets that hold a retention
/// tombstone. A bucket in neither collection has no compaction record and no
/// tombstone, so its `l1/` objects are never swept by rule 3 (a future
/// compaction may still publish a record naming them; issue #273).
async fn bucket_reference_map(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<(HashMap<u32, HashSet<String>>, HashSet<u32>)> {
    let entries = list_commit_entries(store, tenant, signal, shard).await?;
    let mut referenced: HashMap<u32, HashSet<String>> = HashMap::new();
    let mut tombstoned: HashSet<u32> = HashSet::new();
    for (key, entry) in &entries {
        match entry {
            BucketEntry::CompactionRecord(_) => {
                let record = get_compaction_record(store, key).await?;
                let set = referenced.entry(record.ingest_hour_bucket).or_default();
                for part in &record.parts {
                    set.insert(keys::reconstruct_l1_part_key(&record, part)?);
                }
            }
            // A selective-erasure rewrite record (ADR-0064 decision 3) names
            // live L1 output parts exactly as a compaction record does. They
            // must be marked referenced, or the unreferenced-part GC would
            // delete an erased subject's surviving rewritten data.
            BucketEntry::RewriteRecord(_) => {
                let record = get_rewrite_record(store, key).await?;
                let set = referenced.entry(record.ingest_hour_bucket).or_default();
                for part in &record.parts {
                    set.insert(keys::reconstruct_rewrite_part_key(&record, part)?);
                }
            }
            BucketEntry::Tombstone(pk) => {
                tombstoned.insert(pk.ingest_hour_bucket);
            }
            BucketEntry::CommitRecord(_) => {}
        }
    }
    Ok((referenced, tombstoned))
}

// --- Rule 4: idempotency marker sweep (ADR-0051 §5) ------------------------

/// What one idempotency-marker sweep pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IdemSweepOutcome {
    /// Markers past the dedup window, deleted (or, under `dry_run`, that
    /// would have been).
    pub deleted: usize,
    /// Markers within the dedup window, left alone.
    pub kept: usize,
    /// Keys under the `idem/` prefix that did not parse as
    /// `<keyhash32>.<ingest_hour>.idm` and were skipped without deleting or
    /// erroring (the `idem/` prefix is additive and not subject to the
    /// fail-loud unknown-key rule the `c/` prefix uses, ADR-0051 §5 /
    /// docs/catalog-and-mvcc.md).
    pub skipped_malformed: usize,
}

/// Delete every idempotency marker under `t/<tenant_hash>/<signal>/idem/`
/// (ADR-0051 §5) whose `<ingest_hour>` is more than
/// `config.idem_dedup_window_hours` plus [`IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS`]
/// behind the clock's current ingest-hour bucket. The extra margin mirrors
/// `ravel_ingest::idempotency::read_marker`'s own forward-skew tolerance on
/// the read side: without it, a sweeper process whose clock leads an ingest
/// node's by up to that many hours could reap a marker the read path would
/// still honor, ahead of a legitimate retry replaying it (fail-open per
/// ADR-0051, not data loss, but a real gap against this rule's promise that
/// a marker the read path would still honor is never swept out from under
/// it). One LIST of the coarse `idem/` prefix -- coarser than
/// `ravel_ingest::idempotency::read_marker`'s per-key-hash prefix, since the
/// sweep has no client key to scope by and must cover every marker in the
/// signal -- then a per-key age check and delete. A key that does not parse
/// as `<keyhash32>.<ingest_hour>.idm` is logged and skipped, never deleted and
/// never a fatal error: the prefix is additive and no dual-reader question
/// exists for it (unlike the `c/` prefix's fail-loud unknown-shape rule).
pub async fn sweep_idempotency_markers(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<IdemSweepOutcome> {
    let now = clock.now_ns();
    let now_hour = u32::try_from(now.div_euclid(NS_PER_HOUR)).map_err(|_| {
        MaintainError::Invariant(format!("clock reading {now} out of hour-bucket range"))
    })?;
    let min_hour = now_hour
        .saturating_sub(config.idem_dedup_window_hours)
        .saturating_sub(IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS);

    let prefix = idem_prefix(tenant, signal);
    let objects = list_all(store, &prefix).await?;

    let mut deleted = 0usize;
    let mut kept = 0usize;
    let mut skipped_malformed = 0usize;
    for meta in objects {
        let Some(hour) = parse_marker_hour(&meta.key, &prefix) else {
            tracing::warn!(
                key = %meta.key,
                "idempotency sweep: marker key does not parse as <keyhash32>.<ingest_hour>.idm, skipping"
            );
            skipped_malformed += 1;
            continue;
        };

        if hour >= min_hour {
            kept += 1;
            continue;
        }
        if lease.is_protected(&meta.key) {
            kept += 1;
            continue;
        }
        if !config.dry_run {
            store.delete(&meta.key).await?;
        }
        deleted += 1;
    }

    Ok(IdemSweepOutcome {
        deleted,
        kept,
        skipped_malformed,
    })
}

/// `t/<tenant_hash_hex>/<signal>/idem/` -- the prefix covering every
/// idempotency marker for one `(tenant, signal)`, across every client key and
/// ingest hour (ADR-0051 §5, docs/catalog-and-mvcc.md). Coarser than
/// `ravel_ingest::idempotency`'s own (private) per-key-hash prefix builder,
/// which this sweep cannot call (it has no client key to scope by) and does
/// not need to: it reconstructs the same key-layout convention directly.
fn idem_prefix(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/{}/idem/", tenant.to_hex(), signal.key_prefix())
}

/// Parse a listed marker key's `<ingest_hour>` back to its hour bucket, or
/// `None` if the key (with `prefix` stripped) does not match
/// `<keyhash32>.<ingest_hour>.idm`: either segment failing its own shape
/// check is a skip, never a delete. `keyhash32` and the ingest-hour string
/// both contain no `.`, so splitting on the last `.` before the `.idm` suffix
/// isolates the hour segment unambiguously.
fn parse_marker_hour(key: &str, prefix: &str) -> Option<u32> {
    let basename = key.strip_prefix(prefix)?;
    let rest = basename.strip_suffix(&format!(".{MARKER_SUFFIX}"))?;
    let (keyhash, hour_text) = rest.rsplit_once('.')?;
    if !is_keyhash32(keyhash) {
        return None;
    }
    parse_ingest_hour_string(hour_text).ok()
}

/// `true` if `s` is exactly 32 lowercase ASCII hex digits: the shape
/// `ravel_ingest::idempotency::keyhash32` always produces. Rejects anything
/// else -- wrong length, uppercase, non-hex, or (since `/` is never a hex
/// digit) a key with an extra path segment before the hour -- so a
/// non-marker object that merely ends in `.<ingest_hour>.idm` is skipped,
/// never deleted (docs/catalog-and-mvcc.md, ADR-0051 §5).
fn is_keyhash32(s: &str) -> bool {
    s.len() == 32
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
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

/// GET, decode, validate, and key-verify a rewrite record (ADR-0064 decision
/// 3, ADR-0010 §7). `decode_rewrite` also re-verifies the record's own
/// `input_set_hash` and `superseded_record_key` bucket-match on decode.
async fn get_rewrite_record(store: &dyn ObjectStoreBackend, key: &str) -> Result<RewriteRecord> {
    let got = store.get(key, GetRange::Full).await?;
    let record = ravel_commit::erasure::decode_rewrite(got.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("rewrite record decode failed: {e}")))?;
    keys::verify_rewrite_record_key(&record, key)?;
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use bytes::Bytes;
    use ravel_ingest::{IdempotencyReceipt, LookupOutcome, marker_key, read_marker, write_marker};
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantId;

    use super::*;
    use crate::clock::FixedClock;

    fn tenant() -> TenantHash {
        TenantHash([0u8; 16])
    }

    /// A record-less `l0/` data object at a unique identity: no commit record
    /// is ever written for it, so it is an orphan candidate as soon as it
    /// clears the age gate.
    async fn put_orphan(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        seq: u64,
    ) {
        let writer_id = Uuid::from_u128(u128::from(seq) + 1);
        let content_hash = [7u8; 32];
        let key = keys::data_key(tenant, signal, shard, writer_id, 1, seq, &content_hash)
            .expect("valid data key");
        store
            .put(&key, Bytes::new(), PutOptions::default())
            .await
            .expect("seed put");
    }

    /// `MemoryStore`'s fake clock defaults to `0`, so every seeded object's
    /// `last_modified` is `0`; setting the injected clock just past the
    /// orphan age gate makes every seeded object old enough without touching
    /// the store's clock at all.
    fn aged_clock(config: &CompactorConfig) -> FixedClock {
        FixedClock::new(config.orphan_age_gate_ns() + 1)
    }

    #[tokio::test]
    async fn mass_orphan_trips_breaker_and_deletes_nothing() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 1;
        let store = MemoryStore::new();
        for seq in 0..60u64 {
            put_orphan(&store, &tenant, signal, shard, seq).await;
        }
        let config = CompactorConfig::default();
        let clock = aged_clock(&config);

        let err = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect_err("60 orphans out of 60 listed objects trips the breaker");
        match err {
            MaintainError::OrphanBreakerTripped {
                candidates,
                l0_objects_listed,
                min_count,
                max_ratio,
                ..
            } => {
                assert_eq!(candidates, 60);
                assert_eq!(l0_objects_listed, 60);
                assert_eq!(min_count, config.orphan_breaker_min_count);
                assert_eq!(max_ratio, config.orphan_breaker_max_ratio);
            }
            other => panic!("expected OrphanBreakerTripped, got {other:?}"),
        }

        let remaining = list_all(&store, &l0_data_prefix(&tenant, signal, shard).unwrap())
            .await
            .unwrap();
        assert_eq!(
            remaining.len(),
            60,
            "a tripped breaker deletes nothing at all"
        );
    }

    #[tokio::test]
    async fn below_threshold_pass_still_deletes_normally() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 2;
        let store = MemoryStore::new();
        for seq in 0..3u64 {
            put_orphan(&store, &tenant, signal, shard, seq).await;
        }
        let config = CompactorConfig::default();
        let clock = aged_clock(&config);

        let outcome = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("3 candidates is below orphan_breaker_min_count: no trip");
        assert_eq!(outcome.deleted, 3);
        assert!(!outcome.breaker_overridden);

        let remaining = list_all(&store, &l0_data_prefix(&tenant, signal, shard).unwrap())
            .await
            .unwrap();
        assert!(remaining.is_empty(), "all orphans deleted normally");
    }

    #[tokio::test]
    async fn forced_pass_deletes_and_reports_override() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 4;
        let store = MemoryStore::new();
        for seq in 0..60u64 {
            put_orphan(&store, &tenant, signal, shard, seq).await;
        }
        let config = CompactorConfig {
            force_orphan_gc: true,
            ..CompactorConfig::default()
        };
        let clock = aged_clock(&config);

        let outcome = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("force_orphan_gc overrides a would-have-tripped breaker");
        assert_eq!(outcome.deleted, 60);
        assert!(
            outcome.breaker_overridden,
            "reports that it deleted under override"
        );

        let remaining = list_all(&store, &l0_data_prefix(&tenant, signal, shard).unwrap())
            .await
            .unwrap();
        assert!(remaining.is_empty(), "forced pass deletes everything");
    }

    #[tokio::test]
    async fn batched_reverify_lists_commit_prefix_once_per_pass() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 3;
        // Four candidates, comfortably below orphan_breaker_min_count, so the
        // breaker never interferes with either half of this test.
        let config = CompactorConfig::default();

        // The second commit-prefix LIST is the batched re-verify (ADR-0048
        // decision 5): faulting it aborts the pass before any delete, which
        // proves it happens exactly once, not zero times.
        {
            let mem = MemoryStore::new();
            for seq in 0..4u64 {
                put_orphan(&mem, &tenant, signal, shard, seq).await;
            }
            let clock = aged_clock(&config);
            let plan = FaultPlan::empty().with_rule(
                Rule::new(Op::List, ScriptedFault::Timeout)
                    .with_key_contains("/c/")
                    .with_occurrence(Occurrence::Nth(2)),
            );
            let store = FaultStore::new(mem, plan);

            let err = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
                .await
                .expect_err("the batched re-verify LIST faults the pass");
            assert!(matches!(err, MaintainError::Store(_)));
            assert_eq!(
                store.fault_count(Op::List, FaultKind::Timeout),
                1,
                "exactly the batched re-verify LIST faulted"
            );

            let remaining = list_all(&store, &l0_data_prefix(&tenant, signal, shard).unwrap())
                .await
                .unwrap();
            assert_eq!(
                remaining.len(),
                4,
                "nothing deleted when the re-verify faults"
            );
        }

        // A third commit-prefix LIST never happens, no matter how many
        // candidates survived to the delete phase: the re-verify is batched
        // once per pass, not once per candidate.
        {
            let mem = MemoryStore::new();
            for seq in 0..4u64 {
                put_orphan(&mem, &tenant, signal, shard, seq).await;
            }
            let clock = aged_clock(&config);
            let plan = FaultPlan::empty().with_rule(
                Rule::new(Op::List, ScriptedFault::Timeout)
                    .with_key_contains("/c/")
                    .with_occurrence(Occurrence::Nth(3)),
            );
            let store = FaultStore::new(mem, plan);

            let outcome = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
                .await
                .expect("no third commit-prefix LIST: the pass completes");
            assert_eq!(outcome.deleted, 4);
            assert_eq!(
                store.fault_count(Op::List, FaultKind::Timeout),
                0,
                "batched: only two commit-prefix LISTs per pass, regardless of candidate count"
            );
        }
    }

    fn idem_receipt(written_count: u64) -> IdempotencyReceipt {
        IdempotencyReceipt {
            written_count,
            commit_token: "v2:token".to_string(),
        }
    }

    #[tokio::test]
    async fn idem_markers_past_window_swept_recent_kept() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant_hash = tenant_id.hash();
        let signal = Signal::Logs;
        let now_hour = 10_000u32;
        let window = 24u32;
        let config = CompactorConfig {
            idem_dedup_window_hours: window,
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(i64::from(now_hour) * NS_PER_HOUR);

        // The sweep's min_hour also subtracts the shared forward-skew
        // tolerance (fix for issue #531's adversarial checkpoint): a marker
        // is only strictly past the window once
        // now_hour - hour > window + IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS,
        // the same margin read_marker grants on its own upper bound, so the
        // sweep never reaps a marker the read path would still honor.
        let skew = IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS;
        let old_hours = [now_hour - window - skew - 1, now_hour - 200];
        // Within the window (plus skew tolerance), including the exact
        // boundary: now_hour - hour <= window + skew.
        let recent_hours = [now_hour - window - skew, now_hour - 1];

        for (i, hour) in old_hours.iter().enumerate() {
            write_marker(
                &store,
                &tenant_id,
                signal,
                format!("old-key-{i}").as_bytes(),
                *hour,
                &idem_receipt(i as u64),
            )
            .await
            .expect("seed old marker");
        }
        for (i, hour) in recent_hours.iter().enumerate() {
            write_marker(
                &store,
                &tenant_id,
                signal,
                format!("recent-key-{i}").as_bytes(),
                *hour,
                &idem_receipt(100 + i as u64),
            )
            .await
            .expect("seed recent marker");
        }

        let outcome =
            sweep_idempotency_markers(&store, &clock, &config, &NoLeases, &tenant_hash, signal)
                .await
                .expect("sweep must succeed");
        assert_eq!(outcome.deleted, old_hours.len());
        assert_eq!(outcome.kept, recent_hours.len());
        assert_eq!(outcome.skipped_malformed, 0);

        for (i, _hour) in old_hours.iter().enumerate() {
            // A generous window (covering the whole range) turns this lookup into a
            // pure existence check: a Hit here would mean the sweep failed to
            // delete the object, regardless of the sweep's own window.
            let looked_up = read_marker(
                &store,
                &tenant_id,
                signal,
                format!("old-key-{i}").as_bytes(),
                now_hour,
                now_hour,
            )
            .await
            .expect("lookup must succeed");
            assert_eq!(
                looked_up,
                LookupOutcome::Miss,
                "past-window marker {i} must be gone"
            );
        }
        for (i, hour) in recent_hours.iter().enumerate() {
            // Checked as a direct store existence check, not via read_marker:
            // read_marker's own min bound (now_hour - dedup_window) carries no
            // forward-skew margin -- only its upper bound does -- so the
            // boundary case (hour == now_hour - window - skew) sits in a gap
            // the sweep deliberately still protects (for a reader whose clock
            // lags the sweeper's by up to `skew`) but a same-clock
            // `read_marker` call would itself already call a Miss. The
            // sweep's own guarantee is that the object still exists; that is
            // what this asserts.
            let key = marker_key(
                &tenant_id,
                signal,
                format!("recent-key-{i}").as_bytes(),
                *hour,
            );
            assert!(
                store.get(&key, GetRange::Full).await.is_ok(),
                "recent marker {i} at hour {hour} must remain in the store"
            );
        }
    }

    #[tokio::test]
    async fn idem_sweep_never_touches_other_prefixes() {
        let tenant_a = TenantId::new("acme");
        let tenant_a_hash = tenant_a.hash();
        let tenant_b = TenantId::new("other-tenant");
        let tenant_b_hash = tenant_b.hash();
        let signal = Signal::Logs;
        let other_signal = Signal::Spans;

        let now_hour = 10_000u32;
        let window = 24u32;
        let old_hour = now_hour - 200;

        let mem = MemoryStore::new();
        // The one marker the sweep is allowed to touch.
        write_marker(
            &mem,
            &tenant_a,
            signal,
            b"key-a",
            old_hour,
            &idem_receipt(1),
        )
        .await
        .expect("seed tenant_a/logs marker");

        // Decoys seeded directly into the underlying store, bypassing
        // FaultStore's key-substring rules entirely: these prove isolation
        // structurally (the decoys are still readable afterward), not just
        // by asserting no fault fired. An implementation that widened its
        // LIST prefix or delete loop -- e.g. listing the bare `t/<tenant>/`
        // prefix instead of the (tenant, signal) idem prefix -- would delete
        // one of these and this test would catch it even though none of the
        // FaultPlan rules below would ever trigger.
        let l0_decoy_key = keys::data_key(
            &tenant_a_hash,
            signal,
            0,
            Uuid::from_u128(1),
            0,
            0,
            &[0xAB; 32],
        )
        .expect("build decoy l0 data key");
        mem.put(
            &l0_decoy_key,
            Bytes::from_static(b"l0-decoy"),
            PutOptions::default(),
        )
        .await
        .expect("seed l0 decoy");

        let commit_decoy_key = keys::commit_key(
            &tenant_a_hash,
            signal,
            0,
            old_hour,
            Uuid::from_u128(2),
            0,
            0,
        )
        .expect("build decoy commit key");
        mem.put(
            &commit_decoy_key,
            Bytes::from_static(b"commit-decoy"),
            PutOptions::default(),
        )
        .await
        .expect("seed commit decoy");

        write_marker(
            &mem,
            &tenant_b,
            signal,
            b"key-b",
            old_hour,
            &idem_receipt(2),
        )
        .await
        .expect("seed tenant_b/logs decoy marker");

        write_marker(
            &mem,
            &tenant_a,
            other_signal,
            b"key-a-spans",
            old_hour,
            &idem_receipt(3),
        )
        .await
        .expect("seed tenant_a/spans decoy marker");

        let config = CompactorConfig {
            idem_dedup_window_hours: window,
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(i64::from(now_hour) * NS_PER_HOUR);

        let other_tenant_prefix = idem_prefix(&tenant_b_hash, signal);
        let other_signal_prefix = idem_prefix(&tenant_a_hash, other_signal);
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::List, ScriptedFault::Timeout).with_key_contains("/l0/"))
            .with_rule(Rule::new(Op::Delete, ScriptedFault::Timeout).with_key_contains("/l0/"))
            .with_rule(Rule::new(Op::List, ScriptedFault::Timeout).with_key_contains("/c/"))
            .with_rule(Rule::new(Op::Delete, ScriptedFault::Timeout).with_key_contains("/c/"))
            .with_rule(
                Rule::new(Op::List, ScriptedFault::Timeout)
                    .with_key_contains(other_tenant_prefix.clone()),
            )
            .with_rule(
                Rule::new(Op::Delete, ScriptedFault::Timeout)
                    .with_key_contains(other_tenant_prefix),
            )
            .with_rule(
                Rule::new(Op::List, ScriptedFault::Timeout)
                    .with_key_contains(other_signal_prefix.clone()),
            )
            .with_rule(
                Rule::new(Op::Delete, ScriptedFault::Timeout)
                    .with_key_contains(other_signal_prefix),
            );
        let store = FaultStore::new(mem, plan);

        let outcome =
            sweep_idempotency_markers(&store, &clock, &config, &NoLeases, &tenant_a_hash, signal)
                .await
                .expect("sweep must touch only its own (tenant, signal) idem prefix");
        assert_eq!(outcome.deleted, 1);

        assert_eq!(
            store.fault_count(Op::List, FaultKind::Timeout),
            0,
            "no LIST outside the swept (tenant, signal) idem prefix"
        );
        assert_eq!(
            store.fault_count(Op::Delete, FaultKind::Timeout),
            0,
            "no DELETE outside the swept (tenant, signal) idem prefix"
        );

        for decoy_key in [&l0_decoy_key, &commit_decoy_key] {
            assert!(
                store.get(decoy_key, GetRange::Full).await.is_ok(),
                "decoy {decoy_key} must survive the sweep"
            );
        }
        for (decoy_tenant, decoy_signal, key_hint) in [
            (&tenant_b, signal, b"key-b" as &[u8]),
            (&tenant_a, other_signal, b"key-a-spans"),
        ] {
            let decoy_marker_key = marker_key(decoy_tenant, decoy_signal, key_hint, old_hour);
            assert!(
                store.get(&decoy_marker_key, GetRange::Full).await.is_ok(),
                "decoy marker {decoy_marker_key} must survive the sweep"
            );
        }
    }

    #[tokio::test]
    async fn idem_sweep_skips_malformed_marker_key_without_deleting() {
        let store = MemoryStore::new();
        let tenant_hash = TenantId::new("acme").hash();
        let signal = Signal::Logs;

        let prefix = idem_prefix(&tenant_hash, signal);
        let wrong_suffix_key = format!("{prefix}deadbeefdeadbeefdeadbeefdeadbeef.txt");
        let bad_hour_key = format!("{prefix}deadbeefdeadbeefdeadbeefdeadbeef.notahexhour.idm");
        // Real bug this guards against: a key whose pre-hour segment is not a
        // genuine 32-char lowercase-hex keyhash must be skipped, never
        // deleted, exactly like a bad hour string already is (fix for issue
        // #531's adversarial checkpoint).
        let short_keyhash_key = format!("{prefix}deadbeef.19700101T00.idm");
        let uppercase_keyhash_key =
            format!("{prefix}DEADBEEFDEADBEEFDEADBEEFDEADBEEF.19700101T00.idm");
        let not_hex_keyhash_key = format!("{prefix}not-hex-at-all.19700101T00.idm");
        let nested_path_key =
            format!("{prefix}backup/deadbeefdeadbeefdeadbeefdeadbeef.19700101T00.idm");
        let malformed_keys = [
            &wrong_suffix_key,
            &bad_hour_key,
            &short_keyhash_key,
            &uppercase_keyhash_key,
            &not_hex_keyhash_key,
            &nested_path_key,
        ];
        for key in malformed_keys {
            store
                .put(key, Bytes::from_static(b"garbage"), PutOptions::default())
                .await
                .expect("seed malformed key");
        }

        let config = CompactorConfig::default();
        let clock = FixedClock::new(0);

        let outcome =
            sweep_idempotency_markers(&store, &clock, &config, &NoLeases, &tenant_hash, signal)
                .await
                .expect("malformed keys are skipped, never fatal");
        assert_eq!(outcome.deleted, 0);
        assert_eq!(outcome.kept, 0);
        assert_eq!(outcome.skipped_malformed, malformed_keys.len());

        for key in malformed_keys {
            let remaining = store.get(key, GetRange::Full).await;
            assert!(remaining.is_ok(), "malformed key {key} must not be deleted");
        }

        for key in [&wrong_suffix_key, &bad_hour_key] {
            let remaining = store.get(key, GetRange::Full).await;
            assert!(remaining.is_ok(), "malformed key must not be deleted");
        }
    }

    #[tokio::test]
    async fn idem_sweep_dry_run_counts_without_deleting() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant_hash = tenant_id.hash();
        let signal = Signal::Logs;
        let now_hour = 10_000u32;
        let window = 24u32;
        let old_hour = now_hour - 200;

        write_marker(
            &store,
            &tenant_id,
            signal,
            b"key",
            old_hour,
            &idem_receipt(1),
        )
        .await
        .expect("seed old marker");

        let config = CompactorConfig {
            idem_dedup_window_hours: window,
            dry_run: true,
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(i64::from(now_hour) * NS_PER_HOUR);

        let outcome =
            sweep_idempotency_markers(&store, &clock, &config, &NoLeases, &tenant_hash, signal)
                .await
                .expect("dry run must not error");
        assert_eq!(
            outcome.deleted, 1,
            "dry run still counts what would be deleted"
        );
        assert_eq!(outcome.kept, 0);

        let key = marker_key(&tenant_id, signal, b"key", old_hour);
        let still_there = store.get(&key, GetRange::Full).await;
        assert!(still_there.is_ok(), "dry_run must not actually delete");
    }
}
