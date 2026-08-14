//! The sweeper: one component, five eligibility rules
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
//! 5. **Unreferenced catalog-object sweep** (EH-T4, issue #741,
//!    docs/metric-index-plan.md 3-4): a snapshot part under
//!    `t/<tenant_hash>/catalog/<signal>/snap/` or a name-postings object under
//!    the sibling `.../idx/` prefix that the current
//!    `.../catalog/<signal>/HEAD` does not name (neither a `parts[].key` nor
//!    the optional `postings.key`), once the object's `last_modified` age
//!    exceeds the protection horizon. Every fold that rewrites a part or
//!    postings object supersedes the old one by writing a new
//!    content-addressed key and swapping HEAD; the superseded object is left
//!    in place (plan 4 step 8, the "orphan part" crash-matrix row) and leaks
//!    until this rule collects it. Per (tenant, signal), not per shard:
//!    catalog objects and HEAD carry no shard dimension, so the rule LISTs the
//!    two coarse prefixes first and only then reads one HEAD, exactly the
//!    coarse-prefix shape rule 4 uses for `idem/`. A present, decodable HEAD is
//!    the rule's only anchor: an absent HEAD sweeps nothing for that
//!    (tenant, signal), mirroring rule 3's neither-record-nor-tombstone bucket
//!    (a recovery fold with no HEAD recomputes and re-PUTs every part under
//!    keys byte-identical to any surviving old object, adopting it via
//!    `AlreadyExists` without rewriting it, then names it in the HEAD it is
//!    about to CAS -- so record-less catalog objects with no HEAD to compare
//!    against may belong to a fold in flight). A HEAD that is present but fails
//!    to decode aborts the pass without deleting, so a corrupt HEAD can never
//!    cause the live snapshot to be read as unreferenced and swept.
//!
//! All five are **signal-generic**: rules 1-3 operate only on commit-record,
//! compaction-record, and object *keys* plus store `last_modified`, rule 4
//! only on marker keys, and rule 5 only on catalog object keys plus the HEAD
//! it decodes to a referenced-key set, never on a segment byte, so nothing
//! here needs to know RSEG from RLOG. All five are stateless per pass,
//! restartable from zero, and every delete is idempotent (the object-store
//! contract makes deleting a missing key a success). The clock is always
//! injected; rules 1-3 and rule 5 read object age from `last_modified` (which
//! the object-store contract restricts to exactly GC age checks), while rule 4
//! reads it from the ingest hour encoded in the marker's own key.
//!
//! The [`LeaseCheck`] hook is consulted before every delete in all five
//! rules. It ships as the no-op [`NoLeases`] ("nothing is ever protected"):
//! the consistency-model's "not lease-protected" precondition is then
//! vacuously satisfied everywhere. It is a seam for future slow-consumer work
//! (plan §5, Q3), not live logic; no lease machinery is built behind it.
//!
//! [`sweep_shard_zoned`] scopes rules 2 and 3 to a given hour set, mirroring
//! the unit scan's zone split (ADR-0065 decision 3): the caller's per-tick
//! pass lists only its head+tail hours, and a full pass on the slow
//! safety-net cadence still uses [`sweep_shard`] to eventually cover every
//! hour. Rule 1 cannot be scoped this way and always lists the whole shard
//! (L0 keys carry no ingest-hour component); see [`sweep_shard_zoned`]'s doc
//! for that deviation.

use std::collections::{HashMap, HashSet};

use prost::Message;
use ravel_commit::keys::{self, BucketEntry, KeyError, parse_ingest_hour_string};
use ravel_commit::record;
use ravel_object_store::{GetRange, ObjectMeta, ObjectStoreBackend, StoreError, list_all};
use ravel_proto::commit::v1::{CompactionInputIdentity, CompactionRecord, RewriteRecord};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::{CompactorConfig, NS_PER_HOUR};
use crate::error::{MaintainError, Result};
use crate::read::verify_commit_key;

use ravel_ingest::{IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS, MARKER_SUFFIX};

/// A hook the sweeper consults before every delete, in all five rules. The
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
    /// `true` if this pass listed the whole shard (rule 1 always does; rules
    /// 2 and 3 did here too, either because the caller used [`sweep_shard`]
    /// or because [`sweep_shard_zoned`] was asked to widen to every hour).
    /// `false` for a [`sweep_shard_zoned`] pass scoped to a hour subset.
    /// Counter seam for EI-T5's `ravel_maintain_full_sweep_passes_total`: a
    /// caller running the slow safety-net cadence increments its own counter
    /// when this is `true`.
    pub full_pass: bool,
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
        full_pass: true,
    })
}

/// Zone-scoped sweep pass (ADR-0065 decision 3): rules 2 and 3 list only the
/// given `hours`' commit and L1 prefixes instead of the whole shard,
/// mirroring the unit scan's zone split so a per-tick pass never re-lists
/// interior hours a tick's zone recomputation already decided to skip.
/// `hours` is the caller's current head+tail set for this unit.
///
/// Rule 1 (orphan GC) always lists the whole shard regardless of `hours`: L0
/// data keys carry no ingest-hour component (`ravel_commit::keys::data_key`),
/// so there is no hour-scoped prefix to list. This is a structural limit, not
/// an oversight -- flagged as a deviation from a literal reading of the ADR,
/// which does not distinguish rule 1 from rules 2 and 3 when describing the
/// per-tick sweep as hour-scoped.
///
/// The caller is responsible for the slow safety-net cadence: call
/// [`sweep_shard`] instead of this function on that cadence so rules 2 and 3
/// eventually cover every hour, including one a bug or a missed invalidation
/// left permanently out of `hours`. [`SweepReport::full_pass`] is always
/// `false` on the report this function returns.
#[allow(clippy::too_many_arguments)]
pub async fn sweep_shard_zoned(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    hours: &[u32],
) -> Result<SweepReport> {
    let (superseded_records_deleted, superseded_data_deleted) = sweep_superseded_impl(
        store,
        clock,
        config,
        lease,
        tenant,
        signal,
        shard,
        Some(hours),
    )
    .await?;
    let unreferenced_parts_deleted = sweep_unreferenced_parts_impl(
        store,
        clock,
        config,
        lease,
        tenant,
        signal,
        shard,
        Some(hours),
    )
    .await?;
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
        full_pass: false,
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
    sweep_superseded_impl(store, clock, config, lease, tenant, signal, shard, None).await
}

/// Shared implementation behind [`sweep_superseded`] (whole-shard, `hours:
/// None`) and [`sweep_shard_zoned`] (hour-scoped, `hours: Some(_)`).
#[allow(clippy::too_many_arguments)]
async fn sweep_superseded_impl(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    hours: Option<&[u32]>,
) -> Result<(usize, usize)> {
    let now = clock.now_ns();
    let entries = list_commit_entries_scoped(store, tenant, signal, shard, hours).await?;

    let mut records_deleted = 0usize;
    let mut data_deleted = 0usize;
    for (key, entry) in &entries {
        // Both a compaction record and a selective-erasure rewrite record
        // (ADR-0064 decision 3 point 6) render their superseded inputs
        // collectable by this one rule; a raw commit record or tombstone names
        // no superseded input. The two record types are fetched NotFound-
        // tolerantly: a rewrite whose `superseded_record_key` names a
        // predecessor record deletes that predecessor here, and that same
        // predecessor may also appear as its own entry in this listing, so its
        // fetch can legitimately miss once a superseding generation processed
        // earlier in this pass already removed it.
        let (record_keys, data_keys) = match entry {
            BucketEntry::CompactionRecord(_) => {
                let Some(record) = get_compaction_record_opt(store, key).await? else {
                    continue;
                };
                // Horizon gate anchored on the durable created_unix_ns (plan §5).
                if now
                    < record
                        .created_unix_ns
                        .saturating_add(config.protection_horizon_ns)
                {
                    continue;
                }
                gather_l0_inputs(store, tenant, signal, shard, &record).await?
            }
            BucketEntry::RewriteRecord(_) => {
                let Some(record) = get_rewrite_record_opt(store, key).await? else {
                    continue;
                };
                // Horizon gate anchored on the rewrite's own durable
                // created_unix_ns: that is the instant it superseded its
                // inputs, so a query pinned before it is drained by the
                // protection horizon exactly as for a compaction record.
                if now
                    < record
                        .created_unix_ns
                        .saturating_add(config.protection_horizon_ns)
                {
                    continue;
                }
                if !record.inputs.is_empty() {
                    // RawL0 rewrite: the same L0 commit records + data objects
                    // a compaction over the same inputs would supersede.
                    gather_l0_inputs(store, tenant, signal, shard, &record).await?
                } else {
                    // Predecessor rewrite: the whole prior compaction/rewrite
                    // record this one superseded, plus every L1 part it named
                    // (the pre-rewrite parts holding the un-erased subject).
                    // Rule 3 cannot collect those parts while the predecessor
                    // record still references them, so this rule removes both.
                    gather_superseded_predecessor(store, &record.superseded_record_key).await?
                }
            }
            BucketEntry::CommitRecord(_) | BucketEntry::Tombstone(_) => continue,
        };

        // Records first, then data objects (plan §5, docs/consistency-model.md):
        // a crash between the two phases leaves record-less data (orphan GC) or
        // an unreferenced part (rule 3), never a record pointing at a deleted
        // object.
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

/// Gather `(commit record key, data object key)` pairs for every input a
/// compaction or rewrite record names in its `inputs` list. Shared by rule 2's
/// compaction and rewrite-RawL0 arms (ADR-0018, ADR-0064 decision 3 point 6):
/// both name the same raw-L0 input shape. The data key needs each input
/// record's content hash, so each input record is read before it is deleted; an
/// input already gone (a crash-interrupted prior pass) is skipped and its data
/// object, if any, is collected by orphan GC (row 8).
async fn gather_l0_inputs(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    // Generic rather than `&dyn`: the reference is held across the input GETs'
    // `.await`, and a `&dyn SupersededInputs` there is not `Send` (the trait
    // object is not `Sync`), which would make the whole sweep future non-`Send`
    // and unspawnable from the server's maintain loop. A concrete `&R` over the
    // two `Sync` proto record types is `Send`.
    record: &impl SupersededInputs,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut record_keys: Vec<String> = Vec::new();
    let mut data_keys: Vec<String> = Vec::new();
    for commit_key in superseded_input_commit_keys(tenant, signal, shard, record)? {
        match store.get(&commit_key, GetRange::Full).await {
            Ok(got) => {
                let rec = record::decode(&got.data)?;
                // The record's key must reconstruct to the key we fetched it at
                // (ADR-0010 §7): a corrupted-but-decodable input record's own
                // fields, which reconstruct_data_key trusts, must not name a
                // data object outside the bucket this key implies (mirrors
                // read::load_inputs).
                verify_commit_key(&rec, &commit_key)?;
                let data_key = keys::reconstruct_data_key(&rec)?;
                record_keys.push(commit_key);
                data_keys.push(data_key);
            }
            Err(StoreError::NotFound) => {}
            Err(e) => return Err(MaintainError::Store(e)),
        }
    }
    Ok((record_keys, data_keys))
}

/// Reconstruct the commit key of every raw-L0 input a compaction or rewrite
/// record explicitly names, in `inputs` order. This is rule 2's supersession
/// predicate on its own, with no store access: a commit record whose key this
/// returns is superseded by `record` and is a pre-compaction leftover, and one
/// whose key it does not return is not, whatever bucket either sits in.
///
/// It is deliberately keyed on the record's own input list rather than on the
/// bucket the record lives in. The two coincide today only because a
/// compaction or rewrite refuses an unsealed bucket and a sealed bucket's L0
/// set is frozen, so a record over that bucket necessarily covers all of it.
/// Any caller that needs "is this L0 record superseded" must ask this
/// question, not the bucket-membership question, or it inherits that seal
/// invariant as a silent premise: a partial-coverage record (one naming some
/// but not all of its bucket's L0 set) makes the two answers differ, and the
/// bucket-membership answer is the wrong one.
///
/// Shared by rule 2's own [`gather_l0_inputs`] and by the migrate floor-raise
/// re-audit ([`crate::migrate::count_below_target`], issue #923), so the
/// definition of supersession has exactly one implementation and the re-audit
/// stays an independent check of input-set coverage rather than a restatement
/// of the walk's assumptions.
pub(crate) fn superseded_input_commit_keys(
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    record: &impl SupersededInputs,
) -> Result<Vec<String>> {
    let mut keys_out = Vec::with_capacity(record.inputs().len());
    for input in record.inputs() {
        let writer_id = Uuid::parse_str(&input.writer_id)
            .map_err(|_| MaintainError::Key(KeyError::InvalidWriterId(input.writer_id.clone())))?;
        keys_out.push(keys::commit_key(
            tenant,
            signal,
            shard,
            record.ingest_hour_bucket(),
            writer_id,
            input.writer_epoch,
            input.writer_seq,
        )?);
    }
    Ok(keys_out)
}

/// A record that names raw-L0 superseded inputs (a compaction record, or a
/// rewrite record whose live input set was raw L0). Abstracts over the two
/// proto types so [`gather_l0_inputs`] and
/// [`superseded_input_commit_keys`] serve both without duplication.
pub(crate) trait SupersededInputs {
    fn inputs(&self) -> &[CompactionInputIdentity];
    fn ingest_hour_bucket(&self) -> u32;
}

impl SupersededInputs for CompactionRecord {
    fn inputs(&self) -> &[CompactionInputIdentity] {
        &self.inputs
    }
    fn ingest_hour_bucket(&self) -> u32 {
        self.ingest_hour_bucket
    }
}

impl SupersededInputs for RewriteRecord {
    fn inputs(&self) -> &[CompactionInputIdentity] {
        &self.inputs
    }
    fn ingest_hour_bucket(&self) -> u32 {
        self.ingest_hour_bucket
    }
}

/// Gather the deletion targets for a rewrite record that superseded a whole
/// prior compaction/rewrite record (ADR-0064 amendment: `superseded_record_key`
/// names either an `l1.<hash>.cmt` compaction record or an `rw.<hash>.cmt`
/// rewrite record, recursive supersession included). Returns
/// `([predecessor record key], [predecessor part keys...])`: the record is
/// deleted first (records-before-data), then every L1 part it named.
///
/// A predecessor already gone (swept by an earlier iteration of this same pass
/// when the predecessor also appeared as its own entry, or by a crash-
/// interrupted prior pass) yields empty lists: any of its parts that survive
/// become unreferenced under the live rewrite record and are collected by rule
/// 3.
async fn gather_superseded_predecessor(
    store: &dyn ObjectStoreBackend,
    predecessor_key: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    let got = match store.get(predecessor_key, GetRange::Full).await {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Ok((Vec::new(), Vec::new())),
        Err(e) => return Err(MaintainError::Store(e)),
    };
    let mut data_keys: Vec<String> = Vec::new();
    match keys::partition_bucket_entry(predecessor_key) {
        Ok(BucketEntry::CompactionRecord(_)) => {
            let record = CompactionRecord::decode(got.data.as_ref()).map_err(|e| {
                MaintainError::Invariant(format!("superseded compaction record decode failed: {e}"))
            })?;
            keys::verify_compaction_record_key(&record, predecessor_key)?;
            for part in &record.parts {
                data_keys.push(keys::reconstruct_l1_part_key(&record, part)?);
            }
        }
        Ok(BucketEntry::RewriteRecord(_)) => {
            let record = ravel_commit::erasure::decode_rewrite(got.data.as_ref()).map_err(|e| {
                MaintainError::Invariant(format!("superseded rewrite record decode failed: {e}"))
            })?;
            keys::verify_rewrite_record_key(&record, predecessor_key)?;
            for part in &record.parts {
                data_keys.push(keys::reconstruct_rewrite_part_key(&record, part)?);
            }
        }
        Ok(BucketEntry::CommitRecord(_) | BucketEntry::Tombstone(_)) => {
            return Err(MaintainError::Invariant(format!(
                "rewrite superseded_record_key {predecessor_key} names a non-compaction, \
                 non-rewrite entry"
            )));
        }
        Err(KeyError::UnknownBucketEntryShape(k)) => {
            return Err(MaintainError::UnknownBucketEntry(k));
        }
        Err(e) => return Err(MaintainError::Key(e)),
    }
    Ok((vec![predecessor_key.to_string()], data_keys))
}

/// [`get_compaction_record`] tolerant of a NotFound (Ok(None)): the record was
/// swept between this pass's LIST and now (e.g. a superseding rewrite processed
/// earlier in the same pass removed it, or a crash-interrupted prior pass).
async fn get_compaction_record_opt(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<CompactionRecord>> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            let record = CompactionRecord::decode(got.data.as_ref()).map_err(|e| {
                MaintainError::Invariant(format!("compaction record decode failed: {e}"))
            })?;
            keys::verify_compaction_record_key(&record, key)?;
            Ok(Some(record))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// [`get_rewrite_record`] tolerant of a NotFound (Ok(None)); see
/// [`get_compaction_record_opt`] for when that happens.
async fn get_rewrite_record_opt(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<RewriteRecord>> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            let record = ravel_commit::erasure::decode_rewrite(got.data.as_ref()).map_err(|e| {
                MaintainError::Invariant(format!("rewrite record decode failed: {e}"))
            })?;
            keys::verify_rewrite_record_key(&record, key)?;
            Ok(Some(record))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(e) => Err(MaintainError::Store(e)),
    }
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
    sweep_unreferenced_parts_impl(store, clock, config, lease, tenant, signal, shard, None).await
}

/// Shared implementation behind [`sweep_unreferenced_parts`] (whole-shard,
/// `hours: None`) and [`sweep_shard_zoned`] (hour-scoped, `hours: Some(_)`).
/// When scoped, both the commit-prefix listing that builds the reference map
/// and the `l1/` listing are restricted to `hours`: an interior bucket this
/// tick's zone recomputation skipped is never listed by either.
#[allow(clippy::too_many_arguments)]
async fn sweep_unreferenced_parts_impl(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    hours: Option<&[u32]>,
) -> Result<usize> {
    let now = clock.now_ns();
    let gate = config.unreferenced_part_age_gate_ns();
    let (referenced, tombstoned) =
        bucket_reference_map_scoped(store, tenant, signal, shard, hours).await?;

    let objects = list_l1_scoped(store, tenant, signal, shard, hours).await?;
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
        let (fresh_ref, fresh_tomb) =
            bucket_reference_map_scoped(store, tenant, signal, shard, hours).await?;
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
async fn bucket_reference_map_scoped(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    hours: Option<&[u32]>,
) -> Result<(HashMap<u32, HashSet<String>>, HashSet<u32>)> {
    let entries = list_commit_entries_scoped(store, tenant, signal, shard, hours).await?;
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

// --- Rule 5: unreferenced catalog-object sweep (EH-T4, issue #741) ----------

/// What one unreferenced-catalog-object sweep pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CatalogSweepOutcome {
    /// Superseded snapshot parts / postings objects deleted (or, under
    /// `dry_run`, that would have been).
    pub deleted: usize,
    /// Objects left in place: named by the current HEAD, younger than the
    /// protection horizon, lease-protected, or spared because there was no
    /// present HEAD to anchor the sweep (the no-anchor case, in which every
    /// listed object is kept).
    pub kept: usize,
}

/// Delete every snapshot part (`t/<tenant_hash>/catalog/<signal>/snap/`) and
/// name-postings object (sibling `.../idx/`) that the current
/// `.../catalog/<signal>/HEAD` does not name, once the object's `last_modified`
/// age exceeds `config.protection_horizon_ns` (EH-T4, issue #741,
/// docs/metric-index-plan.md 3-4).
///
/// A fold supersedes a part or postings object by writing a fresh
/// content-addressed key and swapping HEAD; the old object is deliberately left
/// in place (plan 4 step 8) and would otherwise leak on every content-changing
/// fold. This rule is the GC track's side of that contract.
///
/// Ordering and safety, mirroring the other physical-delete rules:
/// - **LIST before HEAD, then re-verify HEAD before deleting.** The two coarse
///   prefixes are listed first; HEAD is read *after* the LIST, and a fresh
///   batched re-verify GET of HEAD is taken immediately before the delete loop
///   (the same batched shape rule 1 uses for its commit-prefix re-verify LIST).
///   A candidate the fresh HEAD now names -- a fold's HEAD CAS that landed
///   between the two reads -- is dropped, so a part a completed fold just
///   published is never swept.
/// - **Reference set from a present, decodable HEAD only.** The referenced set
///   is exactly HEAD's `parts[].key` plus its optional `postings.key`. An
///   object under the two prefixes but not in that set is superseded or
///   orphaned.
/// - **No anchor, no sweep.** An absent HEAD sweeps nothing for the
///   (tenant, signal), matching rule 3's neither-record-nor-tombstone bucket
///   exactly. This is not an over-abundance of caution: a recovery fold with no
///   HEAD (`HeadState::Absent`/`Corrupt`) recomputes and re-PUTs every part,
///   and because a non-tail span keys on its stable `watermark_hour` the
///   recomputed key is byte-identical to any surviving old object, so the PUT
///   returns `AlreadyExists` and the fold *adopts the old object without
///   rewriting it* (crates/ravel-catalog/src/fold.rs) before naming it in the
///   HEAD it is about to CAS. With no HEAD to compare against, a record-less
///   catalog object is indistinguishable from a part such a fold is mid-flight
///   on, so it must be left alone.
/// - **Age gate is a reader-pinning buffer, NOT a writer interlock.** The
///   `protection_horizon_ns` term is `max_query_duration + grace` (plan §5): it
///   spares an object a query resolved just before the fold still has pinned.
///   Unlike [`CompactorConfig::orphan_age_gate_ns`] (`grace +
///   max_flush_lifetime`) and [`CompactorConfig::unreferenced_part_age_gate_ns`]
///   (`grace + max_compaction_lifetime`), whose lifetime terms mirror a real
///   writer *abandonment deadline* and so on their own guarantee no future
///   writer can re-reference an object past the gate, the horizon carries no
///   fold-lifetime term and does NOT establish such an interlock here: a fold
///   has no abandonment deadline, and adoption-via-`AlreadyExists` never
///   refreshes `last_modified`, so an object's age says nothing about whether a
///   fold is about to adopt and name it. What bounds the writer race instead is
///   the two points above -- the no-anchor rule (a fold rebuilding from no HEAD
///   adopts old keys, so we never sweep without a HEAD) and the pre-delete HEAD
///   re-verify (a fold that has completed its CAS is seen). The remaining
///   window between the re-verify GET and the delete is the seam the
///   [`LeaseCheck`] hook / future reader-lease work closes (plan §5, Q3); it is
///   not closed by an age gate, and this comment does not claim otherwise.
/// - **Lease/legal-hold gate.** Every delete consults the [`LeaseCheck`] hook,
///   like every other physical delete here.
/// - **Fail-closed on a corrupt HEAD.** A HEAD present but undecodable aborts
///   the pass with an error and deletes nothing, so a corrupt HEAD can never
///   make the live snapshot look unreferenced.
///
/// Per (tenant, signal), not per shard: catalog objects carry no shard
/// dimension, so a driver calls this once per (tenant, signal) per tick, like
/// [`sweep_idempotency_markers`], not inside the per-shard [`sweep_shard`]
/// loop.
///
/// No production driver calls it yet. `ravel-server`'s maintenance tick runs
/// [`sweep_idempotency_markers`] and [`sweep_erasure_requests`] at that
/// granularity but not this rule, so unreferenced catalog objects are
/// currently collected by nothing outside tests. Stated here rather than left
/// as the present-tense "the dispatcher calls this", which this doc claimed
/// before it was true of any rule.
pub async fn sweep_unreferenced_catalog_objects(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<CatalogSweepOutcome> {
    let now = clock.now_ns();
    let horizon = config.protection_horizon_ns;

    // LIST the two coarse prefixes first, before reading HEAD (finding 3: the
    // reference set must be read *after* the listing, matching rules 1 and 3;
    // reading HEAD first is the widest possible race window).
    let mut listed: Vec<ObjectMeta> = Vec::new();
    for prefix in [
        catalog_snap_prefix(tenant, signal),
        catalog_idx_prefix(tenant, signal),
    ] {
        listed.extend(list_all(store, &prefix).await?);
    }

    // Reference set from HEAD, read after the LIST. An absent HEAD is the
    // no-anchor case: sweep nothing (finding 2), never the old "collect every
    // orphan" behavior, because a recovery fold rebuilding from no HEAD adopts
    // surviving old keys via `AlreadyExists` and is about to name them.
    let referenced = match read_head_reference(store, tenant, signal).await? {
        HeadReference::Present(set) => set,
        HeadReference::Absent => {
            return Ok(CatalogSweepOutcome {
                deleted: 0,
                kept: listed.len(),
            });
        }
    };

    let mut kept = 0usize;
    let mut candidates: Vec<ObjectMeta> = Vec::new();
    for meta in listed {
        // Named by the current HEAD: a live part or the live postings object.
        // Never delete; this is the whole safety property.
        if referenced.contains(&meta.key) {
            kept += 1;
            continue;
        }
        // Younger than the protection horizon: spare it (a part a still-running
        // query pinned; see the age-gate caveat above -- this is a
        // reader-pinning buffer, not a writer interlock).
        if object_age_ns(now, &meta) <= horizon {
            kept += 1;
            continue;
        }
        if lease.is_protected(&meta.key) {
            kept += 1;
            continue;
        }
        candidates.push(meta);
    }

    // Fresh batched re-verify GET of HEAD immediately before the delete loop
    // (finding 3), the same batched shape rule 1 uses for its re-verify LIST: a
    // fold's HEAD CAS may have landed since the first read and now name one of
    // these candidates. A HEAD that vanished between the two reads is again the
    // no-anchor case -- spare everything rather than delete without a HEAD.
    if !candidates.is_empty() {
        let fresh = match read_head_reference(store, tenant, signal).await? {
            HeadReference::Present(set) => set,
            HeadReference::Absent => {
                return Ok(CatalogSweepOutcome {
                    deleted: 0,
                    kept: kept + candidates.len(),
                });
            }
        };
        let mut survivors = Vec::with_capacity(candidates.len());
        for meta in candidates {
            if fresh.contains(&meta.key) {
                kept += 1;
            } else {
                survivors.push(meta);
            }
        }
        candidates = survivors;
    }

    let mut deleted = 0usize;
    for meta in &candidates {
        if !config.dry_run {
            store.delete(&meta.key).await?;
        }
        deleted += 1;
    }

    Ok(CatalogSweepOutcome { deleted, kept })
}

/// The outcome of reading the catalog HEAD for one `(tenant, signal)`.
/// [`Self::Absent`] is the no-anchor case rule 5 must not sweep against; a
/// present but undecodable HEAD is not represented here at all, because
/// [`read_head_reference`] fails the whole pass on it (fail-closed).
enum HeadReference {
    /// HEAD is present and decoded: the set of keys it names (every
    /// `parts[].key` plus the optional `postings.key`).
    Present(HashSet<String>),
    /// HEAD is absent. There is no anchor to compare against, so rule 5 sweeps
    /// nothing for this (tenant, signal) (a fold rebuilding from no HEAD adopts
    /// surviving old keys and is about to name them).
    Absent,
}

/// Read the catalog HEAD for one `(tenant, signal)` into a [`HeadReference`].
/// An absent HEAD is [`HeadReference::Absent`] (the no-anchor case, swept
/// nothing); a present but undecodable HEAD is a fail-closed error so the pass
/// deletes nothing rather than treat a live snapshot as unreferenced.
async fn read_head_reference(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<HeadReference> {
    let head_key = catalog_head_key(tenant, signal);
    match store.get(&head_key, GetRange::Full).await {
        Ok(got) => {
            let head = ravel_catalog::decode_head(got.data.as_ref()).map_err(|e| {
                MaintainError::Invariant(format!(
                    "catalog sweep: HEAD at {head_key} failed to decode ({e}); deleting \
                     nothing this pass rather than treating a live snapshot as unreferenced"
                ))
            })?;
            let mut referenced =
                HashSet::with_capacity(head.parts.len() + usize::from(head.postings.is_some()));
            for part in &head.parts {
                referenced.insert(part.key.clone());
            }
            if let Some(postings) = &head.postings {
                referenced.insert(postings.key.clone());
            }
            Ok(HeadReference::Present(referenced))
        }
        Err(StoreError::NotFound) => Ok(HeadReference::Absent),
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// `t/<tenant_hash_hex>/catalog/<signal>/HEAD` -- the mutable head pointer for
/// one `(tenant, signal)` (docs/catalog-and-mvcc.md key layout). No public
/// builder is exported from ravel-catalog, so it is constructed here from the
/// same pieces, matching `idem_prefix`'s local-reconstruction precedent.
fn catalog_head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

/// `t/<tenant_hash_hex>/catalog/<signal>/snap/` -- the prefix covering every
/// snapshot part for one `(tenant, signal)`, across every watermark.
fn catalog_snap_prefix(tenant: &TenantHash, signal: Signal) -> String {
    format!(
        "t/{}/catalog/{}/snap/",
        tenant.to_hex(),
        signal.key_prefix()
    )
}

/// `t/<tenant_hash_hex>/catalog/<signal>/idx/` -- the prefix covering every
/// name-postings object for one `(tenant, signal)`, across every watermark.
fn catalog_idx_prefix(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/idx/", tenant.to_hex(), signal.key_prefix())
}

// --- Rule 6: erasure-request (.dreq) removal (ADR-0064 decision 5) -----------

/// What one erasure-request sweep pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ErasureRequestSweepOutcome {
    /// `.dreq` objects deleted (or, under `dry_run`, that would have been).
    pub deleted: usize,
    /// `.dreq` objects left in place: no `.done` yet (erasure not complete),
    /// still inside the post-completion protection horizon, or lease/legal-hold
    /// protected.
    pub kept: usize,
}

/// Delete every erasure request `t/<tenant_hash>/<signal>/del/<request_id>.dreq`
/// whose erasure is complete and past the post-completion horizon (ADR-0064
/// decision 5, docs/consistency-model.md "Deletion guarantees").
///
/// A `.dreq` carries the subject identifier, so it must not outlive its
/// purpose. Its matching `.done` completion record carries only a predicate
/// hash and per-bucket counts (no subject identifier) and is permanent,
/// deny-delete audit evidence: this rule never deletes a `.done`.
///
/// A `.dreq` is deleted only when ALL hold:
/// - its matching `.done` completion exists (erasure is verified complete);
/// - `now >= done.completed_unix_ns + protection_horizon` -- the same horizon
///   that gates every superseded-input delete (rule 2), anchored on the
///   durable completion timestamp, never on wall-clock at sweep time. This
///   wait is what makes removal safe: retiring the query-time exclusion filter
///   (which happens the instant the `.dreq` disappears, since the resolver's
///   `del/` listing no longer finds it) can never resurrect the subject,
///   because after the horizon no resolvable snapshot can still reference a
///   pre-rewrite input (ADR-0064 §3.5 race window, closed durably). Deleting
///   the `.dreq` a nanosecond early would reopen exactly that window.
/// - the [`LeaseCheck`] passes: a legal hold over the `del/` keyspace pins the
///   request exactly as it pins any other object.
///
/// A completion whose `completed_unix_ns` is zero is treated fail-safe as "not
/// yet a valid horizon anchor" and its `.dreq` is kept: a zero anchor would
/// collapse the horizon gate to always-past and could retire the filter early.
///
/// Per (tenant, signal), not per shard (the `del/` prefix carries no shard
/// dimension): `ravel-server`'s maintenance tick calls this once per (tenant,
/// signal), alongside [`sweep_idempotency_markers`] and after that signal's
/// erasure rewrite pass and completion (`.done`) write, not inside the
/// per-shard [`sweep_shard`] loop. That order is what makes the rule safe:
/// the `.done` this rule waits on is written by the same tick that verified
/// the rewrite, so a `.dreq` is only ever removed after its erasure is
/// durably complete. ([`sweep_unreferenced_catalog_objects`] is at the same
/// granularity but has no driver yet; see its own doc.) A listing entry under
/// `del/` that is neither a `.dreq` nor a `.done` is layout drift and fails
/// the pass loud, matching the resolver's and the rewrite pass's fail-loud
/// discipline for this keyspace.
pub async fn sweep_erasure_requests(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<ErasureRequestSweepOutcome> {
    let now = clock.now_ns();
    let prefix = keys::del_prefix(tenant, signal);
    let objects = list_all(store, &prefix).await?;

    // One LIST, split into pending requests and their completion timestamps.
    // Both suffixes share the `del/` prefix, so this needs no second listing.
    let mut dreq_keys: Vec<(Uuid, String)> = Vec::new();
    let mut done_completed_ns: HashMap<Uuid, i64> = HashMap::new();
    for meta in &objects {
        if let Ok(parsed) = keys::parse_erasure_request_key(&meta.key) {
            dreq_keys.push((parsed.request_id, meta.key.clone()));
        } else if let Ok(parsed) = keys::parse_erasure_completion_key(&meta.key) {
            let got = store.get(&meta.key, GetRange::Full).await?;
            let completion = ravel_commit::erasure::decode_completion(&got.data).map_err(|e| {
                MaintainError::Invariant(format!("erasure completion decode failed: {e}"))
            })?;
            keys::verify_erasure_completion_key(&completion, &meta.key)?;
            done_completed_ns.insert(parsed.request_id, completion.completed_unix_ns);
        } else {
            return Err(MaintainError::UnknownBucketEntry(meta.key.clone()));
        }
    }

    let mut deleted = 0usize;
    let mut kept = 0usize;
    for (request_id, dreq_key) in &dreq_keys {
        let Some(&completed_ns) = done_completed_ns.get(request_id) else {
            // No `.done`: the erasure is not verified complete, so the request
            // (and its query-time exclusion filter) must stay live.
            kept += 1;
            continue;
        };
        // Fail-safe: a zero completion timestamp is not a valid horizon anchor.
        if completed_ns == 0 {
            kept += 1;
            continue;
        }
        if now < completed_ns.saturating_add(config.protection_horizon_ns) {
            kept += 1;
            continue;
        }
        if lease.is_protected(dreq_key) {
            kept += 1;
            continue;
        }
        if !config.dry_run {
            store.delete(dreq_key).await?;
        }
        deleted += 1;
    }

    Ok(ErasureRequestSweepOutcome { deleted, kept })
}

// --- shared helpers --------------------------------------------------------

/// List a shard's commit prefix and classify every key by shape, failing loud
/// on any unknown shape (plan §3.1). Returns `(key, entry)` pairs.
///
/// `hours: None` lists the whole shard, across every hour, in one LIST (the
/// pre-zone-split behavior). `hours: Some(hs)` issues one LIST per hour in
/// `hs` against [`keys::commit_shard_hour_prefix`] instead, and concatenates
/// the results: an hour not in `hs` is never listed, which is the request-
/// count saving the zone split (ADR-0065 decision 3) exists for.
async fn list_commit_entries_scoped(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    hours: Option<&[u32]>,
) -> Result<Vec<(String, BucketEntry)>> {
    let metas = match hours {
        None => {
            let prefix = keys::commit_shard_prefix(tenant, signal, shard)?;
            list_all(store, &prefix).await?
        }
        Some(hs) => {
            let mut metas = Vec::new();
            for hour in hs {
                let prefix = keys::commit_shard_hour_prefix(tenant, signal, shard, *hour)?;
                metas.extend(list_all(store, &prefix).await?);
            }
            metas
        }
    };
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

/// List a shard's `l1/` objects. `hours: None` lists the whole shard in one
/// LIST via [`l1_prefix`] (the pre-zone-split behavior); `hours: Some(hs)`
/// issues one LIST per hour in `hs` against an hour-scoped prefix instead.
async fn list_l1_scoped(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    hours: Option<&[u32]>,
) -> Result<Vec<ObjectMeta>> {
    match hours {
        None => {
            let prefix = l1_prefix(tenant, signal, shard)?;
            Ok(list_all(store, &prefix).await?)
        }
        Some(hs) => {
            let mut metas = Vec::new();
            for hour in hs {
                let prefix = l1_hour_prefix(tenant, signal, shard, *hour)?;
                metas.extend(list_all(store, &prefix).await?);
            }
            Ok(metas)
        }
    }
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

/// `t/<tenant_hash_hex>/<signal>/l1/<shard>/<hour>/` -- the prefix covering
/// one hour's L1 part objects. L1 keys are hour-bucketed (unlike L0), so this
/// is the zone-scoped sweep's narrower alternative to [`l1_prefix`]; composed
/// from existing `pub` pieces (`l1_prefix`, `keys::ingest_hour_string`), no
/// new ravel-commit API needed.
fn l1_hour_prefix(tenant: &TenantHash, signal: Signal, shard: u32, hour: u32) -> Result<String> {
    Ok(format!(
        "{}{}/",
        l1_prefix(tenant, signal, shard)?,
        keys::ingest_hour_string(hour)
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
    use ravel_proto::catalog::v1::{SnapshotHead, SnapshotPartRef, SnapshotPostingsRef};
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

    // --- Rule 5: unreferenced catalog-object sweep (EH-T4, #741) -----------

    /// A [`LeaseCheck`] that protects any key under one prefix. Stands in for a
    /// real [`crate::legal_hold::LegalHoldCheck`] snapshot holding that prefix,
    /// exercising the sweep's per-delete lease gate without seeding audit
    /// records.
    struct HoldPrefix(String);

    impl LeaseCheck for HoldPrefix {
        fn is_protected(&self, key: &str) -> bool {
            key.starts_with(self.0.as_str())
        }
    }

    /// A part ref under the snap prefix. The sweep matches on `key` alone;
    /// `blake3` only has to be 32 bytes for `encode_head`'s own validation.
    fn part_ref(key: &str, blake3: [u8; 32]) -> SnapshotPartRef {
        SnapshotPartRef {
            key: key.to_string(),
            blake3: blake3.to_vec(),
            size: 1,
            entry_count: 1,
            watermark_hour: 100,
            min_hour: 0,
        }
    }

    /// Encode a valid single-or-multi-part HEAD and PUT it at the catalog HEAD
    /// key, so the sweep's `referenced_catalog_keys` GET returns it.
    async fn put_head(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        parts: Vec<SnapshotPartRef>,
        postings: Option<SnapshotPostingsRef>,
    ) {
        let watermark_hour = parts.iter().map(|p| p.watermark_hour).max().unwrap_or(0);
        let head = SnapshotHead {
            format_version: 1,
            tenant_hash: tenant.0.to_vec(),
            signal: 0,
            shard_count: 1,
            watermark_hour,
            parts,
            folder_id: vec![0u8; 16],
            created_unix_ns: 0,
            postings,
            shard_generation_count: 1,
        };
        let bytes = ravel_catalog::encode_head(&head).expect("valid HEAD encodes");
        store
            .put(
                &catalog_head_key(tenant, signal),
                Bytes::from(bytes),
                PutOptions::default(),
            )
            .await
            .expect("seed HEAD");
    }

    /// Seed a catalog object (snapshot part or postings) at `key` with whatever
    /// the store's current fake clock stamps as `last_modified`.
    async fn put_catalog_object(store: &dyn ObjectStoreBackend, key: &str) {
        store
            .put(
                key,
                Bytes::from_static(b"catalog-object"),
                PutOptions::default(),
            )
            .await
            .expect("seed catalog object");
    }

    /// Assert an object is present / absent in the store.
    async fn present(store: &dyn ObjectStoreBackend, key: &str) -> bool {
        store.get(key, GetRange::Full).await.is_ok()
    }

    /// The acceptance test (EH-T4): a snapshot part named by the current HEAD is
    /// spared even when it is far older than the protection horizon, and an
    /// unreferenced part younger than the horizon is spared by the age gate.
    /// Neither delete fires; both objects survive.
    #[tokio::test]
    async fn catalog_sweep_spares_referenced_and_young() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();
        let horizon = config.protection_horizon_ns;
        let now_ns = horizon.saturating_mul(2);

        // Old (store clock 0) referenced part, named by HEAD.
        let referenced_old = format!(
            "{}20260101T00.aaaa.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &referenced_old).await;
        put_head(
            &store,
            &tenant,
            signal,
            vec![part_ref(&referenced_old, [1u8; 32])],
            None,
        )
        .await;

        // Young (store clock at now) unreferenced part.
        store.set_clock_ms((now_ns / 1_000_000) as u64);
        let unreferenced_young = format!(
            "{}20260201T00.bbbb.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &unreferenced_young).await;

        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect("sweep must succeed");

        assert_eq!(outcome.deleted, 0, "nothing eligible: referenced or young");
        assert_eq!(outcome.kept, 2);
        assert!(
            present(&store, &referenced_old).await,
            "an old part the HEAD still names must never be swept"
        );
        assert!(
            present(&store, &unreferenced_young).await,
            "an unreferenced part younger than the horizon is spared by the age gate"
        );
    }

    /// An old, unreferenced snapshot part is swept; the referenced part the
    /// HEAD names is spared in the same pass.
    #[tokio::test]
    async fn catalog_sweep_deletes_old_unreferenced() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();
        let now_ns = config.protection_horizon_ns.saturating_mul(2);

        // Both seeded at store clock 0, so both are old.
        let referenced = format!(
            "{}20260101T00.aaaa.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        let superseded = format!(
            "{}20251231T00.cccc.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &referenced).await;
        put_catalog_object(&store, &superseded).await;
        put_head(
            &store,
            &tenant,
            signal,
            vec![part_ref(&referenced, [1u8; 32])],
            None,
        )
        .await;

        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect("sweep must succeed");

        assert_eq!(outcome.deleted, 1);
        assert_eq!(outcome.kept, 1);
        assert!(
            !present(&store, &superseded).await,
            "the old unreferenced part must be swept"
        );
        assert!(
            present(&store, &referenced).await,
            "the HEAD-named part must be spared"
        );
    }

    /// A postings object named by `HEAD.postings.key` is spared like a part ref;
    /// an unreferenced old postings object under `idx/` is swept.
    #[tokio::test]
    async fn catalog_sweep_spares_referenced_postings() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();
        let now_ns = config.protection_horizon_ns.saturating_mul(2);

        let part_blake3 = [7u8; 32];
        let part = format!(
            "{}20260101T00.aaaa.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        let referenced_postings = format!(
            "{}20260101T00.pppp.npost",
            catalog_idx_prefix(&tenant, signal)
        );
        let stale_postings = format!(
            "{}20251231T00.qqqq.npost",
            catalog_idx_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &part).await;
        put_catalog_object(&store, &referenced_postings).await;
        put_catalog_object(&store, &stale_postings).await;
        put_head(
            &store,
            &tenant,
            signal,
            vec![part_ref(&part, part_blake3)],
            Some(SnapshotPostingsRef {
                key: referenced_postings.clone(),
                blake3: [9u8; 32].to_vec(),
                size: 1,
                name_count: 1,
                part_blake3: vec![part_blake3.to_vec()],
            }),
        )
        .await;

        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect("sweep must succeed");

        assert_eq!(outcome.deleted, 1, "only the stale postings object");
        assert!(
            present(&store, &referenced_postings).await,
            "a postings object the HEAD names must be spared"
        );
        assert!(
            !present(&store, &stale_postings).await,
            "an old unreferenced postings object must be swept"
        );
        assert!(
            present(&store, &part).await,
            "the HEAD-named part is spared"
        );
    }

    /// A legal hold over the object's prefix spares an otherwise-eligible old
    /// unreferenced part, exactly as it does in every other sweep rule.
    #[tokio::test]
    async fn catalog_sweep_spares_legal_hold() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();
        let now_ns = config.protection_horizon_ns.saturating_mul(2);

        let referenced = format!(
            "{}20260101T00.aaaa.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        let held = format!(
            "{}20251231T00.cccc.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &referenced).await;
        put_catalog_object(&store, &held).await;
        put_head(
            &store,
            &tenant,
            signal,
            vec![part_ref(&referenced, [1u8; 32])],
            None,
        )
        .await;

        // Control: without the hold, `held` would be swept (proven by
        // `catalog_sweep_deletes_old_unreferenced`). With a hold over the whole
        // catalog prefix, it survives.
        let hold = HoldPrefix(format!("t/{}/catalog/", tenant.to_hex()));
        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &hold, &tenant, signal)
                .await
                .expect("sweep must succeed");

        assert_eq!(outcome.deleted, 0, "the hold blocks the delete");
        assert!(
            present(&store, &held).await,
            "a held object must never be swept"
        );
    }

    /// Under `dry_run`, the sweep counts what it would delete but calls
    /// `delete` on nothing.
    #[tokio::test]
    async fn catalog_sweep_dry_run_reports_without_deleting() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let store = MemoryStore::new();
        let config = CompactorConfig {
            dry_run: true,
            ..CompactorConfig::default()
        };
        let now_ns = config.protection_horizon_ns.saturating_mul(2);

        let referenced = format!(
            "{}20260101T00.aaaa.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        let superseded = format!(
            "{}20251231T00.cccc.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &referenced).await;
        put_catalog_object(&store, &superseded).await;
        put_head(
            &store,
            &tenant,
            signal,
            vec![part_ref(&referenced, [1u8; 32])],
            None,
        )
        .await;

        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect("dry run must not error");

        assert_eq!(
            outcome.deleted, 1,
            "dry run still counts what it would delete"
        );
        assert!(
            present(&store, &superseded).await,
            "dry_run must not actually delete"
        );
    }

    /// Finding 2: with no HEAD present, rule 5 sweeps NOTHING, even for an
    /// object far older than the horizon. An absent HEAD is the no-anchor case
    /// (mirroring rule 3's neither-record-nor-tombstone bucket): a recovery
    /// fold rebuilding from no HEAD recomputes and re-PUTs every part, adopting
    /// any surviving old object via `AlreadyExists` (which never rewrites it,
    /// so its `last_modified` stays old) and is about to name it in the HEAD it
    /// CASes. With no HEAD to compare against, an old record-less catalog
    /// object is indistinguishable from a part such a fold is mid-flight on, so
    /// deleting it could race that fold's CAS and orphan the new HEAD.
    #[tokio::test]
    async fn catalog_sweep_absent_head_sweeps_nothing() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();
        let now_ns = config.protection_horizon_ns.saturating_mul(2);

        // Old (store clock 0) object under snap/, with NO HEAD anywhere.
        let orphan = format!(
            "{}20251231T00.cccc.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &orphan).await;

        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect("an absent HEAD is not an error");

        assert_eq!(
            outcome.deleted, 0,
            "no HEAD is the no-anchor case: sweep nothing"
        );
        assert_eq!(outcome.kept, 1, "the old object is kept, not collected");
        assert!(
            present(&store, &orphan).await,
            "an old object with no HEAD to anchor the sweep must survive (a \
             recovery fold may be about to adopt and name it)"
        );
    }

    /// A minimal store wrapper that installs a replacement HEAD just before the
    /// Nth GET of the HEAD key, simulating a concurrent fold's HEAD CAS landing
    /// between the sweep's first HEAD read and its pre-delete re-verify read.
    /// Every other operation delegates straight to the inner [`MemoryStore`].
    struct HeadSwapStore {
        inner: MemoryStore,
        head_key: String,
        new_head: Bytes,
        swap_on_get: usize,
        head_gets: std::sync::atomic::AtomicUsize,
    }

    impl HeadSwapStore {
        fn new(inner: MemoryStore, head_key: String, new_head: Bytes, swap_on_get: usize) -> Self {
            HeadSwapStore {
                inner,
                head_key,
                new_head,
                swap_on_get,
                head_gets: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn head_get_count(&self) -> usize {
            self.head_gets.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for HeadSwapStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> std::result::Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> std::result::Result<ravel_object_store::GetOutcome, StoreError> {
            if key == self.head_key {
                let n = self
                    .head_gets
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                if n == self.swap_on_get {
                    // The fold's CAS lands: install the new HEAD that names the
                    // adopted-old object, immediately before the sweep's
                    // re-verify GET reads it.
                    self.inner
                        .put(&self.head_key, self.new_head.clone(), PutOptions::default())
                        .await
                        .expect("install swapped HEAD");
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

    /// Finding 1 + 3: an old, unreferenced object that a concurrent fold adopts
    /// via `AlreadyExists` (so its `last_modified` stays old) and names in a
    /// HEAD it CASes *after* the sweep's first HEAD read must NOT be deleted.
    /// The pre-delete re-verify GET of HEAD sees the fold's just-published HEAD
    /// and spares the object. Without the re-verify (the pre-fix single stale
    /// HEAD read), the object clears the horizon age gate and would be swept --
    /// its age says nothing about the in-flight fold, because adoption never
    /// rewrote it. The control is `catalog_sweep_deletes_old_unreferenced`: the
    /// same-shaped old unreferenced object IS deleted when no later HEAD names
    /// it.
    #[tokio::test]
    async fn catalog_sweep_reverify_spares_object_a_racing_fold_adopts() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let inner = MemoryStore::new();
        let config = CompactorConfig::default();
        let now_ns = config.protection_horizon_ns.saturating_mul(2);

        // Both objects seeded at store clock 0, so both are far older than the
        // horizon. `referenced` is named by the first HEAD; `adopted` is not.
        let referenced = format!(
            "{}20260101T00.aaaa.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        let adopted = format!(
            "{}20251231T00.cccc.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        put_catalog_object(&inner, &referenced).await;
        put_catalog_object(&inner, &adopted).await;
        // First HEAD: names only `referenced`, so `adopted` is an old
        // unreferenced candidate on the first read.
        put_head(
            &inner,
            &tenant,
            signal,
            vec![part_ref(&referenced, [1u8; 32])],
            None,
        )
        .await;

        // The fold's post-CAS HEAD, installed at the re-verify GET: it names
        // the adopted old object (a single-part HEAD is enough -- the
        // re-verify only re-checks the surviving candidate, which is
        // `adopted`; `referenced` was already counted kept on the first read).
        let swapped_head = {
            let head = SnapshotHead {
                format_version: 1,
                tenant_hash: tenant.0.to_vec(),
                signal: 0,
                shard_count: 1,
                watermark_hour: 100,
                parts: vec![part_ref(&adopted, [2u8; 32])],
                folder_id: vec![0u8; 16],
                created_unix_ns: 0,
                postings: None,
                shard_generation_count: 1,
            };
            Bytes::from(ravel_catalog::encode_head(&head).expect("valid swapped HEAD"))
        };

        // swap_on_get == 2: the new HEAD is installed just before the sweep's
        // second HEAD GET, which is the pre-delete re-verify.
        let store = HeadSwapStore::new(inner, catalog_head_key(&tenant, signal), swapped_head, 2);

        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect("sweep must succeed");

        assert_eq!(
            store.head_get_count(),
            2,
            "HEAD is read twice: once for the reference set, once to re-verify \
             immediately before deleting"
        );
        assert_eq!(
            outcome.deleted, 0,
            "the object the fold's re-verify-time HEAD names is spared"
        );
        assert!(
            present(&store, &adopted).await,
            "an old object a racing fold adopted and named must not be swept"
        );
        assert!(present(&store, &referenced).await);
    }

    /// Finding 3: the pre-delete re-verify GET of HEAD actually happens. Fault
    /// the second HEAD GET (the re-verify) and the pass aborts before any
    /// delete, exactly as rule 1's and rule 3's re-verify-fault tests prove for
    /// their re-verify LIST. Mirrors
    /// `batched_reverify_lists_commit_prefix_once_per_pass`.
    #[tokio::test]
    async fn catalog_sweep_reverify_head_get_faults_before_delete() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let mem = MemoryStore::new();
        let config = CompactorConfig::default();
        let now_ns = config.protection_horizon_ns.saturating_mul(2);

        let referenced = format!(
            "{}20260101T00.aaaa.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        let superseded = format!(
            "{}20251231T00.cccc.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        put_catalog_object(&mem, &referenced).await;
        put_catalog_object(&mem, &superseded).await;
        put_head(
            &mem,
            &tenant,
            signal,
            vec![part_ref(&referenced, [1u8; 32])],
            None,
        )
        .await;

        // The second GET of the HEAD key is the batched re-verify: faulting it
        // aborts the pass before any delete, proving it happens exactly once
        // (not zero times) between candidate selection and the delete loop.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Timeout)
                .with_key_contains("/HEAD")
                .with_occurrence(Occurrence::Nth(2)),
        );
        let store = FaultStore::new(mem, plan);

        let clock = FixedClock::new(now_ns);
        let err =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect_err("the re-verify HEAD GET faults the pass");
        assert!(matches!(err, MaintainError::Store(_)), "got: {err:?}");
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Timeout),
            1,
            "exactly the re-verify HEAD GET faulted"
        );
        assert!(
            present(&store, &superseded).await,
            "nothing deleted when the re-verify HEAD GET faults"
        );
    }

    /// A HEAD that is present but does not decode aborts the pass with an error
    /// and deletes nothing: a corrupt HEAD must never make the live snapshot
    /// look unreferenced.
    #[tokio::test]
    async fn catalog_sweep_corrupt_head_deletes_nothing() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();
        let now_ns = config.protection_horizon_ns.saturating_mul(2);

        let part = format!(
            "{}20251231T00.cccc.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &part).await;
        store
            .put(
                &catalog_head_key(&tenant, signal),
                Bytes::from_static(b"not a valid HEAD"),
                PutOptions::default(),
            )
            .await
            .expect("seed corrupt HEAD");

        let clock = FixedClock::new(now_ns);
        let err =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect_err("a corrupt HEAD must fail the pass, not sweep the snapshot");
        assert!(matches!(err, MaintainError::Invariant(_)), "got: {err:?}");
        assert!(
            present(&store, &part).await,
            "no object is deleted when the HEAD cannot be decoded"
        );
    }
}
