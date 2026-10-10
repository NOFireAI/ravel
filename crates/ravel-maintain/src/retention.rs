//! Age-based retention (ADR-0019): the
//! second deletion trigger, and the first that destroys data rather than a
//! redundant copy of it.
//!
//! The flow per sealed bucket is the shape the consistency-model already
//! promises: a durable tombstone transaction, then bucket-wide exclusion from
//! new snapshots (the resolver's job, ravel-catalog), then a horizon-gated
//! physical sweep here.
//!
//! 1. **Expiry evaluation** decodes the bucket's already-listed commit,
//!    compaction, and selective-erasure rewrite records and takes
//!    `max(max_event_ts_ns)` across all of them (no footer reads). A bucket is
//!    expired when it is sealed and that maximum is `< now - R`, so no sample
//!    younger than `R` is ever excluded (ADR-0019 decision 1; the
//!    impossibility floor). ADR-0019 decision 1 names only L0 commit records
//!    and compaction records because selective erasure (ADR-0064) postdates
//!    it; a rewrite record is a live record set exactly as a compaction record
//!    is, and rewrite-record-only is the durable steady state of an erased
//!    bucket once the superseded-input sweep has removed its inputs, so
//!    omitting it retained erased-and-rewritten data past `R` forever
//!    (issue #1321).
//! 2. **Tombstone** is written `CreateIfAbsent` at the fixed per-bucket key
//!    with an injected `retired_at_ns`. It is durable and irreversible:
//!    raising `R` later never resurrects a tombstoned bucket (ADR-0019
//!    decision 2).
//! 3. **Legal-hold gate** (ADR-0042 decision 2) is the first gate in the
//!    physical sweep, before the version probe below, so a held bucket costs
//!    no suffix GETs. It is all-or-nothing over the bucket: every key the pass
//!    would delete is offered to the [`LeaseCheck`], and if any one of them is
//!    protected the pass deletes nothing, leaves the tombstone in place,
//!    counts the hold, and reports `SweptPartial`. Retention's deletes are one
//!    retirement, not a set of independent deletes: the commit records and
//!    the tombstone are what make the bucket's data objects discoverable and
//!    sweepable, so deleting the unheld part of a held bucket loses the held
//!    bytes by a slower route (issue #1697). A bucket that is both legally
//!    held and version-held therefore counts on the legal-hold counter.
//! 4. **Version hold** (ADR-0066 decisions 1 and 2) runs next, still before
//!    any delete. Each data object the sweep is about to delete is
//!    probed for its trailer version through a 16-byte suffix GET, through
//!    the gate of the bucket's own format (RSEG for metrics, RLOG for logs,
//!    RSPAN for spans), and the answer is a typed classification, never a
//!    string: readable here, outside this build's reader window, or corrupt. An object outside the window is
//!    not garbage -- a peer running the other side of a rolling upgrade, or the
//!    build a rollback returns to, reads it normally -- so the sweep declines to
//!    delete anything in that bucket this pass, leaves the tombstone in place,
//!    counts the hold, and reports `SweptPartial`. A corrupt object is swept as
//!    before: no build can read it, holding it protects nothing. This narrows
//!    ADR-0066 decision 4's "retention ages old-version objects out" to objects
//!    this build can actually read; see that ADR's 2026-09-13 amendment.
//! 5. **Physical sweep** runs once `now >= retired_at_ns + protection_horizon`,
//!    deleting in the fixed order L0 commit records, compaction records,
//!    rewrite records, L0 data objects, L1 parts, then the tombstone last, and
//!    only after a verifying LIST shows the bucket's commit prefix holds only
//!    the tombstone and its `l1/` prefix is empty. Any residue leaves the
//!    tombstone in place for the next pass (ADR-0019 decision 4). The rewrite
//!    record is deleted with the other records for the same reason it is read
//!    during expiry evaluation: without it the verifying LIST always found
//!    residue and the sweep never got past `SweptPartial` (issue #1321).
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

use std::sync::atomic::{AtomicU64, Ordering};

use prost::Message;
use ravel_commit::erasure;
use ravel_commit::keys;
use ravel_commit::record;
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum, list_all,
};
use ravel_proto::commit::v1::{CommitRecord, CompactionRecord, RetentionTombstone, RewriteRecord};
use ravel_segment::{TRAILER_LEN, TrailerClass, classify_trailer};
use ravel_types::{Signal, TenantHash};

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::compact::{
    ClaimAcquisition, ClaimedCompaction, compact_bucket_claimed_with_acquisition,
};
use crate::config::{CompactorConfig, RetentionConfig};
use crate::error::{MaintainError, Result};
use crate::reachability::{MarkerContext, MarkerPolicy, SnapshotGate};
use crate::read::{BucketListing, list_bucket, verify_commit_key};
use crate::sweep::LeaseCheck;
use crate::unnamed_marker::{MarkerAnchor, MarkerKind};

/// The HEAD-reachability delete blocker, shared with the superseded-input
/// sweep (see [`crate::reachability`]). Re-exported at this path because
/// retention was its first caller and the maintain crate's public surface
/// names it here.
pub use crate::reachability::{SnapshotBlock, SnapshotReachability};

/// Counter seam for `ravel_maintain_retention_held_out_of_window_objects_total`
/// (ADR-0066 decisions 1 and 2): data objects the physical sweep declined to
/// delete because their trailer version is outside this build's reader window.
///
/// Process-wide and monotonic, incremented once per object per pass that
/// declined it, so a nonzero rate (not just a nonzero total) is the signal: it
/// means a deployment is refusing deletes right now because it is holding
/// objects some other build can read. Holding is the safe answer to an
/// unfinished rolling upgrade, and it is also the only way retention can retain
/// data past its window, so this must not stay nonzero: the remedy is to finish
/// the upgrade, complete `maintain migrate`, or roll back, after which the next
/// pass sweeps the bucket normally.
static HELD_OUT_OF_WINDOW_OBJECTS: AtomicU64 = AtomicU64::new(0);

/// Read [`HELD_OUT_OF_WINDOW_OBJECTS`]. Zero on a healthy deployment.
pub fn held_out_of_window_objects_total() -> u64 {
    HELD_OUT_OF_WINDOW_OBJECTS.load(Ordering::Relaxed)
}

/// Counter seam for `ravel_maintain_retention_held_by_lease_buckets_total`
/// (ADR-0042 decision 2): tombstoned buckets the physical sweep declined to
/// touch this pass because a [`LeaseCheck`] protects at least one key the pass
/// would have deleted. Legal hold ([`crate::legal_hold::LegalHoldCheck`]) is
/// the production implementation of that seam, so in practice this counts
/// buckets parked by a hold.
///
/// Process-wide and monotonic, incremented once per bucket per pass that
/// declined it. It counts buckets rather than keys because the gate is
/// all-or-nothing: one protected key parks the whole bucket, and the number of
/// keys under it says nothing about how many retirements are stalled.
///
/// Unlike [`HELD_OUT_OF_WINDOW_OBJECTS`] a nonzero rate here is not by itself a
/// fault: a hold is deliberate, and this stays nonzero for as long as the hold
/// stands. It exists so the stall is visible at all, because a bucket parked in
/// [`RetentionOutcome::SweptPartial`] is a bucket kept past its retention
/// window, and an operator has to be able to see which holds are doing that and
/// for how long. The total goes flat again once the hold is cleared and the
/// next pass retires the bucket.
static HELD_BY_LEASE_BUCKETS: AtomicU64 = AtomicU64::new(0);

/// Read [`HELD_BY_LEASE_BUCKETS`]. Zero when no hold covers a bucket whose
/// physical sweep is otherwise due.
pub fn held_by_lease_buckets_total() -> u64 {
    HELD_BY_LEASE_BUCKETS.load(Ordering::Relaxed)
}

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
    /// Tombstone present and horizon elapsed, but the bucket was not emptied,
    /// for one of three reasons: a verifying LIST still found residue (a delete
    /// lost to a concurrent write); the sweep declined to delete anything
    /// because at least one data object's format version is outside this
    /// build's reader window (ADR-0066, see
    /// [`held_out_of_window_objects_total`]); or the sweep declined to delete
    /// anything because a [`LeaseCheck`], in production a legal hold, protects
    /// at least one key the pass would have deleted (ADR-0042 decision 2, see
    /// [`held_by_lease_buckets_total`]). In all three the tombstone was left in
    /// place for the next pass to finish.
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

/// Resolve a tenant's effective retention window the same way the catalog fold
/// does (ADR-0078): overlay the durable per-tenant `TenantConfig.retention_ns`
/// on the deployment default that `RetentionConfig::window_for` yields (the
/// `--retention-tenant` per-tenant override if set, else `--retention-default`).
/// The durable record is read from the same object store, under the same key,
/// through the same decoder the fold uses
/// ([`ravel_catalog::read_config_values`]), and the precedence is the single one
/// stated in [`ravel_catalog::resolve_retention_window`] -- durable record wins,
/// then CLI per-tenant override, then CLI default -- so the sweep and the fold
/// never resolve different windows for the same tenant. Without this the sweep
/// read only the CLI map and would tombstone an hour a longer durable window
/// keeps, which the fold's frontier reconcile never drops, stalling the physical
/// sweep on a repeating `BlockedBySnapshot`.
///
/// A config-read fault fails the resolution closed (propagates the error) rather
/// than falling back to the possibly shorter CLI window: a tombstone is
/// irreversible, so a transient store fault must never shorten the window and
/// retire a bucket the durable record would keep. The sweep is idempotent, so
/// the pass simply retries on the next tick.
pub async fn resolve_retention_window_ns(
    store: &dyn ObjectStoreBackend,
    retention: &RetentionConfig,
    tenant: &TenantHash,
) -> Result<Option<i64>> {
    let default_retention_ns = retention.window_for(tenant);
    let tenant_config = ravel_catalog::read_config_values(store, tenant)
        .await
        .map_err(|e| {
            MaintainError::Invariant(format!(
                "tenant config read failed while resolving retention window for {}: {e}",
                tenant.to_hex()
            ))
        })?;
    Ok(ravel_catalog::resolve_retention_window(
        tenant_config.as_ref(),
        default_retention_ns,
    ))
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
    let window_ns = resolve_retention_window_ns(store, retention, &bucket.tenant_hash).await?;
    let mut reach = SnapshotReachability::new().with_read_gate(config.read_gate.clone());
    let outcome = retention_sweep_bucket_with_reach(
        &mut reach, store, clock, config, window_ns, lease, bucket,
    )
    .await?;
    reach
        .reap_after_pass(
            store,
            clock,
            config,
            &bucket.tenant_hash,
            bucket.signal,
            false,
        )
        .await;
    Ok(outcome)
}

/// [`retention_sweep_bucket`] with a caller-owned [`SnapshotReachability`]
/// cache persisted across the buckets of one sweep pass, so the HEAD-
/// reachability gate reads HEAD at most once and each covering snapshot part at
/// most once per pass rather than once per bucket (ADR-0076 request cost). The
/// public [`retention_sweep_bucket`] wraps this with a fresh per-call cache;
/// [`crate::scan::scan_and_maintain_with_memo`] owns one cache for the whole
/// per-(tenant, signal, shard) pass and calls this directly.
///
/// `window_ns` is the tenant's effective retention window already resolved by
/// [`resolve_retention_window_ns`] (durable record over CLI, ADR-0078). The
/// caller resolves it once per pass -- the window is constant across a tenant's
/// buckets, so reading the durable config per bucket would be one redundant GET
/// per bucket -- and threads the same value into every bucket of the pass, so
/// the sweep, the pass's zone classification, and the fold all use one window.
/// `None` means no retention policy for this tenant.
#[allow(clippy::too_many_arguments)]
pub async fn retention_sweep_bucket_with_reach(
    reach: &mut SnapshotReachability,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    window_ns: Option<i64>,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
) -> Result<RetentionOutcome> {
    let (outcome, _expiry) = retention_sweep_bucket_observed(
        reach,
        store,
        clock,
        config,
        window_ns,
        lease,
        bucket,
        RewriteBound::Unread,
    )
    .await?;
    Ok(outcome)
}

/// When an expired bucket expired, as far as one retention evaluation knows
/// it without a request beyond the ones the evaluation already made. Feeds the
/// scan's retention lag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObservedExpiry {
    /// `max_event_ts + window` from the bucket's own records, which the pass
    /// that writes the tombstone reads to decide expiry.
    Exact(i64),
    /// The tombstone's `retired_at_ns`. The tombstone records no event
    /// timestamp, but it is written only once the bucket has expired, so the
    /// expiry is at or before this instant.
    NoLaterThan(i64),
    /// The tombstone's `retired_at_ns`, for a bucket that lists a rewrite
    /// record with no parts, or one this evaluation did not read or failed to
    /// read. A parts-less
    /// rewrite puts its `created_unix_ns` into [`max_event_ts`], and an erasure
    /// can publish one at any time before the bucket expires, so the hour's
    /// nominal deadline is no bound on the expiry here; only this instant is.
    NoLaterThanOnly(i64),
}

/// How a tombstoned bucket's evaluation learns whether its listed rewrite
/// records keep the hour's nominal deadline as an expiry bound, which only a
/// parts-less one does not.
pub(crate) enum RewriteBound<'a> {
    /// Read nothing: any listed rewrite leaves the tombstone as the only bound.
    Unread,
    /// The answer an earlier evaluation of the same bucket read. Rewrite
    /// records are immutable, and an erasure checks for the tombstone before
    /// it rewrites a bucket, so the answer is reused for the bucket's
    /// tombstoned life, including after a partial sweep has deleted its
    /// rewrite records. A rewrite published in the race between that check
    /// and another replica's tombstone write is not seen; its publish time
    /// is after the tombstone, so the expiry the tombstone acted on stands.
    Known(bool),
    /// Read the listed records until the first parts-less one.
    Read(&'a mut RewriteBoundRead),
}

/// What one [`RewriteBound::Read`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RewriteBoundRead {
    /// Rewrite-record GETs issued.
    pub(crate) gets: usize,
    /// The answer, when every GET it needed succeeded. `None` when the bucket
    /// listed no rewrite, or a read failed and the evaluation fell back to the
    /// tombstone-only bound.
    pub(crate) learned: Option<bool>,
}

/// [`retention_sweep_bucket_with_reach`], also returning the bucket's
/// [`ObservedExpiry`] on every outcome that leaves an expired bucket present
/// or retires it, and `None` on the rest.
///
/// For a tombstoned bucket that lists rewrite records, `rewrite_bound` decides
/// the bound: a parts-less rewrite makes it
/// [`ObservedExpiry::NoLaterThanOnly`], and rewrites that all keep parts make
/// it [`ObservedExpiry::NoLaterThan`]. [`RewriteBound::Unread`] reads nothing
/// and returns [`ObservedExpiry::NoLaterThanOnly`], the bound that holds
/// without the read; the scan passes it when it already holds the bucket's
/// exact expiry. The read only feeds the retention lag, so a failed one falls
/// back to that same bound and never fails the evaluation.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn retention_sweep_bucket_observed(
    reach: &mut SnapshotReachability,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    window_ns: Option<i64>,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
    rewrite_bound: RewriteBound<'_>,
) -> Result<(RetentionOutcome, Option<ObservedExpiry>)> {
    let Some(window_ns) = window_ns else {
        return Ok((RetentionOutcome::NoPolicy, None));
    };
    let now = clock.now_ns();
    if !bucket.is_sealed(now, config) {
        return Ok((RetentionOutcome::NotSealed, None));
    }

    let listing = list_bucket(store, bucket).await?;

    // Already tombstoned: the only remaining work is the horizon-gated physical
    // sweep. Anchored on the durable retired_at_ns, exactly as supersession
    // anchors on the compaction record's created_unix_ns.
    if let Some(tombstone_key) = &listing.tombstone_key {
        let (tombstone, tombstone_version) = get_tombstone_versioned(store, tombstone_key).await?;
        let nominal_bounds =
            rewrites_keep_nominal_bound(store, bucket, &listing.rewrite_record_keys, rewrite_bound)
                .await;
        let expiry = Some(if nominal_bounds {
            ObservedExpiry::NoLaterThan(tombstone.retired_at_ns)
        } else {
            ObservedExpiry::NoLaterThanOnly(tombstone.retired_at_ns)
        });
        if now
            >= tombstone
                .retired_at_ns
                .saturating_add(config.protection_horizon_ns)
        {
            let anchor = MarkerAnchor {
                kind: MarkerKind::Retention,
                key: tombstone_key.clone(),
                anchor_unix_ns: tombstone.retired_at_ns,
                version: tombstone_version,
            };
            let outcome = physical_sweep(
                reach, store, clock, config, lease, bucket, &listing, &anchor,
            )
            .await?;
            return Ok((outcome, expiry));
        }
        return Ok((RetentionOutcome::Tombstoned, expiry));
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
    let mut rewrite_records = Vec::with_capacity(listing.rewrite_record_keys.len());
    for key in &listing.rewrite_record_keys {
        rewrite_records.push(get_rewrite_record(store, key).await?);
    }
    let max_event = max_event_ts(&commit_records, &compaction_records, &rewrite_records);
    if !is_expired(max_event, now, window_ns) {
        return Ok((RetentionOutcome::NotExpired, None));
    }

    write_tombstone(store, bucket, now, window_ns, &listing, config.dry_run).await?;
    let expiry = max_event.map(|max| ObservedExpiry::Exact(max.saturating_add(window_ns)));
    Ok((RetentionOutcome::Tombstoned, expiry))
}

/// Run retention before compaction over one bucket (ADR-0019 decision 6): the
/// retention check runs first, so an expired bucket is tombstoned and never
/// compacted. Compaction runs only when retention leaves the bucket live
/// (no policy / not sealed / not expired). Returns the retention outcome and
/// the compaction outcome, if compaction ran.
///
/// The compaction goes through [`compact_bucket_claimed`], so a caller that
/// installed a [`crate::config::ClaimParticipant`] takes the bucket's advisory
/// claim here and a caller that did not runs exactly as before (ADR-1029
/// decision 5).
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
) -> Result<(RetentionOutcome, Option<ClaimedCompaction>)> {
    let window_ns = resolve_retention_window_ns(store, retention, &bucket.tenant_hash).await?;
    let mut reach = SnapshotReachability::new().with_read_gate(config.read_gate.clone());
    let (outcome, compaction, _acquisition) =
        maintain_bucket_with_reach(&mut reach, store, clock, config, window_ns, lease, bucket)
            .await?;
    reach
        .reap_after_pass(
            store,
            clock,
            config,
            &bucket.tenant_hash,
            bucket.signal,
            false,
        )
        .await;
    Ok((outcome, compaction))
}

/// [`maintain_bucket`] with a caller-owned [`SnapshotReachability`] cache
/// shared across the buckets of one sweep pass (ADR-0076 request cost, see
/// [`retention_sweep_bucket_with_reach`]). The public [`maintain_bucket`] wraps
/// this with a fresh per-call cache. `window_ns` is the pass-resolved retention
/// window (see [`retention_sweep_bucket_with_reach`]).
#[allow(clippy::too_many_arguments)]
pub async fn maintain_bucket_with_reach(
    reach: &mut SnapshotReachability,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    window_ns: Option<i64>,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
) -> Result<(
    RetentionOutcome,
    Option<ClaimedCompaction>,
    Option<ClaimAcquisition>,
)> {
    let (outcome, _expiry, compaction, acquisition) = maintain_bucket_observed(
        reach,
        store,
        clock,
        config,
        window_ns,
        lease,
        bucket,
        RewriteBound::Unread,
    )
    .await?;
    Ok((outcome, compaction, acquisition))
}

/// [`maintain_bucket_with_reach`], also returning the retention evaluation's
/// [`ObservedExpiry`] (see [`retention_sweep_bucket_observed`], which takes
/// `rewrite_bound`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn maintain_bucket_observed(
    reach: &mut SnapshotReachability,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    window_ns: Option<i64>,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
    rewrite_bound: RewriteBound<'_>,
) -> Result<(
    RetentionOutcome,
    Option<ObservedExpiry>,
    Option<ClaimedCompaction>,
    Option<ClaimAcquisition>,
)> {
    let (outcome, expiry) = retention_sweep_bucket_observed(
        reach,
        store,
        clock,
        config,
        window_ns,
        lease,
        bucket,
        rewrite_bound,
    )
    .await?;
    let (compaction, acquisition) = match outcome {
        // The bucket is (or is being) retired, or its delete is blocked by a
        // still-reaching snapshot: never compact it.
        RetentionOutcome::Tombstoned
        | RetentionOutcome::Swept
        | RetentionOutcome::SweptPartial
        | RetentionOutcome::BlockedBySnapshot(_) => (None, None),
        RetentionOutcome::NoPolicy | RetentionOutcome::NotSealed | RetentionOutcome::NotExpired => {
            let (compaction, acquisition) =
                compact_bucket_claimed_with_acquisition(store, clock, config, bucket).await?;
            (Some(compaction), acquisition)
        }
    };
    Ok((outcome, expiry, compaction, acquisition))
}

/// The maximum `max_event_ts_ns` across a bucket's L0 commit records,
/// compaction-record parts, and rewrite-record parts. `None` when the bucket
/// holds no records.
///
/// Every record kind `keys::partition_bucket_entry` can classify inside a
/// bucket's commit prefix contributes here except the tombstone (whose own
/// presence short-circuits expiry evaluation entirely). A rewrite record left
/// out of this maximum is a bucket that can never expire: once the
/// protection-horizon sweep has removed a rewrite's superseded inputs, the
/// rewrite record is the bucket's whole live record set (issue #1321).
///
/// A rewrite record with no parts (an erasure that dropped every record in the
/// bucket, which `RewriteRecord.parts` explicitly permits) carries no event
/// timestamp at all, so it contributes its own `created_unix_ns` instead. It
/// holds no sample, so no sample younger than `R` can hide behind it, and
/// anchoring on the instant the record was published is what keeps such a
/// bucket expirable rather than permanently retained metadata.
pub fn max_event_ts(
    commit_records: &[CommitRecord],
    compaction_records: &[CompactionRecord],
    rewrite_records: &[RewriteRecord],
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
    for rec in rewrite_records {
        if rec.parts.is_empty() {
            bump(rec.created_unix_ns);
            continue;
        }
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
    // Every record kind the bucket can hold counts, rewrite records included:
    // `record_count_observed` is the audit evidence for what was in the bucket
    // when it was retired, and a rewrite-record-only bucket reporting zero
    // records observed reads as an empty bucket that was never written to.
    let record_count = (listing.commit_keys.len()
        + listing.compaction_record_keys.len()
        + listing.rewrite_record_keys.len()) as u64;
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
/// fixed order L0 commit records, compaction records, rewrite records, L0 data
/// objects, L1 parts, then the tombstone last, and only after a verifying LIST
/// shows the bucket's commit prefix holds only the tombstone and its `l1/`
/// prefix is empty. Any residue leaves the tombstone in place.
///
/// The record deletes cover every non-tombstone shape
/// `keys::partition_bucket_entry` classifies in the commit prefix, which is
/// the same set [`bucket_is_empty_but_tombstone`] refuses to call empty: a
/// shape deleted by neither is residue forever (issue #1321).
///
/// The bucket's unnamed-since marker (ADR-1133) is deleted after the bucket
/// is verified empty and before the tombstone, so a crash in between leaves
/// a tombstone whose next pass starts a fresh window over nothing.
#[allow(clippy::too_many_arguments)]
async fn physical_sweep(
    reach: &mut SnapshotReachability,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
    listing: &BucketListing,
    anchor: &MarkerAnchor,
) -> Result<RetentionOutcome> {
    let tombstone_key = anchor.key.as_str();
    let dry_run = config.dry_run;
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
    //
    // A clear HEAD answer then waits for the bucket's unnamed-since marker to
    // age past the pinned-query window (ADR-1133): a query that resolved a
    // HEAD from before the fold dropped this bucket may still be reading it.
    // A dry run reads the marker and writes none.
    let head_gate = reach.bucket_gate(store, bucket).await?;
    let marker_key = keys::retention_unnamed_marker_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )?;
    let policy = if dry_run {
        MarkerPolicy::DryRun
    } else {
        MarkerPolicy::Write
    };
    let ctx = MarkerContext::new(clock, config, policy);
    match reach
        .marker_gate(
            store,
            &ctx,
            &bucket.tenant_hash,
            bucket.signal,
            &marker_key,
            anchor,
            head_gate,
        )
        .await
    {
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
    // Rewrite output parts need no separate resolution step: ADR-0064 decision
    // 3 point 2 PUTs them under the same `l1/` part-key shape a compaction
    // record's parts use, so the fresh LIST below already covers them.
    //
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

    // Legal-hold gate (ADR-0042 decision 2), all-or-nothing over the bucket and
    // ahead of every delete. `delete_all` below skips a protected key one key at
    // a time, which is the right rule for a sweep whose deletes are independent
    // of each other. Retention's are not: the commit records name the L0 data
    // objects, the tombstone is what keeps the bucket excluded from snapshots,
    // and the three are one retirement. Deleting the unheld part of a held
    // bucket leaves the held bytes with no record naming them and no tombstone
    // covering them, which loses the data a hold exists to preserve by a slower
    // route and is unrecoverable once done. So the pass asks about every key it
    // would delete and deletes nothing if any one of them is protected.
    //
    // Declining is the conservative direction in both failure modes: a hold
    // wrongly reported here costs a stalled retirement that the next pass
    // completes once the hold clears, while a hold wrongly missed destroys held
    // data. The bucket keeps its tombstone, so it stays excluded from snapshots
    // and the retirement resumes rather than restarting.
    //
    // This runs before the version probe below so a held bucket costs no suffix
    // GETs: the answer needs no I/O, and both gates return the same outcome.
    let protected =
        protected_sweep_keys(lease, listing, &l0_data_keys, &l1_part_keys, tombstone_key);
    if let Some(first) = protected.first() {
        HELD_BY_LEASE_BUCKETS.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            tenant = %bucket.tenant_hash.to_hex(),
            signal = ?bucket.signal,
            shard = bucket.shard,
            ingest_hour_bucket = bucket.ingest_hour_bucket,
            protected_keys = protected.len(),
            first_protected_key = first,
            "retention sweep held a tombstoned bucket: a lease or legal hold protects keys this pass would delete"
        );
        return Ok(RetentionOutcome::SweptPartial);
    }

    // Version hold (ADR-0066 decisions 1 and 2): before deleting anything,
    // refuse if any data object about to be deleted carries a format version
    // outside this build's reader window. Such an object is readable by the
    // other side of a rolling upgrade and by the build a rollback returns to,
    // so deleting it destroys data that is only unreadable HERE. The hold
    // covers the whole bucket rather than the individual object: a commit
    // record deleted while the object it names survives leaves that object
    // undiscoverable, which loses the data by a slower route.
    let held = held_out_of_window(store, bucket, &l0_data_keys, &l1_part_keys).await?;
    if !held.is_empty() {
        HELD_OUT_OF_WINDOW_OBJECTS.fetch_add(held.len() as u64, Ordering::Relaxed);
        tracing::warn!(
            tenant = %bucket.tenant_hash.to_hex(),
            signal = ?bucket.signal,
            shard = bucket.shard,
            ingest_hour_bucket = bucket.ingest_hour_bucket,
            held_objects = held.len(),
            versions = ?held.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            "retention sweep held a tombstoned bucket: format versions outside this build's reader window"
        );
        return Ok(RetentionOutcome::SweptPartial);
    }

    // Deletion order (docs/consistency-model.md "Deletion and GC", ADR-0019
    // decision 4): records, then data objects, then L1 parts, tombstone last.
    delete_all(store, lease, &listing.commit_keys, dry_run).await?;
    delete_all(store, lease, &listing.compaction_record_keys, dry_run).await?;
    delete_all(store, lease, &listing.rewrite_record_keys, dry_run).await?;
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
    if lease.is_protected(tombstone_key) {
        return Ok(RetentionOutcome::SweptPartial);
    }
    // The marker goes before the tombstone (ADR-1133 decision 6). A marker
    // delete that fails keeps the tombstone, so the bucket stays excluded and a
    // later pass retries both.
    if !dry_run && reach.retire_marker(store, &marker_key).await.is_err() {
        return Ok(RetentionOutcome::SweptPartial);
    }
    if !dry_run {
        store.delete(tombstone_key).await?;
    }
    Ok(RetentionOutcome::Swept)
}

/// The data objects of one bucket whose format version is outside this build's
/// reader window, as `(key, version)` pairs in probe order.
///
/// The distinction this draws is the whole point (ADR-0066 decision 2): a
/// version this build does not admit and bytes no build can read are both
/// "cannot read this", and collapsing them turns a rolling upgrade into data
/// loss. It is drawn from each format's own trailer gate, the same one a full
/// read applies, so this cannot disagree with the reader about which versions
/// are admitted. A corrupt object is deliberately NOT held: no build reads it,
/// so holding it keeps nothing alive and would only stall the bucket forever.
///
/// The bucket's signal picks the gate: RSEG for metrics, RLOG for logs, RSPAN
/// for spans. Each format keeps its own trailer and its own window, and probing
/// one with another's gate would call every object corrupt, which is the exact
/// collapse this function exists to prevent. Any other signal is swept
/// unprobed. Audit and alerts also write RLOG, but neither reaches this sweep:
/// audit objects age out through `sweep_audit_retention`, which applies no
/// version hold, and alerts is not a maintained signal.
///
/// The version comes from the trailer, not from the commit record's
/// `segment_format_version`: that field is a writer's stamp no reader checks
/// against the bytes, and the L1 parts are found by LIST, not through a record.
///
/// Cost: one 16-byte suffix GET per data object (L0 and L1), charged to the
/// sweep phase, and only in the pass that would delete (after the tombstone's
/// protection horizon has elapsed and the HEAD-reachability and legal-hold
/// gates are clear). The GET also returns the object's total size, so no
/// separate HEAD is needed, and the gate never asks for the footer.
async fn held_out_of_window(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    l0_data_keys: &[String],
    l1_part_keys: &[String],
) -> Result<Vec<(String, u16)>> {
    let Some(format) = ProbedFormat::for_signal(bucket.signal) else {
        return Ok(Vec::new());
    };
    let mut held = Vec::new();
    for key in l0_data_keys.iter().chain(l1_part_keys.iter()) {
        if let Some(version) = out_of_window_version(store, format, key).await? {
            held.push((key.clone(), version));
        }
    }
    Ok(held)
}

/// The data-object format whose trailer gate the version hold applies.
///
/// One suffix GET of RSEG's [`TRAILER_LEN`] serves all three: the assertion
/// below fails the build if RLOG's or RSPAN's trailer ever differs in length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbedFormat {
    Rseg,
    Rlog,
    Rspan,
}

const _: () = assert!(
    ravel_logseg::footer::TRAILER_LEN as u64 == TRAILER_LEN
        && ravel_rspan::footer::TRAILER_LEN as u64 == TRAILER_LEN
);

impl ProbedFormat {
    fn for_signal(signal: Signal) -> Option<Self> {
        match signal {
            Signal::Metrics => Some(Self::Rseg),
            Signal::Logs => Some(Self::Rlog),
            Signal::Spans => Some(Self::Rspan),
            _ => None,
        }
    }

    /// The trailer version when `tail` (a suffix of an object of `total_size`
    /// bytes covering its trailer) is outside this build's window for this
    /// format, `None` when it is readable here or corrupt.
    ///
    /// RLOG and RSPAN have no trailer-only classifier, so this runs their
    /// suffix open over the trailer alone: it checks magic and then the
    /// version window before any footer byte, and an in-window trailer comes
    /// back as `NeedRange` (or a footer-bounds error), never as
    /// `UnsupportedVersion`.
    fn out_of_window(self, total_size: u64, tail: &[u8]) -> Option<u16> {
        match self {
            Self::Rseg => match classify_trailer(total_size, tail) {
                TrailerClass::OutsideVersionWindow(version) => Some(version),
                TrailerClass::Readable(_) | TrailerClass::Corrupt(_) => None,
            },
            Self::Rlog => match ravel_logseg::open_from_suffix(tail, total_size) {
                Err(ravel_logseg::LogSegError::UnsupportedVersion(version)) => Some(version),
                _ => None,
            },
            Self::Rspan => match ravel_rspan::open_from_suffix(tail, total_size) {
                Err(ravel_rspan::SpanSegError::UnsupportedVersion(version)) => Some(version),
                _ => None,
            },
        }
    }
}

/// The trailer version of one data object when it is outside this build's
/// reader window, `None` when the object is readable here, is corrupt, or is
/// already gone (a delete that a previous pass completed is not a hold).
async fn out_of_window_version(
    store: &dyn ObjectStoreBackend,
    format: ProbedFormat,
    key: &str,
) -> Result<Option<u16>> {
    let got = match store.get(key, GetRange::Suffix(TRAILER_LEN)).await {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Ok(None),
        Err(e) => return Err(MaintainError::Store(e)),
    };
    Ok(format.out_of_window(got.total_size, got.data.as_ref()))
}

/// Every key one physical sweep pass would delete, in delete order, tombstone
/// last.
///
/// The all-or-nothing gate and the deletes below have to enumerate the same
/// set: a key the sweep deletes but never offers to the [`LeaseCheck`] is a
/// hold that did not hold, and that is exactly the shape of the bug this gate
/// closes. The gate reads this iterator; the deletes below are still written
/// out per class, so a new key class goes in both places, and
/// `the_delete_set_is_every_class_in_delete_order` pins that they match.
fn sweep_delete_keys<'a>(
    listing: &'a BucketListing,
    l0_data_keys: &'a [String],
    l1_part_keys: &'a [String],
    tombstone_key: &'a str,
) -> impl Iterator<Item = &'a str> {
    listing
        .commit_keys
        .iter()
        .chain(listing.compaction_record_keys.iter())
        .chain(listing.rewrite_record_keys.iter())
        .chain(l0_data_keys.iter())
        .chain(l1_part_keys.iter())
        .map(String::as_str)
        .chain(std::iter::once(tombstone_key))
}

/// The keys of a pending physical sweep that the [`LeaseCheck`] protects, in
/// delete order. Empty means the sweep may proceed.
///
/// Every key is offered rather than stopping at the first protected one. The
/// check is a pure in-memory prefix match (no I/O), the count is what the
/// operator warning reports, and stopping early would leave the key classes
/// behind the first hit unasked, so a hold covering only those would be decided
/// by the ordering of the classes rather than by its own scope.
fn protected_sweep_keys<'a>(
    lease: &dyn LeaseCheck,
    listing: &'a BucketListing,
    l0_data_keys: &'a [String],
    l1_part_keys: &'a [String],
    tombstone_key: &'a str,
) -> Vec<&'a str> {
    sweep_delete_keys(listing, l0_data_keys, l1_part_keys, tombstone_key)
        .filter(|key| lease.is_protected(key))
        .collect()
}

/// Delete each key idempotently, skipping any the [`LeaseCheck`] protects
/// (a protected key becomes residue that the verifying LIST will catch, so the
/// tombstone stays for a later pass).
///
/// In the retention sweep this per-key skip is now unreachable: the
/// all-or-nothing gate in [`physical_sweep`] has already refused the pass if
/// any of these keys is protected. It stays as the last line of defence, so a
/// future caller that reaches the deletes by another route still cannot delete
/// a held key.
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
///
/// This enumerates by exclusion rather than by listing the shapes it expects,
/// so it needs no change when a new record kind appears: anything
/// `keys::partition_bucket_entry` classifies as other than the tombstone is
/// residue, and an unclassifiable key is layout drift. What that costs is
/// silence about which shape the sweeper forgot to delete -- a rewrite record
/// held this check false on every pass forever until the delete list above
/// learned about it (issue #1321).
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
    let record = record::decode_compaction(got.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("compaction record decode failed: {e}")))?;
    keys::verify_compaction_record_key(&record, key)?;
    Ok(record)
}

/// GET, decode, and key-verify one selective-erasure rewrite record (ADR-0064
/// decision 3, ADR-0010 §7). Decoded through
/// [`ravel_commit::erasure::decode_rewrite`], never a raw `prost` decode: that
/// is the reader with the `format_version` gate (ADR-0066 decision 2), and it
/// also re-verifies the record's own `input_set_hash` and the bucket its
/// `superseded_record_key` names.
async fn get_rewrite_record(store: &dyn ObjectStoreBackend, key: &str) -> Result<RewriteRecord> {
    let got = store.get(key, GetRange::Full).await?;
    let record = erasure::decode_rewrite(got.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("rewrite record decode failed: {e}")))?;
    keys::verify_rewrite_record_key(&record, key)?;
    Ok(record)
}

/// Whether the hour's nominal deadline still bounds a tombstoned bucket's
/// expiry given its listed rewrite records: true unless one of them has no
/// parts, since only a parts-less rewrite stands its `created_unix_ns` in for
/// an event time in [`max_event_ts`]. [`RewriteBound::Unread`] answers false
/// whenever any are listed, and [`RewriteBound::Known`] answers what it holds
/// without reading. A record that fails to read answers false, whatever the
/// error: false is the bound that never over-reads, and the answer only feeds
/// the retention lag, so it must not stop the retention pass that reads it.
async fn rewrites_keep_nominal_bound(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    rewrite_keys: &[String],
    bound: RewriteBound<'_>,
) -> bool {
    let read = match bound {
        RewriteBound::Known(keeps) => return keeps,
        _ if rewrite_keys.is_empty() => return true,
        RewriteBound::Unread => return false,
        RewriteBound::Read(read) => read,
    };
    for key in rewrite_keys {
        read.gets += 1;
        match get_rewrite_record(store, key).await {
            Ok(record) if record.parts.is_empty() => {
                read.learned = Some(false);
                return false;
            }
            Ok(_) => {}
            // Another replica's sweep deleted it after the listing.
            Err(MaintainError::Store(StoreError::NotFound)) => {
                tracing::debug!(
                    tenant = %bucket.tenant_hash.to_hex(),
                    signal = ?bucket.signal,
                    shard = bucket.shard,
                    ingest_hour_bucket = bucket.ingest_hour_bucket,
                    key = %key,
                    "rewrite record gone before its lag-bound read; using the tombstone-only bound"
                );
                return false;
            }
            Err(error) => {
                tracing::warn!(
                    tenant = %bucket.tenant_hash.to_hex(),
                    signal = ?bucket.signal,
                    shard = bucket.shard,
                    ingest_hour_bucket = bucket.ingest_hour_bucket,
                    key = %key,
                    %error,
                    "rewrite record lag-bound read failed; using the tombstone-only bound"
                );
                return false;
            }
        }
    }
    read.learned = Some(true);
    true
}

/// GET, decode, and key-verify one retention tombstone (ADR-0010 §7 discipline).
/// Read and verify a tombstone, with the store version of the bytes read: the
/// tombstone is the retention marker's anchor, and its version is part of the
/// anchor identity (ADR-1133 decision 2).
async fn get_tombstone_versioned(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<(RetentionTombstone, String)> {
    let got = store.get(key, GetRange::Full).await?;
    let tombstone = record::decode_tombstone(got.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("tombstone decode failed: {e}")))?;
    keys::verify_retention_tombstone_key(&tombstone, key)?;
    Ok((tombstone, got.version.0))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_object_store::InstrumentedStore;
    use ravel_object_store::instrument::StoreOp;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::Signal;

    use super::*;

    fn tenant() -> TenantHash {
        TenantHash([0u8; 16])
    }

    /// A [`LeaseCheck`] protecting exactly one key, recording every key it was
    /// asked about.
    struct HoldOneKey {
        held: String,
        asked: std::sync::Mutex<Vec<String>>,
    }

    impl LeaseCheck for HoldOneKey {
        fn is_protected(&self, key: &str) -> bool {
            self.asked.lock().expect("asked lock").push(key.to_string());
            key == self.held
        }
    }

    fn listing_of_every_class() -> BucketListing {
        BucketListing {
            commit_keys: vec!["commit-a".to_string(), "commit-b".to_string()],
            compaction_record_keys: vec!["compaction".to_string()],
            rewrite_record_keys: vec!["rewrite".to_string()],
            tombstone_key: Some("tombstone".to_string()),
        }
    }

    /// The gate's delete set is every class the sweep deletes, in delete order,
    /// tombstone last. The rewrite-record class is the one no integration
    /// fixture here produces, and it is also the class whose omission from the
    /// deletes once stalled every erased bucket forever (issue #1321), so it is
    /// pinned by name.
    #[test]
    fn the_delete_set_is_every_class_in_delete_order() {
        let listing = listing_of_every_class();
        let l0 = vec!["l0-data".to_string()];
        let l1 = vec!["l1-part".to_string()];
        let keys: Vec<&str> = sweep_delete_keys(&listing, &l0, &l1, "tombstone").collect();
        assert_eq!(
            keys,
            vec![
                "commit-a",
                "commit-b",
                "compaction",
                "rewrite",
                "l0-data",
                "l1-part",
                "tombstone",
            ]
        );
    }

    /// Every key is offered to the check, not just enough to reach a verdict:
    /// a hold on the last class in delete order is found, and the classes after
    /// the first protected key are still asked about.
    #[test]
    fn the_gate_offers_every_key_and_finds_a_hold_on_any_class() {
        let listing = listing_of_every_class();
        let l0 = vec!["l0-data".to_string()];
        let l1 = vec!["l1-part".to_string()];
        for held in [
            "commit-a",
            "commit-b",
            "compaction",
            "rewrite",
            "l0-data",
            "l1-part",
            "tombstone",
        ] {
            let lease = HoldOneKey {
                held: held.to_string(),
                asked: std::sync::Mutex::new(Vec::new()),
            };
            let protected = protected_sweep_keys(&lease, &listing, &l0, &l1, "tombstone");
            assert_eq!(protected, vec![held], "a hold on {held} must be found");
            assert_eq!(
                lease.asked.lock().expect("asked lock").len(),
                7,
                "every key is offered even once {held} has already matched"
            );
        }
    }

    /// The retention read of a compaction record refuses a future
    /// `format_version` (ADR-0066 decision 2), not reads it as version 1. The
    /// record is otherwise self-consistent (its identity fields reconstruct its
    /// own key), so the version gate is the only thing that can reject it.
    /// Removing that gate makes this test fail: the record then decodes and
    /// key-verifies as version 1 and the call returns `Ok`.
    #[tokio::test]
    async fn retention_refuses_a_future_version_compaction_record() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let signal = Signal::Metrics;
        let record = CompactionRecord {
            format_version: 3,
            tenant_hash: tenant.0.to_vec(),
            signal: ravel_commit::signal::to_proto(signal) as i32,
            shard: 0,
            ingest_hour_bucket: 1,
            input_set_hash: vec![0x33; 32],
            ..Default::default()
        };
        let key = keys::compaction_record_key_for(&record).expect("key");
        store
            .put(
                &key,
                record::encode_compaction(&record),
                PutOptions::default(),
            )
            .await
            .expect("seed put");

        let err = get_compaction_record(&store, &key)
            .await
            .expect_err("a version-3 compaction record must be refused, not read as v1");
        match &err {
            MaintainError::Invariant(msg) => assert!(
                msg.contains("format_version") && msg.contains("3"),
                "the failure names the version gate and the version seen: {msg}"
            ),
            other => panic!("expected Invariant from the version gate, got {other:?}"),
        }
    }

    /// The retention read of a tombstone refuses a future `format_version`
    /// (ADR-0066 decision 2), not reads it as version 1. Same self-consistent
    /// record and same gate-flip failure argument as the compaction case.
    #[tokio::test]
    async fn retention_refuses_a_future_version_tombstone() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let signal = Signal::Metrics;
        let tombstone = RetentionTombstone {
            format_version: 2,
            tenant_hash: tenant.0.to_vec(),
            signal: ravel_commit::signal::to_proto(signal) as i32,
            shard: 0,
            ingest_hour_bucket: 1,
            retired_at_ns: 1,
            retention_window_ns: 1,
            record_count_observed: 0,
        };
        let key = keys::retention_tombstone_key_for(&tombstone).expect("key");
        store
            .put(
                &key,
                record::encode_tombstone(&tombstone),
                PutOptions::default(),
            )
            .await
            .expect("seed put");

        let err = get_tombstone_versioned(&store, &key)
            .await
            .expect_err("a version-2 tombstone must be refused, not read as v1");
        match &err {
            MaintainError::Invariant(msg) => assert!(
                msg.contains("format_version") && msg.contains("2"),
                "the failure names the version gate and the version seen: {msg}"
            ),
            other => panic!("expected Invariant from the version gate, got {other:?}"),
        }
    }

    const RETIRED_AT_NS: i64 = 100 * crate::config::NS_PER_HOUR;
    const WINDOW_NS: i64 = 10 * crate::config::NS_PER_HOUR;

    /// Seed a tombstone retired at [`RETIRED_AT_NS`] for `hour` of the test
    /// tenant's metrics shard 0, and return that bucket.
    async fn put_tombstone(store: &dyn ObjectStoreBackend, hour: u32) -> Bucket {
        let tombstone = RetentionTombstone {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard: 0,
            ingest_hour_bucket: hour,
            retired_at_ns: RETIRED_AT_NS,
            retention_window_ns: WINDOW_NS as u64,
            record_count_observed: 1,
        };
        store
            .put(
                &keys::retention_tombstone_key_for(&tombstone).expect("key"),
                record::encode_tombstone(&tombstone),
                PutOptions::default(),
            )
            .await
            .expect("seed tombstone");
        Bucket::new(tenant(), Signal::Metrics, 0, hour)
    }

    /// A rewrite record with `parts` over one L0 input of `bucket`, its key set
    /// by `seed`.
    fn rewrite_record(
        bucket: &Bucket,
        seed: u128,
        parts: Vec<ravel_proto::commit::v1::CompactionPart>,
    ) -> RewriteRecord {
        let inputs = vec![ravel_proto::commit::v1::CompactionInputIdentity {
            writer_id: uuid::Uuid::from_u128(seed).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        }];
        let request_id = uuid::Uuid::from_u128(7).to_string();
        RewriteRecord {
            format_version: 1,
            tenant_hash: bucket.tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(bucket.signal) as i32,
            shard: bucket.shard,
            ingest_hour_bucket: bucket.ingest_hour_bucket,
            input_set_hash: ravel_commit::erasure::compute_rewrite_input_set_hash(
                &inputs,
                None,
                std::slice::from_ref(&request_id),
            )
            .to_vec(),
            inputs,
            parts,
            drops: vec![ravel_proto::commit::v1::RewriteDrop {
                request_id,
                dropped_count: 1,
            }],
            created_unix_ns: RETIRED_AT_NS - WINDOW_NS - 1,
            superseded_record_key: String::new(),
        }
    }

    /// Seed a rewrite record with `parts` over one L0 input of `bucket`, with
    /// the key `seed` sets, and return that key.
    async fn put_rewrite_seeded(
        store: &dyn ObjectStoreBackend,
        bucket: &Bucket,
        seed: u128,
        parts: Vec<ravel_proto::commit::v1::CompactionPart>,
    ) -> String {
        let rewrite = rewrite_record(bucket, seed, parts);
        let key = keys::rewrite_record_key_for(&rewrite).expect("key");
        store
            .put(
                &key,
                ravel_commit::erasure::encode_rewrite(&rewrite),
                PutOptions::default(),
            )
            .await
            .expect("seed rewrite");
        key
    }

    /// [`put_rewrite_seeded`] with seed 1.
    async fn put_rewrite(
        store: &dyn ObjectStoreBackend,
        bucket: &Bucket,
        parts: Vec<ravel_proto::commit::v1::CompactionPart>,
    ) -> String {
        put_rewrite_seeded(store, bucket, 1, parts).await
    }

    /// One part, so a rewrite carrying it keeps the nominal bound.
    fn a_part() -> ravel_proto::commit::v1::CompactionPart {
        ravel_proto::commit::v1::CompactionPart {
            content_hash: vec![0x40; 32],
            object_size: 1,
            sample_count: 1,
            max_event_ts_ns: crate::config::NS_PER_HOUR + 1,
            ..Default::default()
        }
    }

    /// One retention evaluation of `bucket` at `now_ns`.
    async fn evaluate_at(
        store: &dyn ObjectStoreBackend,
        bucket: &Bucket,
        now_ns: i64,
        bound: RewriteBound<'_>,
    ) -> Result<(RetentionOutcome, Option<ObservedExpiry>)> {
        retention_sweep_bucket_observed(
            &mut SnapshotReachability::new(),
            store,
            &crate::clock::FixedClock::new(now_ns),
            // The pinned-query window (ADR-1133) zeroed, so a swept bucket's
            // marker is written and clears in the same evaluation.
            &CompactorConfig {
                max_query_duration_ns: 0,
                head_cache_ttl_ns: 0,
                clock_skew_allowance_ns: 0,
                ..CompactorConfig::default()
            },
            Some(WINDOW_NS),
            &crate::sweep::NoLeases,
            bucket,
            bound,
        )
        .await
    }

    /// One retention evaluation of `bucket` just after [`RETIRED_AT_NS`]:
    /// its outcome and expiry, what its rewrite read did (`None` when the
    /// evaluation was told not to read), and the GETs the store saw.
    async fn observe(
        store: &InstrumentedStore<MemoryStore>,
        bucket: &Bucket,
        read_rewrites: bool,
    ) -> (
        RetentionOutcome,
        Option<ObservedExpiry>,
        Option<RewriteBoundRead>,
        u64,
    ) {
        let gets = || store.metrics().snapshot().op(StoreOp::Get).calls;
        let before = gets();
        let mut read = RewriteBoundRead::default();
        let bound = if read_rewrites {
            RewriteBound::Read(&mut read)
        } else {
            RewriteBound::Unread
        };
        let (outcome, expiry) = evaluate_at(store, bucket, RETIRED_AT_NS + 1, bound)
            .await
            .expect("retention pass");
        (
            outcome,
            expiry,
            read_rewrites.then_some(read),
            gets() - before,
        )
    }

    /// A pass over an already-tombstoned bucket reports the tombstone's
    /// `retired_at_ns` as the bucket's expiry bound, and marks it as the only
    /// bound once the bucket lists a rewrite record with no parts: such a
    /// rewrite stands its `created_unix_ns` in for an event time, so the
    /// hour's nominal deadline can sit far before the real expiry (issue
    /// #2073). Telling that apart for one rewrite costs exactly one GET of the
    /// record, counted, and none without a read. Flipped lines: the parts-less
    /// arm in `rewrites_keep_nominal_bound` removed reads `NoLaterThan` against
    /// `NoLaterThanOnly`; the `read.gets += 1` removed reads the counter as 0
    /// against 1.
    #[tokio::test]
    async fn a_tombstoned_bucket_with_a_parts_less_rewrite_is_bounded_by_its_tombstone_alone() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let bucket = put_tombstone(&store, 1).await;

        let (outcome, expiry, read, _) = observe(&store, &bucket, true).await;
        assert_eq!(outcome, RetentionOutcome::Tombstoned);
        assert_eq!(
            expiry,
            Some(ObservedExpiry::NoLaterThan(RETIRED_AT_NS)),
            "a bucket with no rewrite keeps the nominal deadline as a bound"
        );
        assert_eq!(
            read,
            Some(RewriteBoundRead::default()),
            "no rewrite, no rewrite GET, nothing learned"
        );

        put_rewrite(&store, &bucket, Vec::new()).await;
        let (_, unread, _, gets_unread) = observe(&store, &bucket, false).await;
        assert_eq!(
            unread,
            Some(ObservedExpiry::NoLaterThanOnly(RETIRED_AT_NS)),
            "an unread rewrite leaves the tombstone as the only bound"
        );
        let (outcome, expiry, read, gets_read) = observe(&store, &bucket, true).await;
        assert_eq!(outcome, RetentionOutcome::Tombstoned);
        assert_eq!(
            expiry,
            Some(ObservedExpiry::NoLaterThanOnly(RETIRED_AT_NS)),
            "a bucket with a parts-less rewrite is bounded by its tombstone alone"
        );
        assert_eq!(
            read,
            Some(RewriteBoundRead {
                gets: 1,
                learned: Some(false),
            }),
            "the rewrite GET is counted and its answer learned"
        );
        assert_eq!(
            gets_read,
            gets_unread + 1,
            "reading the rewrite costs exactly one GET"
        );
    }

    /// A rewrite that keeps parts carries their event times into
    /// [`max_event_ts`], so its tombstoned bucket keeps the nominal deadline as
    /// a bound alongside the tombstone (issue #2073 review). Flipped line: the
    /// closing `true` of `rewrites_keep_nominal_bound` replaced with `false`
    /// reads `NoLaterThanOnly` against `NoLaterThan`.
    #[tokio::test]
    async fn a_tombstoned_bucket_whose_rewrite_keeps_parts_keeps_the_nominal_bound() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let bucket = put_tombstone(&store, 1).await;
        put_rewrite(&store, &bucket, vec![a_part()]).await;

        let (_, _, _, gets_unread) = observe(&store, &bucket, false).await;
        let (outcome, expiry, read, gets_read) = observe(&store, &bucket, true).await;
        assert_eq!(outcome, RetentionOutcome::Tombstoned);
        assert_eq!(
            expiry,
            Some(ObservedExpiry::NoLaterThan(RETIRED_AT_NS)),
            "a rewrite that keeps parts leaves the nominal deadline a bound"
        );
        assert_eq!(
            read,
            Some(RewriteBoundRead {
                gets: 1,
                learned: Some(true),
            })
        );
        assert_eq!(gets_read, gets_unread + 1);
    }

    /// The rewrite read only feeds the retention lag, so a failed one never
    /// fails the retention evaluation (issue #2073 review): a store error, a
    /// record that does not decode, and a record gone before its GET each
    /// leave the evaluation `Ok`, give the tombstone-only bound (the rewrite
    /// here keeps parts, so a read that succeeded would give `NoLaterThan`),
    /// learn nothing, and still let the physical sweep run once the protection
    /// horizon has elapsed. Flipped lines: the `return false` of the arm that
    /// logs a failed read removed reads `NoLaterThan` against
    /// `NoLaterThanOnly` in the `Transient` case; the `return false` of the
    /// `NotFound` arm removed reads the same in the `NotFoundBlip` case.
    #[tokio::test]
    async fn a_failed_rewrite_read_falls_back_to_the_tombstone_bound_and_still_sweeps() {
        use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};

        for fault in [
            ScriptedFault::Transient("rewrite GET refused".to_string()),
            ScriptedFault::CorruptRange,
            ScriptedFault::NotFoundBlip,
        ] {
            let kind = fault.kind();
            let bucket = Bucket::new(tenant(), Signal::Metrics, 0, 1);
            let rewrite_key =
                keys::rewrite_record_key_for(&rewrite_record(&bucket, 1, Vec::new())).expect("key");
            let plan = FaultPlan::empty()
                .with_rule(Rule::new(Op::Get, fault).with_key_contains(rewrite_key.clone()));
            let store = FaultStore::new(MemoryStore::new(), plan);
            put_tombstone(&store, 1).await;
            assert_eq!(
                put_rewrite(&store, &bucket, vec![a_part()]).await,
                rewrite_key
            );

            let mut read = RewriteBoundRead::default();
            let (outcome, expiry) = evaluate_at(
                &store,
                &bucket,
                RETIRED_AT_NS + 1,
                RewriteBound::Read(&mut read),
            )
            .await
            .expect("a failed rewrite read must not fail the evaluation");
            assert_eq!(outcome, RetentionOutcome::Tombstoned, "{kind:?}");
            assert_eq!(
                expiry,
                Some(ObservedExpiry::NoLaterThanOnly(RETIRED_AT_NS)),
                "{kind:?}: a failed read gives the bound that never over-reads"
            );
            assert_eq!(
                read,
                RewriteBoundRead {
                    gets: 1,
                    learned: None,
                },
                "{kind:?}: one GET issued, and a failed read is not an answer"
            );
            assert_eq!(store.fault_count(Op::Get, kind), 1, "{kind:?} fired");

            let past_horizon = RETIRED_AT_NS + CompactorConfig::default().protection_horizon_ns;
            let mut read = RewriteBoundRead::default();
            let (outcome, expiry) =
                evaluate_at(&store, &bucket, past_horizon, RewriteBound::Read(&mut read))
                    .await
                    .expect("a failed rewrite read must not fail the evaluation");
            assert_eq!(
                outcome,
                RetentionOutcome::Swept,
                "{kind:?}: the physical sweep still runs"
            );
            assert_eq!(expiry, Some(ObservedExpiry::NoLaterThanOnly(RETIRED_AT_NS)));
            assert_eq!(store.fault_count(Op::Get, kind), 2, "{kind:?} fired again");
            let left = list_bucket(store.inner(), &bucket).await.expect("list");
            assert!(
                left.rewrite_record_keys.is_empty() && left.tombstone_key.is_none(),
                "{kind:?}: the sweep deleted the rewrite and the tombstone: {left:?}"
            );
        }
    }

    /// The read stops at the first parts-less rewrite in key order: with two
    /// rewrites, one that keeps parts listed first costs two GETs, and the
    /// parts-less one listed first costs one. Flipped lines: the parts-less
    /// arm's `return false` removed, with the closing answer replaced by
    /// `*read.learned.get_or_insert(true)` so the answer stays right, reads
    /// 2 GETs against 1 in the second arrangement.
    #[tokio::test]
    async fn the_rewrite_read_stops_at_the_first_parts_less_record() {
        let bucket = Bucket::new(tenant(), Signal::Metrics, 0, 1);
        let key_of =
            |seed| keys::rewrite_record_key_for(&rewrite_record(&bucket, seed, Vec::new()));
        let (first, second) = if key_of(1).expect("key") < key_of(2).expect("key") {
            (1, 2)
        } else {
            (2, 1)
        };

        for (parts_less_first, want_gets) in [(false, 2), (true, 1)] {
            let store = InstrumentedStore::new(MemoryStore::new());
            put_tombstone(&store, 1).await;
            let (first_parts, second_parts) = if parts_less_first {
                (Vec::new(), vec![a_part()])
            } else {
                (vec![a_part()], Vec::new())
            };
            let first_key = put_rewrite_seeded(&store, &bucket, first, first_parts).await;
            let second_key = put_rewrite_seeded(&store, &bucket, second, second_parts).await;
            assert_eq!(
                list_bucket(&store, &bucket)
                    .await
                    .expect("list")
                    .rewrite_record_keys,
                vec![first_key, second_key],
                "the listing is in key order"
            );

            let (_, _, _, gets_unread) = observe(&store, &bucket, false).await;
            let (_, expiry, read, gets_read) = observe(&store, &bucket, true).await;
            assert_eq!(
                expiry,
                Some(ObservedExpiry::NoLaterThanOnly(RETIRED_AT_NS)),
                "parts-less first: {parts_less_first}"
            );
            assert_eq!(
                read,
                Some(RewriteBoundRead {
                    gets: want_gets,
                    learned: Some(false),
                }),
                "parts-less first: {parts_less_first}"
            );
            assert_eq!(
                gets_read,
                gets_unread + want_gets as u64,
                "parts-less first: {parts_less_first}"
            );
        }
    }

    /// A known answer is used without a GET, and wins over the listing: a
    /// partial sweep deletes rewrite records before the tombstone, and the
    /// expiry the parts-less one set does not change with it. Flipped line:
    /// the `Known` arm moved after the empty-listing check reads `NoLaterThan`
    /// against `NoLaterThanOnly`.
    #[tokio::test]
    async fn a_known_answer_needs_no_read_and_outlives_the_listing() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let bucket = put_tombstone(&store, 1).await;
        let gets = || store.metrics().snapshot().op(StoreOp::Get).calls;

        let before = gets();
        let (_, expiry) = evaluate_at(
            &store,
            &bucket,
            RETIRED_AT_NS + 1,
            RewriteBound::Known(false),
        )
        .await
        .expect("retention pass");
        assert_eq!(expiry, Some(ObservedExpiry::NoLaterThanOnly(RETIRED_AT_NS)));
        let no_rewrite_gets = gets() - before;

        put_rewrite(&store, &bucket, Vec::new()).await;
        let before = gets();
        let (_, expiry) = evaluate_at(
            &store,
            &bucket,
            RETIRED_AT_NS + 1,
            RewriteBound::Known(true),
        )
        .await
        .expect("retention pass");
        assert_eq!(expiry, Some(ObservedExpiry::NoLaterThan(RETIRED_AT_NS)));
        assert_eq!(
            gets() - before,
            no_rewrite_gets,
            "a known answer reads no rewrite record"
        );
    }
}
