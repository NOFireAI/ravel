//! The sweeper: one component, five eligibility rules
//! (docs/consistency-model.md "Deletion
//! and GC"). This is the first implementation of any deletion in Ravel.
//!
//! 1. **Orphan GC** (ADR-0010 §11, quarantine ADR-0058 amendment): an `l0/`
//!    data object with no commit record, older than `grace +
//!    max_flush_lifetime`. The writer interlock (a writer abandons any flush
//!    older than `max_flush_lifetime` and never publishes it afterward) is what
//!    makes this safe: a record-less object that old can never gain a commit
//!    record later, so removing it cannot orphan a future reader. Commit-record
//!    absence is re-verified with one fresh strongly consistent LIST shared by
//!    every candidate in the pass (ADR-0048 decision 5), then gated by a
//!    mass-orphan circuit breaker (ADR-0048 decision 4): a pass that would
//!    collect at least `orphan_breaker_min_count` candidates AND more than
//!    `orphan_breaker_max_ratio` of the shard's listed L0 objects collects
//!    nothing and halts, because that shape is the signature of an out-of-band
//!    commit-record loss, not routine cleanup. Below those thresholds the
//!    breaker does not trip, and the same small or thinly-spread loss used to
//!    be deleted permanently. So orphan GC no longer deletes a candidate
//!    directly: it copies the object to `quarantine/<original key>/q<ns>`
//!    (copy first, then delete the live key, never the reverse) and
//!    [`sweep_quarantine`] physically deletes the copy only after a second
//!    horizon (`quarantine_horizon_ns`, default 7 days) measured from the
//!    quarantine timestamp embedded in the key. A small record loss under the
//!    breaker is then recoverable for a week and reported by
//!    [`SweepReport::orphans_quarantined`], instead of vanishing silently.
//! 2. **Superseded-input sweep** (ADR-0018): the L0 commit records and data
//!    objects a compaction record names in its input list, once
//!    `now >= record.created_unix_ns + protection_horizon` AND the live catalog
//!    HEAD snapshot no longer names the input (the ADR-0020 delete blocker,
//!    the same gate retention uses, [`crate::reachability`]). Records are
//!    deleted before data objects, so a crash mid-sweep never leaves a commit
//!    record pointing at a deleted data object visible to a resolver. The
//!    horizon alone is not enough: a selective-erasure rewrite record can land
//!    in any sealed hour, including one the fold's fixed reconcile window and
//!    retention-frontier band both miss, and the snapshot part covering that
//!    hour then keeps naming the pre-rewrite inputs. Deleting them on schedule
//!    would fail every query over that hour closed until the fold caught up, so
//!    a still-named input is held for a later pass instead.
//! 3. **Unreferenced-part cleanup**: an `l1/` object referenced by no
//!    compaction record in its bucket, once the object is older than `grace +
//!    max_compaction_lifetime` and one of two branch conditions holds:
//!    (a) a compaction record already exists for the bucket, so any object no
//!    record names is a leftover; or (b) a retention tombstone exists for the
//!    bucket and no compaction record does, which makes any future compaction
//!    impossible (`compact_bucket`'s tombstone gate returns before building or
//!    publishing, ADR-0019), so every record-less `l1/` object in the bucket
//!    can never be re-referenced by a legal future publish. A
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
//! 5. **Unreferenced catalog-object sweep**: a snapshot part under
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
//! vacuously satisfied everywhere. It is a seam for future slow-consumer work,
//! not live logic; no lease machinery is built behind it.
//!
//! [`sweep_shard_zoned`] scopes rules 2 and 3 to a given hour set, mirroring
//! the unit scan's zone split (ADR-0065 decision 3): the caller's per-tick
//! pass lists only its head+tail hours, and a full pass on the slow
//! safety-net cadence still uses [`sweep_shard`] to eventually cover every
//! hour. Rule 1 cannot be scoped this way and, when it runs, lists the whole shard
//! (L0 keys carry no ingest-hour component); see [`sweep_shard_zoned`]'s doc
//! for that deviation.

use std::collections::{BTreeSet, HashMap, HashSet};

use ravel_catalog::{
    erasure_dominated_compaction_records, select_authoritative_compaction_records,
};
use ravel_commit::keys::{self, BucketEntry, KeyError, parse_ingest_hour_string};
use ravel_commit::record;
use ravel_object_store::{
    GetRange, ObjectMeta, ObjectStoreBackend, PutOptions, StoreError, list_all,
};
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionRecord, ErasureCompletion, RewriteRecord,
};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::{CompactorConfig, NS_PER_HOUR};
use crate::error::{MaintainError, Result};
use crate::reachability::{
    MarkerContext, MarkerPolicy, MarkerStats, SnapshotBlock, SnapshotGate, SnapshotObject,
    SnapshotReachability, catalog_head_key,
};
use crate::read::verify_commit_key;
use crate::unnamed_marker::{MarkerAnchor, MarkerKind, MarkerReapOutcome};

use ravel_ingest::{IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS, MARKER_SUFFIX};

/// A hook the sweeper consults before every delete, in all five rules. The
/// only implementation today is [`NoLeases`] (nothing is ever protected); this
/// is a seam for future reader-lease / slow-consumer work, never
/// a correctness dependency of the current design. In-flight readers are
/// protected by the protection horizon, the age gates, the ADR-0020 HEAD
/// delete blocker and, for the retention and superseded-input rules, the
/// unnamed-since marker gate (ADR-1133): a candidate HEAD no longer names is
/// deleted only once its marker is older than `max_query_duration +
/// head_cache_ttl + 4 * clock_skew_allowance`. That window covers a query
/// with a validated deadline; a reader with none (the fold, scrub,
/// compaction, an erasure rewrite, `ravel-cli export`) is not covered by it.
pub trait LeaseCheck: Send + Sync {
    /// Return `true` if `key` is protected by an active reader lease and must
    /// not be deleted this pass.
    fn is_protected(&self, key: &str) -> bool;
}

/// The shipped [`LeaseCheck`]: nothing is ever protected, so the
/// consistency-model's "not lease-protected" GC precondition is vacuously
/// satisfied everywhere.
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
    /// Rule 1: record-less `l0/` data objects removed from the live keyspace
    /// (orphan GC). Since the ADR-0058 quarantine amendment these are moved to
    /// the `quarantine/` prefix rather than deleted outright, so this counts
    /// objects quarantined this pass, equal to [`Self::orphans_quarantined`];
    /// the name is retained because the operator-facing meaning ("orphan
    /// candidates GC removed from the live L0 set") is unchanged.
    ///
    /// This is NOT the pass's whole orphan-candidate count. A candidate whose
    /// copy failed is left live and counted only in
    /// [`Self::orphans_quarantine_refused`], so the present total is
    /// `orphans_deleted + orphans_withheld + orphans_quarantine_refused`. That
    /// third term is what the ADR-0058 decision-1 `orphans_present` gauge in
    /// `ravel-server` adds: a refused candidate is still present, and it is
    /// refused precisely in the store-fault case where the gauge matters most,
    /// so leaving it out would drop the signal exactly when it fires.
    pub orphans_deleted: usize,
    /// Rule 1: record-less `l0/` data objects moved to `quarantine/` this pass
    /// (ADR-0058 amendment). Equal to [`Self::orphans_deleted`]; a distinctly
    /// named counter an operator can alert on to see quarantine activity
    /// (small out-of-band commit-record loss below the mass-orphan breaker).
    pub orphans_quarantined: usize,
    /// Rule 1: orphan candidates NOT quarantined this pass because the copy to
    /// `quarantine/` failed, so the live object was left in place (fail-closed:
    /// the dangerous delete never runs when its safe copy did not). A persistent
    /// nonzero value is an operator signal that quarantine cannot make progress
    /// (a store fault, a permissions or capacity problem on the prefix), not the
    /// ordinary steady state, which is `0`.
    pub orphans_quarantine_refused: usize,
    /// The quarantine reaper ([`sweep_quarantine`]): objects physically deleted
    /// from `quarantine/` this pass because their embedded quarantine timestamp
    /// is more than `quarantine_horizon_ns` behind the clock. This is the only
    /// place orphan-GC'd data is ever physically removed. Always `0` on a pass
    /// that did not run rule 1 ([`OrphanPass::Skip`]) or whose breaker tripped:
    /// the reaper runs on candidate selection's cadence so the breaker's hold
    /// on it holds until the next selecting pass, not until the next tick.
    pub quarantine_reaped: usize,
    /// Rule 2: superseded L0 commit records deleted.
    pub superseded_records_deleted: usize,
    /// Rule 2: superseded L0 data objects deleted.
    pub superseded_data_deleted: usize,
    /// Rule 2: deletes the store refused this pass
    /// ([`SupersededSweepOutcome::deletes_refused`]). The refusing chain keeps
    /// its remaining keys for a later pass and the pass still succeeds, so this
    /// field is the only per-pass record of the refusal; it feeds
    /// `ravel_maintain_superseded_deletes_refused_total`. The steady state is
    /// `0`.
    pub superseded_deletes_refused: usize,
    /// Rule 2: objects held this pass because the live catalog HEAD snapshot
    /// still names them ([`SupersededSweepOutcome::held_by_snapshot`]); feeds
    /// `ravel_maintain_superseded_inputs_held_total{reason="named"}`.
    pub superseded_held_by_snapshot: usize,
    /// Rule 2: objects held this pass because HEAD or a covering snapshot part
    /// could not be read
    /// ([`SupersededSweepOutcome::held_by_unreadable_head`]); feeds
    /// `ravel_maintain_superseded_inputs_held_total{reason="unreadable_head"}`.
    pub superseded_held_by_unreadable_head: usize,
    /// Rule 2: objects held this pass because their unnamed-since marker has
    /// not yet aged past the pinned-query window
    /// ([`SupersededSweepOutcome::held_by_pinned_window`], ADR-1133); feeds
    /// `ravel_maintain_superseded_inputs_held_total{reason="pinned_window"}`.
    pub superseded_held_by_pinned_window: usize,
    /// Rule 2: the unnamed-since marker requests and transitions of this pass
    /// ([`SupersededSweepOutcome::unnamed_markers`]).
    pub unnamed_markers: MarkerStats,
    /// The orphan-marker reap this pass ran, if any: on a pass whose rule 2
    /// gated a candidate, and on every full pass ([`sweep_shard`]).
    pub unnamed_marker_reap: Option<MarkerReapOutcome>,
    /// Rule 2: chain groups skipped whole this pass because a legal hold
    /// protects a key in them
    /// ([`SupersededSweepOutcome::chain_groups_held_by_legal_hold`]); feeds
    /// `ravel_maintain_superseded_groups_held_by_legal_hold_total`.
    pub superseded_groups_held_by_legal_hold: usize,
    /// Rule 3: unreferenced `l1/` part objects deleted.
    pub unreferenced_parts_deleted: usize,
    /// Bytes of the objects [`Self::quarantine_reaped`] deleted this pass, from
    /// each reaped object's listed [`ObjectMeta::size`]. Known without an extra
    /// request because the reaper already listed the quarantine prefix; feeds
    /// `ravel_maintain_bytes_reclaimed_total`.
    pub quarantine_reaped_bytes: u64,
    /// Bytes of the objects [`Self::unreferenced_parts_deleted`] deleted this
    /// pass, from each part's listed [`ObjectMeta::size`]. Known without an
    /// extra request because rule 3 already listed the `l1/` prefix; feeds
    /// `ravel_maintain_bytes_reclaimed_total`.
    pub unreferenced_parts_bytes: u64,
    /// Bytes of the objects [`Self::superseded_data_deleted`] deleted: each L0
    /// data object at its commit record's `object_size`, charged on the pass
    /// that deletes it, and each superseded L1 part at the `object_size` its
    /// compaction or rewrite record carries, charged on the pass that deletes
    /// that record. A part delete that finds nothing still succeeds, so a part
    /// charged on its own delete would be charged again by the next pass
    /// whenever the record naming it was refused. Rule 2 already read those
    /// records to find the keys, so this costs no request; it is the size the
    /// writer recorded, not a listed size or wire bytes. Feeds
    /// `ravel_maintain_bytes_reclaimed_total`.
    pub superseded_data_bytes: u64,
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
    /// Whether this pass ran rule 1 at all (see [`OrphanPass`]).
    ///
    /// Every orphan field above is zero/`false` on a [`OrphanPass::Skip`]
    /// pass because rule 1 never ran, not because the pass looked and found
    /// nothing. The two are indistinguishable from the counts alone, so a
    /// consumer keeping a last-observed-value gauge (`ravel-server`'s
    /// `orphans_present` and `orphans_withheld`) reads this field and leaves
    /// its gauges alone on `Skip`: the gauge then reports the last pass that
    /// actually measured, whose cadence is the full-sweep interval, instead
    /// of being zeroed by every tick in between.
    pub orphan_pass: OrphanPass,
    /// `true` if this pass listed the whole shard (rule 1 always does; rules
    /// 2 and 3 did here too, either because the caller used [`sweep_shard`]
    /// or because [`sweep_shard_zoned`] was asked to widen to every hour).
    /// `false` for a [`sweep_shard_zoned`] pass scoped to a hour subset.
    /// Counter seam for `ravel_maintain_full_sweep_passes_total`: a
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
    let (report, _holds) =
        sweep_shard_with_holds(store, clock, config, lease, tenant, signal, shard).await?;
    Ok(report)
}

/// [`sweep_shard`], also returning what rule 2 held this pass, unioned with
/// [`SupersededHolds::absorb`] across shards. These are a deleting pass's
/// holds, which miss every chain under a rewrite still inside its protection
/// horizon, so they are not an input to rule 6: [`sweep_erasure_requests`]
/// observes its own. The operator signal for a hold is the WARN line this pass
/// logs; the returned value is for a caller that wants to aggregate the holds
/// across shards.
pub async fn sweep_shard_with_holds(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<(SweepReport, SupersededHolds)> {
    let mut reach = SnapshotReachability::new();
    let (superseded, superseded_data_bytes) = sweep_superseded_impl(
        &mut reach,
        store,
        clock,
        config,
        lease,
        tenant,
        signal,
        shard,
        None,
        SweepMode::Delete,
    )
    .await?;
    log_superseded_holds(tenant, signal, shard, &superseded);
    // A full pass reaps orphan markers even when rule 2 gated nothing, so a
    // tenant left with only orphans still has them reaped (ADR-1133 decision
    // 6). A pass that already reaped does not reap again.
    let full_pass_reap = reach
        .reap_after_pass(store, clock, config, tenant, signal, true)
        .await;
    let unnamed_marker_reap = superseded.unnamed_marker_reap.clone().or(full_pass_reap);
    let mut superseded_holds = SupersededHolds::default();
    superseded_holds.absorb(&superseded);
    let (unreferenced_parts_deleted, unreferenced_parts_bytes) =
        sweep_unreferenced_parts_impl(store, clock, config, lease, tenant, signal, shard, None)
            .await?;
    let (
        orphans_deleted,
        orphans_refused,
        orphan_breaker_tripped,
        orphans_withheld,
        orphan_breaker_overridden,
    ) = match sweep_orphans(store, clock, config, lease, tenant, signal, shard).await {
        Ok(outcome) => (
            outcome.deleted,
            outcome.refused,
            false,
            0,
            outcome.breaker_overridden,
        ),
        Err(MaintainError::OrphanBreakerTripped { candidates, .. }) => {
            (0, 0, true, candidates, false)
        }
        Err(e) => return Err(e),
    };
    // The quarantine reaper is the second horizon on rule 1's output. It runs
    // every pass so it is reachable from the same maintain tick as the sweep,
    // and whole-shard because quarantine keys are not hour-bucketed.
    //
    // It is skipped on a pass whose mass-orphan breaker tripped: a trip means
    // a record loss large enough to page is live now, and reaping during one
    // destroys the copies taken before the loss grew. A `force_orphan_gc`
    // override is not a trip, so an operator who has decided still reclaims.
    let quarantine = if orphan_breaker_tripped {
        QuarantineSweepOutcome::default()
    } else {
        sweep_quarantine(store, clock, config, lease, tenant, signal, shard).await?
    };
    Ok((
        SweepReport {
            orphans_deleted,
            orphans_quarantined: orphans_deleted,
            orphans_quarantine_refused: orphans_refused,
            quarantine_reaped: quarantine.reaped,
            superseded_records_deleted: superseded.records_deleted,
            superseded_data_deleted: superseded.data_deleted,
            superseded_deletes_refused: superseded.deletes_refused,
            superseded_held_by_snapshot: superseded.held_by_snapshot,
            superseded_held_by_unreadable_head: superseded.held_by_unreadable_head,
            superseded_held_by_pinned_window: superseded.held_by_pinned_window,
            unnamed_markers: reach.marker_stats().clone(),
            unnamed_marker_reap,
            superseded_groups_held_by_legal_hold: superseded.chain_groups_held_by_legal_hold,
            unreferenced_parts_deleted,
            quarantine_reaped_bytes: quarantine.reaped_bytes,
            unreferenced_parts_bytes,
            superseded_data_bytes,
            orphan_breaker_tripped,
            orphans_withheld,
            orphan_breaker_overridden,
            orphan_pass: OrphanPass::Run,
            full_pass: true,
        },
        superseded_holds,
    ))
}

/// Whether a [`sweep_shard_zoned_with_holds`] pass runs rule 1 (orphan GC) at
/// all. Orphan candidate selection's phase (a) is the one full-shard LIST of
/// the `l0/` data prefix that a zone split cannot narrow (L0 data keys carry
/// no ingest-hour component), so unlike rules 2 and 3 it cannot be scoped
/// down to `hours` -- it can only be run in full or skipped entirely this
/// pass. The caller drives `Run` vs `Skip` from its own full-sweep cadence
/// memo (e.g. [`crate::MaintainMemo::full_sweep_due`]'s consumer): there is no
/// new interval or flag here, only a gate on the existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OrphanPass {
    /// Run rule 1 (candidate selection, the re-verify LIST, the breaker gate,
    /// and quarantine) exactly as an ungated pass would.
    ///
    /// The default, so a [`SweepReport`] built from [`Default`] reports a
    /// measured orphan pass: that is what [`sweep_shard`] does
    /// unconditionally and what every pass did before this gate existed.
    /// Only a call site that asks for [`OrphanPass::Skip`] reports otherwise.
    #[default]
    Run,
    /// Skip rule 1 entirely this pass: no `l0/` data prefix LIST, no
    /// candidate selection, no breaker evaluation. The quarantine reaper is
    /// skipped with it: a pass that did not evaluate the breaker reports
    /// not-tripped structurally, so reaping here would undo the hold a
    /// tripped selecting pass just took. Reaping stays on candidate
    /// selection's cadence, which is what makes the two horizons chained
    /// rather than independent.
    Skip,
}

/// Zone-scoped sweep pass (ADR-0065 decision 3): rules 2 and 3 list only the
/// given `hours`' commit and L1 prefixes instead of the whole shard,
/// mirroring the unit scan's zone split so a per-tick pass never re-lists
/// interior hours a tick's zone recomputation already decided to skip.
/// `hours` is the caller's current head+tail set for this unit.
///
/// Rule 1 (orphan GC), when run, always lists the whole shard regardless of
/// `hours`: L0 data keys carry no ingest-hour component
/// (`ravel_commit::keys::data_key`), so there is no hour-scoped prefix to
/// list. This is a structural limit, not an oversight -- flagged as a
/// deviation from a literal reading of the ADR, which does not distinguish
/// rule 1 from rules 2 and 3 when describing the per-tick sweep as
/// hour-scoped. This wrapper always passes [`OrphanPass::Run`], preserving
/// every existing caller's behavior; a caller that wants to gate rule 1 off
/// this pass (the shipping per-tick cadence, see
/// [`sweep_shard_zoned_with_holds`]) calls that function directly.
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
    let (report, _holds) = sweep_shard_zoned_with_holds(
        store,
        clock,
        config,
        lease,
        tenant,
        signal,
        shard,
        hours,
        OrphanPass::Run,
    )
    .await?;
    Ok(report)
}

/// [`sweep_shard_zoned`], also returning what rule 2 held this pass, exactly as
/// [`sweep_shard_with_holds`] does for the full pass.
///
/// `orphan_pass` gates rule 1 (see [`OrphanPass`]). On [`OrphanPass::Skip`]
/// the returned [`SweepReport`]'s orphan and breaker fields
/// (`orphans_deleted`, `orphans_quarantined`, `orphans_quarantine_refused`,
/// `orphan_breaker_tripped`, `orphans_withheld`, `orphan_breaker_overridden`)
/// are all zero/`false` for this pass -- never a stale value carried over
/// from a previous [`OrphanPass::Run`] pass, since they are computed fresh
/// every call and this call never touches rule 1's state. Those zeros are
/// structural, not a measurement of zero orphans, which is why the report
/// also carries [`SweepReport::orphan_pass`]: a consumer that cannot tell
/// them apart publishes "no orphans" for every tick between two full sweeps.
/// `quarantine_reaped` is `0` on a `Skip` pass too: the reaper is rule 1's
/// second horizon, and it runs only on a pass that ran candidate selection
/// and did not trip the breaker, so the breaker's hold on the reaper cannot
/// be stepped around by the next pass that skipped rule 1 (see
/// [`OrphanPass::Skip`]). Quarantined objects keep aging out on the
/// full-sweep cadence.
#[allow(clippy::too_many_arguments)]
pub async fn sweep_shard_zoned_with_holds(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    hours: &[u32],
    orphan_pass: OrphanPass,
) -> Result<(SweepReport, SupersededHolds)> {
    let mut reach = SnapshotReachability::new();
    let (superseded, superseded_data_bytes) = sweep_superseded_impl(
        &mut reach,
        store,
        clock,
        config,
        lease,
        tenant,
        signal,
        shard,
        Some(hours),
        SweepMode::Delete,
    )
    .await?;
    log_superseded_holds(tenant, signal, shard, &superseded);
    let mut superseded_holds = SupersededHolds::default();
    superseded_holds.absorb(&superseded);
    let (unreferenced_parts_deleted, unreferenced_parts_bytes) = sweep_unreferenced_parts_impl(
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
    let (
        orphans_deleted,
        orphans_refused,
        orphan_breaker_tripped,
        orphans_withheld,
        orphan_breaker_overridden,
    ) = match orphan_pass {
        // No `l0/` data prefix LIST this pass: rule 1 is not due (see
        // `OrphanPass`). Fresh zeros, never a value left over from a prior
        // `Run` pass.
        OrphanPass::Skip => (0, 0, false, 0, false),
        OrphanPass::Run => {
            match sweep_orphans(store, clock, config, lease, tenant, signal, shard).await {
                Ok(outcome) => (
                    outcome.deleted,
                    outcome.refused,
                    false,
                    0,
                    outcome.breaker_overridden,
                ),
                Err(MaintainError::OrphanBreakerTripped { candidates, .. }) => {
                    (0, 0, true, candidates, false)
                }
                Err(e) => return Err(e),
            }
        }
    };
    // The quarantine reaper runs only on a pass that ran candidate selection
    // and whose breaker did not trip, so it keeps rule 1's cadence and the two
    // horizons stay chained by construction. A `Skip` pass never evaluated the
    // breaker, so its `orphan_breaker_tripped` is structurally false; reaping
    // there would physically delete the very copies the previous selecting
    // pass held by tripping. It is whole-shard because quarantine keys are not
    // hour-bucketed (like rule 1 itself). A `force_orphan_gc` override is not
    // a trip, so an operator who has decided still reclaims.
    let quarantine = if orphan_pass == OrphanPass::Run && !orphan_breaker_tripped {
        sweep_quarantine(store, clock, config, lease, tenant, signal, shard).await?
    } else {
        QuarantineSweepOutcome::default()
    };
    Ok((
        SweepReport {
            orphans_deleted,
            orphans_quarantined: orphans_deleted,
            orphans_quarantine_refused: orphans_refused,
            quarantine_reaped: quarantine.reaped,
            superseded_records_deleted: superseded.records_deleted,
            superseded_data_deleted: superseded.data_deleted,
            superseded_deletes_refused: superseded.deletes_refused,
            superseded_held_by_snapshot: superseded.held_by_snapshot,
            superseded_held_by_unreadable_head: superseded.held_by_unreadable_head,
            superseded_held_by_pinned_window: superseded.held_by_pinned_window,
            unnamed_markers: superseded.unnamed_markers.clone(),
            unnamed_marker_reap: superseded.unnamed_marker_reap.clone(),
            superseded_groups_held_by_legal_hold: superseded.chain_groups_held_by_legal_hold,
            unreferenced_parts_deleted,
            quarantine_reaped_bytes: quarantine.reaped_bytes,
            unreferenced_parts_bytes,
            superseded_data_bytes,
            orphan_breaker_tripped,
            orphans_withheld,
            orphan_breaker_overridden,
            orphan_pass,
            full_pass: false,
        },
        superseded_holds,
    ))
}

/// Surface a superseded-input hold to an operator running the combined
/// [`sweep_shard`] / [`sweep_shard_zoned`] pass. [`SweepReport`] carries the
/// three hold counts and the refusal count; this line adds what it does not,
/// the held request ids, the truncated buckets and the unattached dominated
/// records, and names the shard. Silent on a pass that held nothing, which is
/// every ordinary pass.
fn log_superseded_holds(
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    outcome: &SupersededSweepOutcome,
) {
    if outcome.held() == 0
        && outcome.chain_groups_held_by_legal_hold == 0
        && outcome.dominated_records_unattached == 0
        && outcome.held_request_ids.is_empty()
        && outcome.held_truncated_buckets.is_empty()
        && outcome.deletes_refused == 0
    {
        return;
    }
    tracing::warn!(
        tenant_hash = %tenant.to_hex(),
        signal = signal.key_prefix(),
        shard,
        held_by_snapshot = outcome.held_by_snapshot,
        held_by_unreadable_head = outcome.held_by_unreadable_head,
        chain_groups_held_by_legal_hold = outcome.chain_groups_held_by_legal_hold,
        held_requests = outcome.held_request_ids.len(),
        held_truncated_buckets = outcome.held_truncated_buckets.len(),
        dominated_records_unattached = outcome.dominated_records_unattached,
        deletes_refused = outcome.deletes_refused,
        "superseded-input sweep: held inputs the live catalog HEAD snapshot still names \
         (or could not be read, or a legal hold protects, or the store refused a delete); \
         they are collected once the fold reconciles their hour, HEAD is rebuilt, or the \
         hold or refusal is lifted"
    );
}

// --- Rule 1: orphan GC (ADR-0010 §11) --------------------------------------

/// What one orphan-GC pass did (ADR-0048 decisions 4 and 5). A pass that
/// trips the mass-orphan breaker without an override returns
/// [`MaintainError::OrphanBreakerTripped`] instead of this type, and deletes
/// nothing; [`sweep_shard`] folds that error into [`SweepReport`]'s breaker
/// fields for callers that run the whole shard.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrphanSweepOutcome {
    /// Record-less `l0/` data objects removed from the live keyspace this pass.
    /// Since the ADR-0058 quarantine amendment "removed" means moved to
    /// `quarantine/` (copy then delete of the live key), not physically
    /// deleted; the field keeps its name because it is the count of orphan
    /// candidates GC took out of the live L0 set.
    pub deleted: usize,
    /// Orphan candidates left in place this pass because the copy to
    /// `quarantine/` failed. The live object is untouched (fail-closed), and
    /// the candidate is retried next pass.
    pub refused: usize,
    /// This pass exceeded the breaker's threshold but proceeded anyway
    /// because [`CompactorConfig::force_orphan_gc`] was set (ADR-0048
    /// decision 4's one-shot operator override).
    pub breaker_overridden: bool,
}

/// Quarantine every record-less `l0/` data object older than the orphan age
/// gate (ADR-0058 amendment). Four phases (ADR-0048 decisions 4 and 5): (a)
/// candidate selection over one listing of the shard's L0 data objects,
/// filtered by the commit-record identities already present, the age gate, and
/// lease protection; (b) one fresh strongly consistent LIST of the commit
/// prefix, shared by every candidate, dropping any whose identity now appears
/// (replacing the old per-candidate full-shard LIST, the dominant request cost
/// of a sweep); (c) the mass-orphan circuit breaker gate; (d) move each
/// surviving candidate to the `quarantine/` prefix (copy then delete the live
/// key). A tripped, non-overridden breaker returns
/// [`MaintainError::OrphanBreakerTripped`] before phase (d), so the breaker is
/// all-or-nothing: either every surviving candidate is quarantined, or none
/// are. Physical deletion of a quarantined object happens only later, in
/// [`sweep_quarantine`], after a second horizon.
///
/// The move is copy-first, delete-second, per object: the bytes are copied to
/// `quarantine/<original key>/q<quarantined_at_ns>` and only then is the live
/// key deleted, so a crash or store fault between the two leaves the object in
/// at least one location, never none. A candidate whose copy fails is left
/// live and reported in [`OrphanSweepOutcome::refused`] (fail-closed: the
/// delete never runs when its copy did not), and retried on the next pass.
/// [`OrphanSweepOutcome::deleted`] counts candidates successfully moved out of
/// the live keyspace this pass.
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

    // Phase (d): quarantine. The breaker (phase (c)) already returned if the
    // pass should collect zero; here every surviving candidate is moved to the
    // `quarantine/` prefix, copy first and delete of the live key second, so a
    // crash or fault between the two never destroys the only copy. A copy
    // failure leaves the object live and is counted, not fatal: nothing is
    // deleted that was not first safely copied.
    let quarantined_at_ns = now;
    let mut quarantined = 0usize;
    let mut refused = 0usize;
    for (meta, _) in &candidates {
        if config.dry_run {
            quarantined += 1;
            continue;
        }
        let dest = quarantine_key(&meta.key, quarantined_at_ns);
        match quarantine_object(store, &meta.key, &dest).await {
            Ok(QuarantineMove::Copied) => {
                store.delete(&meta.key).await?;
                quarantined += 1;
            }
            // The live object vanished between the listing and the copy (a
            // concurrent pass, or a prior crashed pass that had already
            // deleted it): nothing to move, and idempotent.
            Ok(QuarantineMove::SourceGone) => {}
            Err(e) => {
                tracing::warn!(
                    tenant_hash = %tenant.to_hex(),
                    signal = signal.key_prefix(),
                    shard,
                    key = %meta.key,
                    error = %e,
                    "orphan GC: copy to quarantine failed; leaving the object live and \
                     retrying next pass (fail-closed: never delete what was not copied)"
                );
                refused += 1;
            }
        }
    }

    if quarantined > 0 || refused > 0 {
        tracing::warn!(
            tenant_hash = %tenant.to_hex(),
            signal = signal.key_prefix(),
            shard,
            quarantined,
            refused,
            l0_objects_listed,
            breaker_overridden = would_trip && config.force_orphan_gc,
            "orphan GC: moved record-less L0 data objects to quarantine (recoverable for \
             quarantine_horizon_ns before physical deletion); a nonzero count below the \
             mass-orphan breaker's thresholds can be small out-of-band commit-record loss"
        );
    }

    Ok(OrphanSweepOutcome {
        deleted: quarantined,
        refused,
        breaker_overridden: would_trip && config.force_orphan_gc,
    })
}

/// Whether [`quarantine_object`] copied the source or found it already gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuarantineMove {
    /// The source bytes were copied to the quarantine key.
    Copied,
    /// The source object was already absent (NotFound): nothing to copy, and
    /// the live-key delete is skipped. Idempotent with a prior crashed pass.
    SourceGone,
}

/// Copy one object's bytes to its quarantine key (`quarantine/<original
/// key>/q<ns>`). NotFound on the source is reported as
/// [`QuarantineMove::SourceGone`] rather than an error: the object vanished
/// between the listing and the copy, which is not a fault.
///
/// The overwrite [`PutOptions`] make a retry idempotent WITHIN one pass only.
/// The destination key embeds that pass's `quarantined_at_ns`, so a crash
/// between the copy and the live-key delete leaves the object live, and the
/// next pass quarantines it again under a different `/q<ns>`: a second copy,
/// not an overwrite of the first. The duplicate is self-cleaning, since the
/// reaper collects both on their own horizons, and the live object is never
/// deleted without a copy of it existing. Do not read the overwrite as
/// cross-pass idempotence.
async fn quarantine_object(
    store: &dyn ObjectStoreBackend,
    src: &str,
    dest: &str,
) -> Result<QuarantineMove> {
    let got = match store.get(src, GetRange::Full).await {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Ok(QuarantineMove::SourceGone),
        Err(e) => return Err(MaintainError::Store(e)),
    };
    store.put(dest, got.data, PutOptions::default()).await?;
    Ok(QuarantineMove::Copied)
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

// --- Rule 1b: quarantine reaper (ADR-0058 amendment) -----------------------

/// Top-level prefix every quarantined orphan lives under. A new additive
/// key space alongside `t/` and `sys/`, listed only by [`sweep_quarantine`]
/// and never by any other sweep rule, so no `t/`-scoped listing ever sees it.
const QUARANTINE_PREFIX: &str = "quarantine/";

/// The quarantine key for one live object: `quarantine/<original key>/q<ns>`.
/// The whole original key is preserved verbatim (strip the `quarantine/`
/// prefix and the trailing `/q<ns>` segment to recover it for a restore), and
/// the trailing segment records when the object was quarantined so
/// [`sweep_quarantine`] can gate the second horizon on the injected clock
/// rather than on the copy's store `last_modified` (which, like every other
/// GC age signal that can, this path reads from the key, not the object; see
/// rule 4's ingest-hour marker). Zero-padded to a fixed width so the segment
/// is unambiguous and lexicographically ordered.
fn quarantine_key(original: &str, quarantined_at_ns: i64) -> String {
    format!("{QUARANTINE_PREFIX}{original}/q{quarantined_at_ns:020}")
}

/// `quarantine/t/<tenant_hash>/<signal>/l0/<shard>/` -- the prefix covering
/// every quarantined orphan for one `(tenant, signal, shard)`, the quarantine
/// mirror of [`l0_data_prefix`].
fn quarantine_l0_data_prefix(tenant: &TenantHash, signal: Signal, shard: u32) -> Result<String> {
    Ok(format!(
        "{QUARANTINE_PREFIX}{}",
        l0_data_prefix(tenant, signal, shard)?
    ))
}

/// The quarantine timestamp encoded in a quarantine key's trailing `/q<ns>`
/// segment, or `None` if it is absent or unparseable. `None` is treated as
/// not-yet-expired by [`sweep_quarantine`] (fail-closed: a malformed key is
/// never reaped early).
fn parse_quarantine_timestamp(quarantine_key: &str) -> Option<i64> {
    quarantine_key
        .rsplit('/')
        .next()
        .and_then(|seg| seg.strip_prefix('q'))
        .and_then(|digits| digits.parse::<i64>().ok())
}

/// The live key a quarantine key was copied from: strip the `quarantine/`
/// prefix and the trailing `/q<ns>` segment.
///
/// The reaper asks the lease check about this as well as about the quarantine
/// key itself. A legal-hold scope is validated to start with `t/<tenant_hex>/`
/// and `LegalHoldCheck::is_protected` is a prefix match, so a hold can never
/// match a `quarantine/...` key. Without this, a hold placed after an object
/// was quarantined would not stop the reap.
fn original_key_from_quarantine(quarantine_key: &str) -> Option<&str> {
    let without_prefix = quarantine_key.strip_prefix(QUARANTINE_PREFIX)?;
    let (original, stamp) = without_prefix.rsplit_once('/')?;
    stamp.strip_prefix('q')?;
    Some(original)
}

/// What one quarantine-reaper pass did (ADR-0058 amendment).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuarantineSweepOutcome {
    /// Objects physically deleted from `quarantine/` this pass, past the second
    /// horizon.
    pub reaped: usize,
    /// Bytes of the [`Self::reaped`] objects, summed from each object's listed
    /// [`ObjectMeta::size`] as the pass deletes it. No extra request: the size
    /// comes from the LIST the reaper already issues.
    pub reaped_bytes: u64,
    /// Objects left in quarantine this pass, still inside the second horizon
    /// (or with an unparseable timestamp, or held: a hold on the recovered
    /// original key binds to its quarantine copy).
    pub retained: usize,
}

/// Physically delete quarantined orphan objects for one `(tenant, signal,
/// shard)` whose embedded quarantine timestamp is more than
/// `quarantine_horizon_ns` behind the clock (ADR-0058 amendment).
///
/// This is the second and final horizon on orphan-GC'd data and the only place
/// it is ever physically removed: [`sweep_orphans`] moved these objects out of
/// the live keyspace into `quarantine/`, giving an operator a recovery window
/// for a small out-of-band commit-record loss the mass-orphan breaker does not
/// catch. It runs whole-shard like rule 1 (quarantine keys are not
/// hour-bucketed), is stateless and idempotent (deleting a missing key is a
/// success), and fails closed on a malformed key (an unparseable age is never
/// reaped). The age is read from the key, not the copy's store
/// `last_modified`, so the horizon is deterministic under the injected clock.
pub async fn sweep_quarantine(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<QuarantineSweepOutcome> {
    let now = clock.now_ns();
    let horizon = config.quarantine_horizon_ns;
    let prefix = quarantine_l0_data_prefix(tenant, signal, shard)?;
    let objects = list_all(store, &prefix).await?;

    let mut reaped = 0usize;
    let mut reaped_bytes = 0u64;
    let mut retained = 0usize;
    for meta in objects {
        let Some(quarantined_at_ns) = parse_quarantine_timestamp(&meta.key) else {
            retained += 1;
            continue;
        };
        if now.saturating_sub(quarantined_at_ns) <= horizon {
            retained += 1;
            continue;
        }
        let held = lease.is_protected(&meta.key)
            || original_key_from_quarantine(&meta.key).is_some_and(|k| lease.is_protected(k));
        if held {
            retained += 1;
            continue;
        }
        if !config.dry_run {
            store.delete(&meta.key).await?;
        }
        reaped += 1;
        reaped_bytes = reaped_bytes.saturating_add(meta.size);
    }

    if reaped > 0 {
        tracing::warn!(
            tenant_hash = %tenant.to_hex(),
            signal = signal.key_prefix(),
            shard,
            reaped,
            retained,
            "quarantine reaper: physically deleted orphan-GC'd objects past the second \
             horizon; this is the point at which quarantined data becomes unrecoverable"
        );
    }

    Ok(QuarantineSweepOutcome {
        reaped,
        reaped_bytes,
        retained,
    })
}

// --- Rule 2: superseded-input sweep (ADR-0018) -----------------------------

/// One `(shard, ingest hour)` bucket in which a sweep pass held part of a
/// supersession chain it could not walk to the end (a generation's record was
/// already gone). The requests the missing generation applied are not
/// discoverable from any surviving record, so the erasure-request sweep holds
/// every `.dreq` that could name such a bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeldBucket {
    pub shard: u32,
    pub ingest_hour_bucket: u32,
}

/// What one superseded-input sweep pass did.
///
/// The two `held_*` counters are the object-granular counterpart of
/// retention's [`crate::retention::RetentionOutcome::BlockedBySnapshot`]: a
/// count plus the blocked reason, so an operator watching inputs pile up can
/// tell the ordinary lagging-fold case ([`SnapshotBlock::Named`]) from an
/// unreadable HEAD or snapshot part ([`SnapshotBlock::Unreadable`]).
///
/// `held_request_ids` and `held_truncated_buckets` are what the
/// erasure-request sweep consumes, from an observing pass
/// ([`SweepMode::GateOnly`]): this pass is the only component that already
/// knows, per chain group, both which erasure requests the group's
/// generations applied and whether the group was held. Publishing that here
/// is what lets rule 6 decide without depending on a completion record's
/// optional per-bucket drop list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SupersededSweepOutcome {
    /// Superseded commit, compaction, and rewrite records deleted (or, under
    /// `dry_run`, that would have been).
    pub records_deleted: usize,
    /// Superseded data objects and L1 parts deleted (or, under `dry_run`, that
    /// would have been).
    pub data_deleted: usize,
    /// Objects (records plus data) held this pass because the live catalog HEAD
    /// snapshot still names them: a query over that hour would fail closed if
    /// they were deleted now. The combined pass copies it into
    /// [`SweepReport::superseded_held_by_snapshot`], which feeds
    /// `ravel_maintain_superseded_inputs_held_total{reason="named"}`.
    pub held_by_snapshot: usize,
    /// Objects held this pass because HEAD, or a snapshot part covering the
    /// record's hour, was present but could not be read: fail-closed, since
    /// non-reachability cannot be proven from data that cannot be read. A
    /// persistent nonzero value here is an operator signal, not the ordinary
    /// lagging-fold case. The combined pass copies it into
    /// [`SweepReport::superseded_held_by_unreadable_head`], which feeds
    /// `ravel_maintain_superseded_inputs_held_total{reason="unreadable_head"}`.
    pub held_by_unreadable_head: usize,
    /// Objects held this pass because HEAD names none of them but their
    /// unnamed-since marker is missing, was written for another record, or
    /// has not yet aged past the pinned-query window (ADR-1133): a query that
    /// resolved a HEAD from before the drop may still be reading them. Every
    /// collectable group reports here for at least one pass. The combined pass
    /// copies it into [`SweepReport::superseded_held_by_pinned_window`], which
    /// feeds `ravel_maintain_superseded_inputs_held_total{reason="pinned_window"}`.
    pub held_by_pinned_window: usize,
    /// The unnamed-since marker requests and transitions of this pass.
    pub unnamed_markers: MarkerStats,
    /// What this pass's orphan-marker reap did, when one ran.
    pub unnamed_marker_reap: Option<MarkerReapOutcome>,
    /// Chain groups skipped whole this pass because the [`LeaseCheck`] protects
    /// at least one key in them. The unit is the group, not the object: a
    /// group is one indivisible deletion unit, so a hold over any single key in
    /// it stops all of it. The combined pass copies it into
    /// [`SweepReport::superseded_groups_held_by_legal_hold`], which feeds
    /// `ravel_maintain_superseded_groups_held_by_legal_hold_total`.
    pub chain_groups_held_by_legal_hold: usize,
    /// Every erasure request id applied anywhere on a chain group this pass
    /// held, for any of the three reasons above. While a request is in this
    /// set an object that predates its rewrite is still in the store, so its
    /// `.dreq` (and with it the query-time exclusion filter) must survive.
    pub held_request_ids: BTreeSet<String>,
    /// The buckets in which this pass held a group whose supersession chain it
    /// could not walk to the end. The requests the missing generation applied
    /// are not in `held_request_ids`, because no surviving record names them,
    /// so rule 6 falls back to the bucket.
    pub held_truncated_buckets: BTreeSet<HeldBucket>,
    /// Erasure-dominated version 2 compaction records kept this pass because
    /// no rewrite's chain group took them, in buckets with no rewrite still
    /// inside its protection horizon. Each one holds parts that re-encode a
    /// pre-erasure record, so a nonzero value is an operator signal. Counted
    /// by a deleting pass only.
    pub dominated_records_unattached: usize,
    /// Deletes the store refused this pass (access denied, a failed
    /// precondition, or a permanent error, such as a deny policy on part of
    /// the keyspace). Not fatal while at least one delete in the pass
    /// succeeds: the refusing group keeps every key it had not yet deleted and
    /// is reported held, the next pass whose scope includes that hour retries
    /// it, and the other groups are collected. A pass in which every delete it
    /// attempted was refused fails with the first refusal's error instead. Each
    /// refusal is logged at WARN, and the combined pass copies the count into
    /// [`SweepReport::superseded_deletes_refused`], which feeds
    /// `ravel_maintain_superseded_deletes_refused_total`; a persistent nonzero
    /// value is an operator signal.
    pub deletes_refused: usize,
}

impl SupersededSweepOutcome {
    /// Objects held this pass for any reason.
    pub fn held(&self) -> usize {
        self.held_by_snapshot + self.held_by_unreadable_head + self.held_by_pinned_window
    }

    /// Record what a held group means for rule 6: every request the group's
    /// objects predate must keep its `.dreq`, and a group whose chain could
    /// not be walked to the end names requests no surviving record does, so
    /// its bucket is reported instead.
    fn note_hold(&mut self, group: &SupersededGroup, shard: u32) {
        self.held_request_ids
            .extend(group.request_ids.iter().cloned());
        if group.truncated {
            self.held_truncated_buckets.insert(HeldBucket {
                shard,
                ingest_hour_bucket: group.ingest_hour_bucket,
            });
        }
    }
}

/// What a superseded-input sweep held, in the form rule 6 consumes: the union
/// over however many shards the pass swept. Rule 6 takes it only from its own
/// observing pass, which walks every chain in the signal whatever its age.
///
/// This is the whole input to the erasure-request guard. Rule 6 asks no
/// question of its own about supersession chains: rule 2 already walked them,
/// already gated them, and already knows which requests the objects it held
/// predate. A completion record's `bucket_drops` plays no part in it at all,
/// neither to decide a hold nor to narrow which buckets are observed: the
/// field is optional on the wire, and a list that is present but partial is
/// indistinguishable from a complete one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SupersededHolds {
    /// [`SupersededSweepOutcome::held_request_ids`], unioned across shards.
    pub request_ids: BTreeSet<String>,
    /// [`SupersededSweepOutcome::held_truncated_buckets`], unioned across
    /// shards.
    pub truncated_buckets: BTreeSet<HeldBucket>,
}

impl SupersededHolds {
    /// Fold one shard's outcome into the union.
    pub fn absorb(&mut self, outcome: &SupersededSweepOutcome) {
        self.request_ids
            .extend(outcome.held_request_ids.iter().cloned());
        self.truncated_buckets
            .extend(outcome.held_truncated_buckets.iter().copied());
    }

    /// `true` when nothing was held: every `.dreq` past its horizon is
    /// collectable, which is the ordinary steady state.
    pub fn is_empty(&self) -> bool {
        self.request_ids.is_empty() && self.truncated_buckets.is_empty()
    }
}

/// Delete the L0 commit records and data objects named in each horizon-passed
/// compaction record's input list, records before data objects, skipping any
/// the live catalog HEAD snapshot still names (the ADR-0020 delete blocker).
///
/// A delete the store refuses (access denied, a failed precondition, or a
/// permanent error) keeps the rest of its own supersession chain for the next
/// pass and is counted in [`SupersededSweepOutcome::deletes_refused`]; the
/// other chains are still collected. A pass in which every delete it attempted
/// was refused fails with the first refusal's error, and any other store error
/// fails the pass.
pub async fn sweep_superseded(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<SupersededSweepOutcome> {
    let mut reach = SnapshotReachability::new();
    let (outcome, _data_bytes) = sweep_superseded_impl(
        &mut reach,
        store,
        clock,
        config,
        lease,
        tenant,
        signal,
        shard,
        None,
        SweepMode::Delete,
    )
    .await?;
    Ok(outcome)
}

/// Whether a [`sweep_superseded_impl`] pass deletes what it cleared, or only
/// computes the gate and the holds.
///
/// [`SweepMode::GateOnly`] is the pass the erasure-request guard decides from.
/// It is not `dry_run`,
/// which reports what a deleting pass would have removed: `records_deleted`
/// and `data_deleted` are always zero here, and the only outputs a caller
/// reads are [`SupersededSweepOutcome::held_request_ids`] and
/// [`SupersededSweepOutcome::held_truncated_buckets`].
///
/// An observing pass gathers strictly more than a deleting one, save the chain
/// groups entered from a version 2 record, which apply no request and so hold
/// none. The two filters that decide whether a chain is *collectable yet*, the
/// protection horizon and the skip of a record a present rewrite's chain group
/// holds, say
/// nothing about whether a HEAD-named part still resolves that chain's inputs,
/// which is the only question the erasure filter's hold turns on. So both
/// filters apply to deletion alone, and every chain in scope is gathered and
/// gated for the holds. A chain the gate clears contributes nothing either
/// way; a chain the gate blocks contributes the same request ids and buckets
/// whatever its age.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepMode {
    Delete,
    GateOnly,
}

impl SweepMode {
    /// Whether this pass may skip a chain the horizon, or a present successor,
    /// keeps from being deleted this pass. Only a deleting pass may: an
    /// observing pass has to see every chain, because a young chain's inputs
    /// are more resolvable than an aged one's, not less.
    fn gathers_only_deletable(self) -> bool {
        matches!(self, SweepMode::Delete)
    }
}

/// Shared implementation behind [`sweep_superseded`] (whole-shard, `hours:
/// None`) and [`sweep_shard_zoned`] (hour-scoped, `hours: Some(_)`).
///
/// `reach` is the pass's [`SnapshotReachability`] cache: HEAD is read at most
/// once for the pass and each covering snapshot part at most once, never once
/// per input, and a pass with no horizon-passed record reads neither.
///
/// Also returns the bytes of the data objects and parts
/// [`SupersededSweepOutcome::data_deleted`] counts, each charged at the
/// `object_size` the record naming it carries, so sizing them costs no
/// request ([`SweepReport::superseded_data_bytes`]). A part is charged when the
/// chain record naming it is deleted, not when the part is.
#[allow(clippy::too_many_arguments)]
async fn sweep_superseded_impl(
    reach: &mut SnapshotReachability,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    hours: Option<&[u32]>,
    mode: SweepMode,
) -> Result<(SupersededSweepOutcome, u64)> {
    let now = clock.now_ns();
    // Both skips below narrow what this pass may DELETE. An observing pass
    // (`SweepMode::GateOnly`) applies neither: see [`SweepMode`].
    let deleting = mode.gathers_only_deletable();
    let entries = list_commit_entries_scoped(store, tenant, signal, shard, hours).await?;
    // Every rewrite record in scope, read once for the pass, plus the set of
    // record keys those rewrites supersede. In a deleting pass a record another
    // present rewrite supersedes is not processed from its own listing entry:
    // it belongs to that rewrite's chain group, which is gated and deleted as
    // one unit. Doing both would let the gate clear a predecessor from its own
    // entry (where the group holds only that predecessor's outputs) while the
    // same predecessor's raw inputs are held from another entry, which is
    // exactly how a record could vanish ahead of the inputs it superseded.
    let mut record_versions: HashMap<String, String> = HashMap::new();
    let rewrites = load_rewrite_records(store, &entries, &mut record_versions).await?;
    let superseded_by_present: HashSet<&str> = rewrites
        .values()
        .map(|r| r.superseded_record_key.as_str())
        .filter(|k| !k.is_empty())
        .collect();
    // Every compaction record in scope, read once for the pass, and the input
    // identities the AUTHORITATIVE records of each bucket name. A record whose
    // inputs overlap another's may be the loser of its overlap component, and
    // the resolver serves an input only the loser names as a raw L0 segment
    // rather than from any part: see [`AuthoritativeInputs`].
    let compactions = load_compaction_records(store, &entries, &mut record_versions).await?;
    let authoritative = AuthoritativeInputs::from_records(&compactions);
    // What version 2 records add: a chain group entered from each one at the
    // head of its chain, and each erasure-dominated one joining its rewrite's
    // group. A deleting pass skips a dominated record, and every
    // record below it, from its own entry exactly as it skips a record a
    // rewrite names: all of them are in that rewrite's group.
    let version_2 = Version2Groups::from_records(&compactions, &rewrites, tenant, signal, shard);
    let superseded_by_present: HashSet<&str> = superseded_by_present
        .into_iter()
        .chain(version_2.rewrite_members.iter().map(String::as_str))
        .collect();

    // Phase A: gather every group this pass could delete, deduplicated by
    // chain identity across the whole pass. Two live rewrites naming the same
    // `superseded_record_key` gather the identical chain, so without the
    // dedup every key in it is gated twice and counted twice, and
    // `records_deleted` / `data_deleted` exceed the number of distinct objects
    // the pass removed.
    let mut groups: Vec<SupersededGroup> = Vec::new();
    // The record each group was first gathered from, by index into `groups`:
    // the key of the group's unnamed-since marker (ADR-1133 decision 1).
    let mut entered_from: Vec<String> = Vec::new();
    let mut by_identity: HashMap<String, usize> = HashMap::new();
    // Buckets holding a rewrite this deleting pass left for its horizon, and
    // the buckets and applied requests of the chains a walk refused.
    let mut young_rewrite_buckets: HashSet<u32> = HashSet::new();
    let mut refused_buckets: BTreeSet<u32> = BTreeSet::new();
    let mut refused_request_ids: BTreeSet<String> = BTreeSet::new();
    let mut refuse = |bucket: u32, applied: Vec<String>, reason: ChainRefusal, entry_key: &str| {
        refused_request_ids.extend(applied);
        if refused_buckets.insert(bucket) {
            tracing::warn!(
                tenant_hash = %tenant.to_hex(),
                signal = signal.key_prefix(),
                shard,
                ingest_hour_bucket = bucket,
                record_key = %entry_key,
                reason = reason.reason(),
                "superseded-input sweep: a supersession chain walk was refused; nothing of the \
                 chain is reclaimed, and it is held in a truncated bucket"
            );
        }
    };
    for (key, entry) in &entries {
        // An observing pass gathers this entry too. The successor's own gather
        // covers the same chain only when the successor superseded a whole
        // record; a successor that superseded raw L0 inputs directly never
        // walks back past them, so skipping the predecessor here would drop
        // its chain from the holds entirely.
        if deleting && superseded_by_present.contains(key.as_str()) {
            continue;
        }
        // Both a compaction record and a selective-erasure rewrite record
        // (ADR-0064 decision 3 point 6) render their superseded inputs
        // collectable by this one rule; a raw commit record or tombstone names
        // no superseded input. A compaction record is fetched NotFound-
        // tolerantly, and a rewrite absent from `rewrites` is skipped the same
        // way: a crash-interrupted prior pass can leave a listed key gone.
        //
        // `applied` is the live record's own drops. Everything in the groups it
        // gathers predates them: a group's L1 parts are the pre-image this
        // record erased a subject out of, and its raw L0 inputs are the
        // pre-image below that. So a hold on the group is a hold on those
        // requests' `.dreq`s, on top of whatever the generations inside the
        // group applied themselves.
        let (gathered, applied) = match entry {
            BucketEntry::CompactionRecord(_) => {
                let Some(record) = compactions.get(key) else {
                    continue;
                };
                // Horizon gate anchored on the durable created_unix_ns. It
                // bounds when the inputs may be DELETED; an observing pass
                // gathers them whatever the record's age.
                if deleting
                    && now
                        < record
                            .created_unix_ns
                            .saturating_add(config.protection_horizon_ns)
                {
                    continue;
                }
                let superseded = authoritative.superseded_view(record);
                let mut gathered =
                    gather_l0_inputs(store, tenant, signal, shard, &superseded).await?;
                // A version 2 record also supersedes the record it names, and
                // that record's own predecessors down a version 2 chain: one
                // group, gated on this record's horizon. Only a deleting pass
                // gathers it. The group applied no erasure request and a
                // missing link in it hides none, so it can add nothing to an
                // observing pass's holds. A refused walk reclaims nothing the
                // entry gathered.
                if deleting && version_2.heads.contains(key) {
                    match gather_superseded_chain(
                        store,
                        tenant,
                        signal,
                        shard,
                        &record.superseded_record_key,
                        ChainEntry::Version2,
                        Version2Links::Follow,
                    )
                    .await?
                    {
                        ChainWalk::Gathered(chain) => gathered.extend(chain),
                        ChainWalk::Refused(reason) => {
                            refuse(record.ingest_hour_bucket, Vec::new(), reason, key);
                            continue;
                        }
                    }
                }
                (gathered, Vec::new())
            }
            BucketEntry::RewriteRecord(_) => {
                let Some(record) = rewrites.get(key) else {
                    continue;
                };
                // Horizon gate anchored on the rewrite's own durable
                // created_unix_ns: that is the instant it superseded its
                // inputs, so a query pinned before it is drained by the
                // protection horizon exactly as for a compaction record. An
                // observing pass skips the gate: a rewrite still inside its
                // horizon is the case where a stale HEAD is MOST likely to
                // resolve the chain's inputs, so the erasure filter's hold has
                // to see it.
                if deleting
                    && now
                        < record
                            .created_unix_ns
                            .saturating_add(config.protection_horizon_ns)
                {
                    young_rewrite_buckets.insert(record.ingest_hour_bucket);
                    continue;
                }
                let applied: Vec<String> = record
                    .drops
                    .iter()
                    .map(|d| d.request_id.clone())
                    .filter(|id| !id.is_empty())
                    .collect();
                if !record.inputs.is_empty() {
                    // RawL0 rewrite: the same L0 commit records + data objects
                    // a compaction over the same inputs would supersede.
                    (
                        gather_l0_inputs(store, tenant, signal, shard, record).await?,
                        applied,
                    )
                } else {
                    // Predecessor rewrite: the whole supersession chain behind
                    // it, down to the raw L0 inputs the oldest generation
                    // superseded. Rule 3 cannot collect a superseded
                    // generation's parts while its record still references
                    // them, so this rule removes records and parts together.
                    // An erasure-dominated version 2 record re-encodes a
                    // record on this chain and may hold the erased subject, so
                    // it joins the group, the group of a predecessor already
                    // gone included.
                    //
                    // In a bucket whose version 2 supersession does not
                    // resolve, no pass follows a version 2 link. A walk
                    // refused for that, or for a chain past the depth bound
                    // or a revisit, is held in a truncated bucket in every
                    // pass kind, so a deleting pass and the observing pass
                    // refuse the same chains when they walk them, though only
                    // the observing pass walks every rewrite's chain, and the
                    // shard's other buckets are still swept.
                    let bucket = record.ingest_hour_bucket;
                    let links = if version_2.unresolved.contains(&bucket) {
                        Version2Links::Refuse
                    } else {
                        Version2Links::Follow
                    };
                    let mut gathered = match gather_superseded_chain(
                        store,
                        tenant,
                        signal,
                        shard,
                        &record.superseded_record_key,
                        ChainEntry::Rewrite,
                        links,
                    )
                    .await?
                    {
                        ChainWalk::Gathered(gathered) => gathered,
                        ChainWalk::Refused(reason) => {
                            refuse(bucket, applied, reason, key);
                            continue;
                        }
                    };
                    if gathered.is_empty() {
                        gathered.push(SupersededGroup::over_absent_predecessor(
                            bucket,
                            &record.superseded_record_key,
                        ));
                    }
                    for group in &mut gathered {
                        version_2.join_dominated(group, &compactions)?;
                    }
                    gathered.retain(|group| group.object_count() > 0);
                    (gathered, applied)
                }
            }
            BucketEntry::CommitRecord(_) | BucketEntry::Tombstone(_) => continue,
        };

        for mut group in gathered {
            group.request_ids.extend(applied.iter().cloned());
            match by_identity.get(&group.identity) {
                Some(&index) => groups[index].absorb_duplicate(group),
                None => {
                    by_identity.insert(group.identity.clone(), groups.len());
                    groups.push(group);
                    entered_from.push(key.clone());
                }
            }
        }
    }

    // Phase B: gate every group, once. Nothing is deleted before every group
    // has an answer, so a held group never splits a delete phase.
    //
    // The lease/legal-hold check is per group, not per key: a group is one
    // indivisible deletion unit, and its phase order exists so a record always
    // outlives the objects it superseded. Skipping only the protected keys
    // inside the phases breaks that order, and a prefix-scoped legal hold
    // covering the data keys but not the commit prefix is exactly the shape
    // that does it -- the records that erased a subject go while the bytes
    // holding it stay.
    //
    // The HEAD-reachability gate (ADR-0020 delete blocker) is object-granular
    // because this rule deletes individual objects out of a bucket whose
    // surviving compaction or rewrite outputs the snapshot legitimately still
    // names. A group is indivisible there too: a predecessor record must
    // outlive every part it names, or rule 3 would collect those parts on the
    // next pass and undo the hold; and a whole supersession chain is one group
    // so a HEAD that still names the oldest generation's raw inputs holds
    // every record above them too.
    let mut outcome = SupersededSweepOutcome::default();
    if deleting {
        outcome.dominated_records_unattached =
            version_2.count_unattached(&groups, &young_rewrite_buckets, tenant, signal, shard);
    }
    // A refused chain is not deleted, so it is held. Its walk collected no
    // request a rewrite below the refusing one applied, so its bucket is
    // reported as truncated.
    outcome.held_request_ids.extend(refused_request_ids);
    outcome
        .held_truncated_buckets
        .extend(
            refused_buckets
                .into_iter()
                .map(|ingest_hour_bucket| HeldBucket {
                    shard,
                    ingest_hour_bucket,
                }),
        );
    // The pinned-query window (ADR-1133). Every group a record's entry
    // gathered shares that record's one unnamed-since marker, so the marker
    // stands for all of them: it is written only when HEAD names none of
    // them, and a group whose siblings are named or unreadable waits with
    // them. A lease-held group's HEAD answer counts too, so the marker covers
    // it once the hold lifts, and the marker is kept while it is held. An
    // observing pass and a dry run only read markers.
    let policy = if deleting && !config.dry_run {
        MarkerPolicy::Write
    } else {
        MarkerPolicy::ReadOnly
    };
    let ctx = MarkerContext::new(clock, config, policy);
    let mut head_gates: Vec<SnapshotGate> = Vec::with_capacity(groups.len());
    let mut marker_keys: Vec<Option<String>> = Vec::with_capacity(groups.len());
    // Per marker key: the combined HEAD answer of every group sharing it.
    let mut combined: HashMap<String, SnapshotGate> = HashMap::new();
    for (group, entry_key) in groups.iter().zip(&entered_from) {
        let head_gate = reach
            .object_gate(
                store,
                tenant,
                signal,
                group.ingest_hour_bucket,
                &group.objects,
            )
            .await?;
        head_gates.push(head_gate);
        if group.objects.is_empty() {
            marker_keys.push(None);
            continue;
        }
        let marker_key = keys::record_unnamed_marker_key(entry_key).unwrap_or_default();
        let slot = combined
            .entry(marker_key.clone())
            .or_insert(SnapshotGate::Clear);
        *slot = combine_head_gates(*slot, head_gate);
        marker_keys.push(Some(marker_key));
    }
    let mut verdicts: HashMap<String, SnapshotGate> = HashMap::new();
    for (index, entry_key) in entered_from.iter().enumerate() {
        let Some(marker_key) = &marker_keys[index] else {
            continue;
        };
        if verdicts.contains_key(marker_key) {
            continue;
        }
        let head_gate = combined
            .get(marker_key)
            .copied()
            .unwrap_or(SnapshotGate::Blocked(SnapshotBlock::Unreadable));
        let anchor = superseded_anchor(entry_key, &compactions, &rewrites, &record_versions);
        let verdict = match (marker_key.is_empty(), anchor) {
            // A key that does not reconstruct, or an anchor this pass did
            // not read: the marker cannot be checked, so the group blocks.
            (true, _) | (_, None) => SnapshotGate::Blocked(SnapshotBlock::Unreadable),
            (false, Some(anchor)) => {
                reach
                    .marker_gate(store, &ctx, tenant, signal, marker_key, &anchor, head_gate)
                    .await
            }
        };
        verdicts.insert(marker_key.clone(), verdict);
    }

    let mut cleared: Vec<&SupersededGroup> = Vec::with_capacity(groups.len());
    let mut cleared_marker_keys: Vec<Option<&str>> = Vec::with_capacity(groups.len());
    // Marker keys with at least one group not cleared this pass: their marker
    // still gates something and is not retired.
    let mut marker_still_gating: HashSet<&str> = HashSet::new();
    for (index, group) in groups.iter().enumerate() {
        let marker_key = marker_keys[index].as_deref();
        let gate = match (head_gates[index], marker_key) {
            (SnapshotGate::Clear, Some(k)) => match verdicts.get(k).copied() {
                Some(SnapshotGate::Clear) => SnapshotGate::Clear,
                // This group is unnamed, but a sibling under the same marker
                // is named: the window has not started for the marker.
                Some(SnapshotGate::Blocked(SnapshotBlock::Named)) => {
                    SnapshotGate::Blocked(SnapshotBlock::PinnedWindow)
                }
                Some(blocked) => blocked,
                None => SnapshotGate::Blocked(SnapshotBlock::Unreadable),
            },
            (gate, _) => gate,
        };
        if gate != SnapshotGate::Clear
            && let Some(k) = marker_key
        {
            marker_still_gating.insert(k);
        }
        if let Some(protected) = group.protected_key(lease) {
            if let Some(k) = marker_key {
                marker_still_gating.insert(k);
            }
            tracing::warn!(
                tenant_hash = %tenant.to_hex(),
                signal = signal.key_prefix(),
                shard,
                ingest_hour_bucket = group.ingest_hour_bucket,
                protected_key = %protected,
                group_objects = group.object_count(),
                "superseded-input sweep: a lease or legal hold protects a key in a supersession \
                 chain, so the whole chain is skipped this pass; deleting the unprotected part of \
                 it would break the order that keeps a record alive until the objects it \
                 superseded are gone"
            );
            outcome.chain_groups_held_by_legal_hold += 1;
            outcome.note_hold(group, shard);
            continue;
        }
        match gate {
            SnapshotGate::Clear => {
                cleared.push(group);
                cleared_marker_keys.push(marker_key);
            }
            SnapshotGate::Blocked(SnapshotBlock::Named) => {
                outcome.held_by_snapshot += group.object_count();
                outcome.note_hold(group, shard);
            }
            SnapshotGate::Blocked(SnapshotBlock::Unreadable) => {
                outcome.held_by_unreadable_head += group.object_count();
                outcome.note_hold(group, shard);
            }
            SnapshotGate::Blocked(SnapshotBlock::PinnedWindow) => {
                outcome.held_by_pinned_window += group.object_count();
                outcome.note_hold(group, shard);
            }
        }
    }
    outcome.unnamed_markers = reach.marker_stats().clone();

    if !deleting {
        // Nothing is deleted, and nothing in the outcome says which of these
        // groups a deleting pass could have collected: an observing pass
        // answers one question, "does a HEAD-named part still resolve this
        // chain", and a young chain answers it exactly as an aged one does.
        // The two object counters can exceed the number of distinct objects
        // here, because a predecessor is gathered both from its own entry and
        // from its successor's chain walk; only the held request ids and
        // buckets are consumed.
        return Ok((outcome, 0));
    }

    // Phase C: every cleared group's superseded-input records first, then every
    // cleared group's data objects (docs/consistency-model.md): a crash
    // between the two phases leaves record-less data (orphan GC) or an
    // unreferenced part (rule 3), never a record pointing at a deleted
    // object.
    //
    // The chains' own records go last of all, oldest generation first, so a
    // rewrite record outlives every input it superseded: a crash inside this
    // rule leaves the record still naming inputs that are already gone
    // (harmless, and the next pass finishes the job), never a surviving input
    // with the record that erased a subject out of it deleted, which would
    // leave the erasure request's filter with nothing durable to discover it
    // by.
    //
    // Groups of distinct identity can still share a key: a dominated version
    // 2 record naming an absent record joins every group that ended there. A
    // key is deleted and counted once per pass.
    //
    // A delete the store refuses ([`delete_refused`]) stops only the group it
    // belongs to: that group deletes none of its later keys this pass, in any
    // of the three loops, so its own deletes stop where a crash at that key
    // would stop them. A group that meets a key another group's delete was
    // refused on stops there too. Another group can still delete a key in a
    // stopped group's tail when the two share it, and that is safe: the only
    // keys groups of distinct identity share are a joined dominated version 2
    // record and its parts, which `join_dominated` adds to every such group
    // together, so a group that deletes them deletes the record's parts in the
    // data loop and the record itself in the chain loop, and no record left
    // in the stopped group names a key the other group removed. Every stopped
    // group is reported held, since objects it superseded may still be
    // present.
    //
    // A pass in which every delete it attempted was refused fails with the
    // first refusal's error: a credential that may not delete anywhere refuses
    // every group, and that is a store-wide fault, not one chain's. Any other
    // store error fails the pass.
    let mut deleted: HashSet<&str> = HashSet::new();
    let mut refused: HashSet<&str> = HashSet::new();
    let mut stopped: Vec<bool> = vec![false; cleared.len()];
    let mut deletes_succeeded = 0usize;
    let mut data_bytes = 0u64;
    let mut first_refusal: Option<StoreError> = None;
    for delete_loop in [
        DeleteLoop::InputRecords,
        DeleteLoop::Data,
        DeleteLoop::ChainRecords,
    ] {
        // Each marker goes after the objects it gated and before any record
        // (ADR-1133 decision 6), and only once every group under it has been
        // deleted whole. A failed marker delete keeps the records of every
        // group under it for the next pass.
        if delete_loop == DeleteLoop::ChainRecords && policy == MarkerPolicy::Write {
            let mut retired: HashMap<&str, bool> = HashMap::new();
            for (index, marker_key) in cleared_marker_keys.iter().enumerate() {
                let Some(marker_key) = *marker_key else {
                    continue;
                };
                if stopped[index] {
                    marker_still_gating.insert(marker_key);
                }
            }
            for (index, marker_key) in cleared_marker_keys.iter().enumerate() {
                let Some(marker_key) = *marker_key else {
                    continue;
                };
                if marker_still_gating.contains(marker_key) {
                    continue;
                }
                let ok = match retired.get(marker_key) {
                    Some(&ok) => ok,
                    None => {
                        let ok = reach.retire_marker(store, marker_key).await.is_ok();
                        retired.insert(marker_key, ok);
                        ok
                    }
                };
                if !ok {
                    stopped[index] = true;
                }
            }
            outcome.unnamed_markers = reach.marker_stats().clone();
        }
        for (index, group) in cleared.iter().enumerate() {
            for k in group.loop_keys(delete_loop) {
                if stopped[index] || refused.contains(k.as_str()) {
                    stopped[index] = true;
                    break;
                }
                if deleted.contains(k.as_str()) {
                    continue;
                }
                if !config.dry_run {
                    match store.delete(k).await {
                        Ok(()) => deletes_succeeded += 1,
                        Err(e) if delete_refused(&e) => {
                            tracing::warn!(
                                tenant_hash = %tenant.to_hex(),
                                signal = signal.key_prefix(),
                                shard,
                                ingest_hour_bucket = group.ingest_hour_bucket,
                                key = %k,
                                error = %e,
                                "superseded-input sweep: the store refused a delete; the rest of \
                                 this supersession chain is kept for the next pass over its hour, \
                                 and the other chains are still collected"
                            );
                            refused.insert(k);
                            outcome.deletes_refused += 1;
                            first_refusal.get_or_insert(e);
                            stopped[index] = true;
                            break;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                deleted.insert(k);
                if delete_loop == DeleteLoop::Data {
                    outcome.data_deleted += 1;
                    data_bytes =
                        data_bytes.saturating_add(group.data_sizes.get(k).copied().unwrap_or(0));
                } else {
                    outcome.records_deleted += 1;
                    data_bytes =
                        data_bytes.saturating_add(group.part_bytes.get(k).copied().unwrap_or(0));
                }
            }
        }
    }
    if deletes_succeeded == 0
        && let Some(e) = first_refusal
    {
        return Err(e.into());
    }
    for (group, _) in cleared.iter().zip(&stopped).filter(|(_, s)| **s) {
        outcome.note_hold(group, shard);
    }
    outcome.unnamed_marker_reap = reach
        .reap_after_pass(store, clock, config, tenant, signal, false)
        .await;
    outcome.unnamed_markers = reach.marker_stats().clone();
    Ok((outcome, data_bytes))
}

/// The HEAD answer for a set of groups sharing one marker: unreadable if any
/// is, else named if any is, else clear.
fn combine_head_gates(a: SnapshotGate, b: SnapshotGate) -> SnapshotGate {
    use SnapshotBlock::{Named, Unreadable};
    match (a, b) {
        (SnapshotGate::Blocked(Unreadable), _) | (_, SnapshotGate::Blocked(Unreadable)) => {
            SnapshotGate::Blocked(Unreadable)
        }
        (SnapshotGate::Blocked(Named), _) | (_, SnapshotGate::Blocked(Named)) => {
            SnapshotGate::Blocked(Named)
        }
        (SnapshotGate::Blocked(other), _) | (_, SnapshotGate::Blocked(other)) => {
            SnapshotGate::Blocked(other)
        }
        (SnapshotGate::Clear, SnapshotGate::Clear) => SnapshotGate::Clear,
    }
}

/// The anchor identity of the record a chain group was entered from, from
/// the copies of the records this pass already read, or `None` when the pass
/// did not read it (the marker then cannot be checked, and blocks).
fn superseded_anchor(
    entry_key: &str,
    compactions: &HashMap<String, CompactionRecord>,
    rewrites: &HashMap<String, RewriteRecord>,
    versions: &HashMap<String, String>,
) -> Option<MarkerAnchor> {
    let anchor_unix_ns = compactions
        .get(entry_key)
        .map(|r| r.created_unix_ns)
        .or_else(|| rewrites.get(entry_key).map(|r| r.created_unix_ns))?;
    Some(MarkerAnchor {
        kind: MarkerKind::Superseded,
        key: entry_key.to_string(),
        anchor_unix_ns,
        version: versions.get(entry_key)?.clone(),
    })
}

/// Whether a failed delete is a refusal of that one object, which rule 2's
/// phase C tolerates per supersession chain: access denied (a deny policy on
/// part of the keyspace, for example), a failed precondition, and a permanent
/// error. A pass in which every delete it attempted was refused still fails,
/// with the first refusal's error; a pass with at least one successful delete
/// and some refusals succeeds and counts them. A retryable error, a read-only
/// store, and a backend with no delete support fail the pass on the first
/// delete.
fn delete_refused(e: &StoreError) -> bool {
    matches!(
        e,
        StoreError::AccessDenied(_) | StoreError::PreconditionFailed | StoreError::Permanent(_)
    )
}

/// GET every rewrite record among `entries`, keyed by its commit key, tolerant
/// of a key that vanished between the pass's LIST and now.
///
/// `versions` receives each record's store version, the anchor identity of
/// an unnamed-since marker keyed by it (ADR-1133 decision 2).
async fn load_rewrite_records(
    store: &dyn ObjectStoreBackend,
    entries: &[(String, BucketEntry)],
    versions: &mut HashMap<String, String>,
) -> Result<HashMap<String, RewriteRecord>> {
    let mut out: HashMap<String, RewriteRecord> = HashMap::new();
    for (key, entry) in entries {
        if !matches!(entry, BucketEntry::RewriteRecord(_)) {
            continue;
        }
        if let Some((record, version)) = get_rewrite_record_versioned(store, key).await? {
            out.insert(key.clone(), record);
            versions.insert(key.clone(), version);
        }
    }
    Ok(out)
}

/// GET every compaction record among `entries`, keyed by its commit key,
/// tolerant of a key that vanished between the pass's LIST and now.
/// `versions` receives each record's store version, as for
/// [`load_rewrite_records`].
async fn load_compaction_records(
    store: &dyn ObjectStoreBackend,
    entries: &[(String, BucketEntry)],
    versions: &mut HashMap<String, String>,
) -> Result<HashMap<String, CompactionRecord>> {
    let mut out: HashMap<String, CompactionRecord> = HashMap::new();
    for (key, entry) in entries {
        if !matches!(entry, BucketEntry::CompactionRecord(_)) {
            continue;
        }
        if let Some((record, version)) = get_compaction_record_versioned(store, key).await? {
            out.insert(key.clone(), record);
            versions.insert(key.clone(), version);
        }
    }
    Ok(out)
}

/// The input identities the authoritative compaction records of each
/// ingest-hour bucket name.
///
/// Compaction records whose input sets overlap resolve to one authoritative
/// record per overlap component
/// ([`select_authoritative_compaction_records`], the same function the
/// snapshot resolver and the index fold use). The losers' parts are served
/// from nowhere, and an input only a loser names is served as a raw L0
/// segment: it is the sole server of its rows. So an input is superseded only
/// where an authoritative record names it. Treating a loser's whole input set
/// as superseded deletes that sole server and turns duplicate rows into
/// missing rows. A loser's own parts stay referenced for as long as the losing
/// record exists (the reference map does not distinguish winners from losers,
/// and a node that has not adopted this rule may still serve them), so this
/// pass reclaims nothing of the loser's.
///
/// A record a present version 2 record supersedes is excluded by the selector
/// like a loser, but it is not kept like one: it and its parts are reclaimed
/// as a chain group entered from the version 2 record, and an
/// erasure-dominated version 2 record goes with its rewrite's chain group
/// ([`Version2Groups`]). Neither group holds a raw L0 input that only this
/// rule's compaction arm would otherwise decide on, so the input view here is
/// unchanged by them.
#[derive(Default)]
struct AuthoritativeInputs {
    by_bucket: HashMap<u32, HashSet<(String, u64, u64)>>,
}

impl AuthoritativeInputs {
    fn from_records(records: &HashMap<String, CompactionRecord>) -> Self {
        // Per bucket, because an overlap component is a property of one
        // ingest-hour bucket: that is the unit the resolver reads.
        let mut per_bucket: HashMap<u32, Vec<(&str, &CompactionRecord)>> = HashMap::new();
        for (key, record) in records {
            per_bucket
                .entry(record.ingest_hour_bucket)
                .or_default()
                .push((key.as_str(), record));
        }
        let mut by_bucket: HashMap<u32, HashSet<(String, u64, u64)>> = HashMap::new();
        for (bucket, in_bucket) in per_bucket {
            // An input is superseded here only where an authoritative record
            // names it both with version 2 supersession honoured (the rule the
            // resolver serves by) and with it ignored (the rule this pass
            // deleted by before the resolver honoured it). Excluding a
            // predecessor can hand its overlap component to another record, so
            // the honoured view alone can name an input the ignored view serves
            // raw; the intersection keeps either view from widening what this
            // pass deletes while a predecessor is present. A bucket whose version 2
            // supersession does not resolve (a cycle, a chain past the depth
            // bound, or a version 2 record whose inputs differ from the record
            // it names) fails every resolve over it; this pass treats none of
            // its inputs as superseded rather than guess, and leaves the other
            // buckets alone.
            let honoured = match select_authoritative_compaction_records(&in_bucket) {
                Ok(selection) => selection,
                Err(error) => {
                    tracing::error!(
                        ingest_hour_bucket = bucket,
                        %error,
                        "superseded-input sweep: unresolvable compaction supersession; \
                         no input of this bucket is treated as superseded"
                    );
                    by_bucket.insert(bucket, HashSet::new());
                    continue;
                }
            };
            let ignored: Vec<(&str, CompactionRecord)> = in_bucket
                .iter()
                .map(|(key, record)| {
                    (
                        *key,
                        CompactionRecord {
                            superseded_record_key: String::new(),
                            ..(*record).clone()
                        },
                    )
                })
                .collect();
            // With every `superseded_record_key` cleared there is no chain to
            // walk, so this selection cannot fail; an error here would still
            // mean "treat nothing as superseded".
            let Ok(ignored_selection) = select_authoritative_compaction_records(&ignored) else {
                by_bucket.insert(bucket, HashSet::new());
                continue;
            };
            let named = |selection: &ravel_catalog::AuthoritativeSelection<'_>| {
                let mut identities: HashSet<(String, u64, u64)> = HashSet::new();
                for (key, record) in &in_bucket {
                    if selection.is_excluded(key) {
                        continue;
                    }
                    for input in &record.inputs {
                        identities.insert((
                            input.writer_id.clone(),
                            input.writer_epoch,
                            input.writer_seq,
                        ));
                    }
                }
                identities
            };
            let ignored_named = named(&ignored_selection);
            let identities = named(&honoured)
                .into_iter()
                .filter(|identity| ignored_named.contains(identity))
                .collect();
            by_bucket.insert(bucket, identities);
        }
        Self { by_bucket }
    }

    /// The subset of `record`'s inputs this rule may treat as superseded: the
    /// ones an authoritative record of the same bucket also names. For a
    /// winner that is its whole input set; for a loser it is the overlap with
    /// its winner.
    fn superseded_view(&self, record: &CompactionRecord) -> SupersededSubset {
        let authoritative = self.by_bucket.get(&record.ingest_hour_bucket);
        let inputs = record
            .inputs
            .iter()
            .filter(|input| {
                authoritative.is_some_and(|set| {
                    set.contains(&(
                        input.writer_id.clone(),
                        input.writer_epoch,
                        input.writer_seq,
                    ))
                })
            })
            .cloned()
            .collect();
        SupersededSubset {
            inputs,
            ingest_hour_bucket: record.ingest_hour_bucket,
        }
    }
}

/// What version 2 compaction records mean for rule 2's chain groups, per
/// ingest-hour bucket, derived through the catalog's shared rules
/// ([`erasure_dominated_compaction_records`], then
/// [`select_authoritative_compaction_records`] over the records left).
///
/// A version 2 record that no present version 2 record supersedes and no
/// rewrite dominates enters a chain group of its own ([`ChainEntry::Version2`])
/// that holds the records below it and their parts. A dominated version 2
/// record joins the chain group of the rewrite whose chain reaches the record
/// it names ([`Self::join_dominated`]): a walk down a rewrite's chain follows
/// what each record supersedes, so it reaches a dominated record only when a
/// record on the chain names it.
///
/// A bucket whose supersession does not resolve (a cycle, a chain past the
/// depth bound, a version 2 record whose inputs differ from the record it
/// names) fails every resolve over it. It gets no version 2 chain group and no
/// dominated record, and no pass's rewrite chain walk steps past a version 2
/// record in it ([`Version2Links::Refuse`]), so this pass
/// reclaims nothing on account of its version 2 records. When it is the
/// compaction selector that failed, [`AuthoritativeInputs`] likewise treats
/// none of the bucket's inputs as superseded; when only the erasure-dominance
/// resolution failed (a rewrite chain past the depth bound), it still does.
#[derive(Default)]
struct Version2Groups {
    /// The version 2 records that enter a chain group of their own.
    heads: HashSet<String>,
    /// Records that belong to a present rewrite's chain group although the
    /// rewrite may not name them: every dominated version 2 record, and every
    /// record one of those names down its chain. A deleting pass does not
    /// process them from their own listing entry, for the reason it skips a
    /// record a rewrite names. The `(4, 4)` counts in
    /// `sweep_reclaims_an_erasure_dominated_version_2_record_with_its_rewrite`
    /// fail without the skip: the dominated record's own entry gathers the raw
    /// inputs a second time.
    rewrite_members: HashSet<String>,
    /// Each bucket's dominated version 2 records.
    dominated: HashMap<u32, Vec<String>>,
    /// The buckets whose version 2 supersession does not resolve.
    unresolved: HashSet<u32>,
}

impl Version2Groups {
    fn from_records(
        compactions: &HashMap<String, CompactionRecord>,
        rewrites: &HashMap<String, RewriteRecord>,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
    ) -> Self {
        let mut compactions_by_bucket: HashMap<u32, Vec<(&str, &CompactionRecord)>> =
            HashMap::new();
        for (key, record) in compactions {
            compactions_by_bucket
                .entry(record.ingest_hour_bucket)
                .or_default()
                .push((key.as_str(), record));
        }
        let mut rewrites_by_bucket: HashMap<u32, Vec<(&str, &RewriteRecord)>> = HashMap::new();
        for (key, record) in rewrites {
            rewrites_by_bucket
                .entry(record.ingest_hour_bucket)
                .or_default()
                .push((key.as_str(), record));
        }
        let mut out = Self::default();
        for (bucket, in_bucket) in compactions_by_bucket {
            if in_bucket
                .iter()
                .all(|(_, record)| record.superseded_record_key.is_empty())
            {
                continue;
            }
            let bucket_rewrites = rewrites_by_bucket.remove(&bucket).unwrap_or_default();
            let prefix = keys::commit_shard_hour_prefix(tenant, signal, shard, bucket)
                .unwrap_or_else(|_| format!("ingest hour bucket {bucket}"));
            let unresolvable = |error: ravel_catalog::CatalogError| {
                tracing::error!(
                    ingest_hour_bucket = bucket,
                    %error,
                    "superseded-input sweep: unresolvable compaction supersession; nothing is \
                     reclaimed on account of this bucket's version 2 records"
                );
            };
            let dominated =
                match erasure_dominated_compaction_records(&in_bucket, &bucket_rewrites, &prefix) {
                    Ok(dominated) => dominated,
                    Err(error) => {
                        unresolvable(error);
                        out.unresolved.insert(bucket);
                        continue;
                    }
                };
            let candidates: Vec<(&str, &CompactionRecord)> = in_bucket
                .iter()
                .copied()
                .filter(|(key, _)| !dominated.contains(key))
                .collect();
            let selection = match select_authoritative_compaction_records(&candidates) {
                Ok(selection) => selection,
                Err(error) => {
                    unresolvable(error);
                    out.unresolved.insert(bucket);
                    continue;
                }
            };
            for (key, record) in &candidates {
                if !record.superseded_record_key.is_empty() && !selection.superseded().contains(key)
                {
                    out.heads.insert((*key).to_string());
                }
            }
            let by_key: HashMap<&str, &CompactionRecord> = in_bucket.iter().copied().collect();
            let mut bucket_dominated: Vec<String> = Vec::with_capacity(dominated.len());
            for key in dominated {
                bucket_dominated.push(key.to_string());
                let mut cursor = Some(key);
                // Each step inserts a record not yet a member or stops, so this
                // ends within the bucket's record count whatever the shape.
                while let Some(member) = cursor {
                    if !out.rewrite_members.insert(member.to_string()) {
                        break;
                    }
                    cursor = by_key
                        .get(member)
                        .map(|record| record.superseded_record_key.as_str())
                        .filter(|named| by_key.contains_key(named));
                }
            }
            bucket_dominated.sort();
            out.dominated.insert(bucket, bucket_dominated);
        }
        out
    }

    /// Add to a rewrite's chain group every dominated version 2 record whose
    /// chain reaches one of the group's records, or the absent record the
    /// rewrite's walk ended at, with its parts. The records go ahead of the
    /// chain's own, newest first: a record is dominated only while the key it
    /// names is on a present rewrite's chain (present or not) or names a
    /// dominated record, so deleting the chain's records, or a dominated
    /// record before the one naming it, could leave a survivor of a crash
    /// undominated and served.
    fn join_dominated(
        &self,
        group: &mut SupersededGroup,
        compactions: &HashMap<String, CompactionRecord>,
    ) -> Result<()> {
        let Some(dominated) = self.dominated.get(&group.ingest_hour_bucket) else {
            return Ok(());
        };
        let mut in_group: HashSet<&str> = group
            .chain_record_keys
            .iter()
            .chain(group.absent_end.iter())
            .map(String::as_str)
            .collect();
        let mut joined: Vec<&str> = Vec::new();
        loop {
            let before = joined.len();
            for key in dominated {
                let Some(record) = compactions.get(key) else {
                    continue;
                };
                if in_group.contains(key.as_str())
                    || !in_group.contains(record.superseded_record_key.as_str())
                {
                    continue;
                }
                in_group.insert(key);
                joined.push(key);
            }
            if joined.len() == before {
                break;
            }
        }
        let mut joined_keys: Vec<String> = Vec::with_capacity(joined.len());
        for key in joined.into_iter().rev() {
            let Some(record) = compactions.get(key) else {
                continue;
            };
            let mut record_part_bytes = 0u64;
            for (part_key, object, size) in ChainLink::Compaction(record.clone()).part_targets()? {
                record_part_bytes = record_part_bytes.saturating_add(size);
                group.data_keys.push(part_key);
                group.objects.push(object);
            }
            group.part_bytes.insert(key.to_string(), record_part_bytes);
            joined_keys.push(key.to_string());
        }
        joined_keys.append(&mut group.chain_record_keys);
        group.chain_record_keys = joined_keys;
        Ok(())
    }

    /// Log and count every dominated record no gathered group holds, in a
    /// bucket where no rewrite was left for its horizon: it would otherwise be
    /// kept silently, with parts that re-encode a pre-erasure record.
    fn count_unattached(
        &self,
        groups: &[SupersededGroup],
        young_rewrite_buckets: &HashSet<u32>,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
    ) -> usize {
        let attached: HashSet<&str> = groups
            .iter()
            .flat_map(|group| group.chain_record_keys.iter().map(String::as_str))
            .collect();
        let mut unattached = 0;
        for (bucket, dominated) in &self.dominated {
            if young_rewrite_buckets.contains(bucket) {
                continue;
            }
            for key in dominated {
                if attached.contains(key.as_str()) {
                    continue;
                }
                tracing::warn!(
                    tenant_hash = %tenant.to_hex(),
                    signal = signal.key_prefix(),
                    shard,
                    ingest_hour_bucket = *bucket,
                    record_key = %key,
                    "superseded-input sweep: an erasure-dominated version 2 record is in no \
                     rewrite's chain group, so it is kept this pass"
                );
                unattached += 1;
            }
        }
        unattached
    }
}

/// The superseded-input view of one compaction record: its inputs narrowed to
/// the ones an authoritative record names. Carries the record's own bucket so
/// [`gather_l0_inputs`] reconstructs the same keys it would from the record.
struct SupersededSubset {
    inputs: Vec<CompactionInputIdentity>,
    ingest_hour_bucket: u32,
}

/// One of rule 2's phase C delete loops, in the order they run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteLoop {
    /// Every raw L0 input's commit record.
    InputRecords,
    /// The input data objects and every generation's parts.
    Data,
    /// The chain's own compaction and rewrite records, oldest first.
    ChainRecords,
}

/// One indivisible deletion unit for rule 2: the records to delete first, the
/// data objects to delete after them, and the snapshot identities those
/// objects carry so the HEAD-reachability gate can decide the whole unit at
/// once.
///
/// A raw-L0 input is its own group (one commit record plus one data object),
/// so a still-named input holds only itself. A whole supersession chain is one
/// group: every superseded generation's record and parts plus the raw L0 inputs
/// the oldest generation superseded, because deleting a record without the
/// objects below it would expose them to rule 3 or leave them resolvable with
/// nothing durable naming them as erased.
struct SupersededGroup {
    /// The ingest hour every object in the group sits in: the parent record's
    /// own bucket, which is also the hour whose covering snapshot parts the
    /// gate must read.
    ingest_hour_bucket: u32,
    record_keys: Vec<String>,
    data_keys: Vec<String>,
    /// The size of each L0 data object in `data_keys`, as its commit record's
    /// `object_size` records it. A part has no entry here: see `part_bytes`.
    data_sizes: HashMap<String, u64>,
    /// For each of `chain_record_keys`, the summed `object_size` of the parts
    /// it names, charged when that record's delete succeeds. A part delete that finds nothing still succeeds, so
    /// the next pass after a refused record delete deletes the same parts again.
    part_bytes: HashMap<String, u64>,
    /// The supersession chain's own compaction/rewrite records, oldest
    /// generation first, deleted after every object they superseded.
    chain_record_keys: Vec<String>,
    objects: Vec<SnapshotObject>,
    /// Every erasure request id the objects in this group predate: the drops of
    /// the live record that superseded the group, plus the drops of every
    /// generation inside it. A hold on the group is a hold on each of these
    /// requests' `.dreq`s.
    request_ids: BTreeSet<String>,
    /// The chain walk stopped at a generation whose record was already gone
    /// and was not a compaction record, so whatever requests that generation
    /// applied are named by no surviving record and cannot appear in
    /// `request_ids`.
    truncated: bool,
    /// The absent record the chain walk ended at, if it ended at one. A
    /// dominated version 2 record naming it belongs to this group
    /// ([`Version2Groups::join_dominated`]).
    absent_end: Option<String>,
    /// Dedup key: the oldest record this group deletes, which is the one thing
    /// two live rewrites over the same predecessor gather identically.
    identity: String,
}

impl SupersededGroup {
    /// The group of a rewrite whose predecessor, at `absent_key`, is already
    /// gone: nothing of the chain is left, but a dominated version 2 record
    /// naming the same key may still be.
    ///
    /// The group is truncated only when [`absent_link_cuts_chain`] says so. It
    /// survives only when a dominated version 2 record joins it, and a version
    /// 2 record names only a compaction record key.
    fn over_absent_predecessor(ingest_hour_bucket: u32, absent_key: &str) -> Self {
        Self {
            ingest_hour_bucket,
            record_keys: Vec::new(),
            data_keys: Vec::new(),
            data_sizes: HashMap::new(),
            part_bytes: HashMap::new(),
            chain_record_keys: Vec::new(),
            objects: Vec::new(),
            request_ids: BTreeSet::new(),
            truncated: absent_link_cuts_chain(absent_key),
            absent_end: Some(absent_key.to_string()),
            identity: absent_key.to_string(),
        }
    }

    /// Objects this group would delete, for the held counters.
    fn object_count(&self) -> usize {
        self.record_keys.len() + self.data_keys.len() + self.chain_record_keys.len()
    }

    /// The keys one of rule 2's phase C loops deletes from this group.
    fn loop_keys(&self, delete_loop: DeleteLoop) -> &[String] {
        match delete_loop {
            DeleteLoop::InputRecords => &self.record_keys,
            DeleteLoop::Data => &self.data_keys,
            DeleteLoop::ChainRecords => &self.chain_record_keys,
        }
    }

    /// Every key this group deletes, in delete order.
    fn keys(&self) -> impl Iterator<Item = &String> {
        self.record_keys
            .iter()
            .chain(self.data_keys.iter())
            .chain(self.chain_record_keys.iter())
    }

    /// The first key in this group the `lease` protects, if any.
    fn protected_key(&self, lease: &dyn LeaseCheck) -> Option<&str> {
        self.keys()
            .find(|k| lease.is_protected(k))
            .map(String::as_str)
    }

    /// Fold in a second gather of the same group (a sibling live rewrite over
    /// the same predecessor, or, in an observing pass, a predecessor gathered
    /// from its own entry and again through a successor's walk). The merged
    /// group carries both gathers' requests, a truncation either saw, and the
    /// union of their keys and objects: two walks entered from different links
    /// share the oldest record but not the generations above it, and a HEAD
    /// naming only a part the second walk reached must still hold the group,
    /// whichever gather the listing order put first.
    ///
    /// When one gather's keys contain the other's, which is every case of one
    /// walk entered below the other, its vectors are taken whole, so the
    /// merged group keeps a delete order a single walk produced. Otherwise the
    /// keys the first gather lacks are appended after its own, each vector in
    /// the second gather's order.
    fn absorb_duplicate(&mut self, other: SupersededGroup) {
        self.request_ids.extend(other.request_ids.iter().cloned());
        self.truncated |= other.truncated;
        // A key's recorded size is the same whichever gather read it.
        self.data_sizes.extend(
            other
                .data_sizes
                .iter()
                .map(|(key, size)| (key.clone(), *size)),
        );
        self.part_bytes.extend(
            other
                .part_bytes
                .iter()
                .map(|(key, size)| (key.clone(), *size)),
        );
        let (covers, covered) = {
            let mine: HashSet<&String> = self.keys().collect();
            let theirs: HashSet<&String> = other.keys().collect();
            (theirs.is_subset(&mine), mine.is_subset(&theirs))
        };
        if covers {
            return;
        }
        if covered {
            self.record_keys = other.record_keys;
            self.data_keys = other.data_keys;
            self.chain_record_keys = other.chain_record_keys;
            self.objects = other.objects;
            if other.absent_end.is_some() {
                self.absent_end = other.absent_end;
            }
            return;
        }
        // Neither gather covers the other. No input reaches this today: in a
        // deleting pass duplicates are sibling rewrites over one predecessor,
        // whose gathers are identical, and in an observing pass the
        // successor's walk contains the predecessor's own gather. Appending the
        // second gather's chain records after the first's would not keep one
        // walk's oldest-first order if they interleaved, so a change that makes
        // this branch reachable must merge chain_record_keys by generation.
        let union = |mine: &mut Vec<String>, theirs: Vec<String>| {
            let present: HashSet<String> = mine.iter().cloned().collect();
            mine.extend(theirs.into_iter().filter(|k| !present.contains(k)));
        };
        union(&mut self.record_keys, other.record_keys);
        union(&mut self.data_keys, other.data_keys);
        union(&mut self.chain_record_keys, other.chain_record_keys);
        let objects: HashSet<SnapshotObject> = self.objects.iter().copied().collect();
        self.objects
            .extend(other.objects.into_iter().filter(|o| !objects.contains(o)));
        if self.absent_end.is_none() {
            self.absent_end = other.absent_end;
        }
    }
}

/// Gather one [`SupersededGroup`] per input a compaction or rewrite record
/// names in its `inputs` list: the input's commit record, its data object, and
/// the level-0 snapshot identity both share. Shared by rule 2's compaction and
/// rewrite-RawL0 arms (ADR-0018, ADR-0064 decision 3 point 6): both name the
/// same raw-L0 input shape. The data key needs each input record's content
/// hash, so each input record is read before it is deleted; an input already
/// gone (a crash-interrupted prior pass) yields no group and its data object,
/// if any, is collected by orphan GC (row 8).
///
/// One group per input, not one for the whole record: an input the live HEAD
/// no longer names is still collectable in a pass where a sibling input is
/// held.
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
) -> Result<Vec<SupersededGroup>> {
    let mut groups: Vec<SupersededGroup> = Vec::new();
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
                let writer_id = Uuid::parse_str(&rec.writer_id).map_err(|_| {
                    MaintainError::Key(KeyError::InvalidWriterId(rec.writer_id.clone()))
                })?;
                groups.push(SupersededGroup {
                    ingest_hour_bucket: rec.ingest_hour_bucket,
                    identity: commit_key.clone(),
                    record_keys: vec![commit_key],
                    data_sizes: HashMap::from([(data_key.clone(), rec.object_size)]),
                    part_bytes: HashMap::new(),
                    data_keys: vec![data_key],
                    chain_record_keys: Vec::new(),
                    objects: vec![SnapshotObject::L0 {
                        shard: rec.shard,
                        ingest_hour_bucket: rec.ingest_hour_bucket,
                        writer_id: writer_id.into_bytes(),
                        writer_epoch: rec.writer_epoch,
                        writer_seq: rec.writer_seq,
                    }],
                    request_ids: BTreeSet::new(),
                    truncated: false,
                    absent_end: None,
                });
            }
            Err(StoreError::NotFound) => {}
            Err(e) => return Err(MaintainError::Store(e)),
        }
    }
    Ok(groups)
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
/// re-audit ([`crate::migrate::count_below_target`]), so the
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

impl SupersededInputs for SupersededSubset {
    fn inputs(&self) -> &[CompactionInputIdentity] {
        &self.inputs
    }
    fn ingest_hour_bucket(&self) -> u32 {
        self.ingest_hour_bucket
    }
}

/// One generation on a supersession chain: the compaction or rewrite record a
/// newer record superseded (ADR-0064 amendment: a rewrite's
/// `superseded_record_key` names either an `l1.<hash>.cmt` compaction record
/// or an `rw.<hash>.cmt` rewrite record, recursive supersession included; a
/// version 2 compaction record's names the compaction record it re-encodes).
enum ChainLink {
    Compaction(CompactionRecord),
    Rewrite(RewriteRecord),
}

impl ChainLink {
    fn ingest_hour_bucket(&self) -> u32 {
        match self {
            ChainLink::Compaction(r) => r.ingest_hour_bucket,
            ChainLink::Rewrite(r) => r.ingest_hour_bucket,
        }
    }

    /// Whether this generation superseded raw L0 inputs (the end of the
    /// chain), rather than another compaction/rewrite record. A version 2
    /// compaction record is a link: it names the record it re-encodes.
    fn names_raw_l0_inputs(&self) -> bool {
        match self {
            ChainLink::Compaction(r) => r.superseded_record_key.is_empty(),
            ChainLink::Rewrite(r) => !r.inputs.is_empty(),
        }
    }

    /// The record this generation itself superseded, or `None` at the end of
    /// the chain.
    fn superseded_record_key(&self) -> Option<&str> {
        let key = match self {
            ChainLink::Compaction(r) => &r.superseded_record_key,
            ChainLink::Rewrite(r) => &r.superseded_record_key,
        };
        if key.is_empty() { None } else { Some(key) }
    }

    fn is_version_2_compaction(&self) -> bool {
        matches!(self, ChainLink::Compaction(r) if !r.superseded_record_key.is_empty())
    }

    /// The erasure request ids this generation applied (empty for a compaction
    /// record, which applies none).
    fn applied_request_ids(&self) -> Vec<&str> {
        match self {
            ChainLink::Compaction(_) => Vec::new(),
            ChainLink::Rewrite(r) => r.drops.iter().map(|d| d.request_id.as_str()).collect(),
        }
    }

    /// This generation's output L1 part keys, their snapshot identities, and
    /// each part's `object_size` as the record carries it.
    fn part_targets(&self) -> Result<Vec<(String, SnapshotObject, u64)>> {
        match self {
            ChainLink::Compaction(record) => {
                let input_set_hash = input_set_hash_array(&record.input_set_hash)?;
                record
                    .parts
                    .iter()
                    .map(|part| {
                        Ok((
                            keys::reconstruct_l1_part_key(record, part)?,
                            SnapshotObject::L1 {
                                shard: record.shard,
                                ingest_hour_bucket: record.ingest_hour_bucket,
                                input_set_hash,
                                part_index: part.part_index,
                            },
                            part.object_size,
                        ))
                    })
                    .collect()
            }
            ChainLink::Rewrite(record) => {
                let input_set_hash = input_set_hash_array(&record.input_set_hash)?;
                record
                    .parts
                    .iter()
                    .map(|part| {
                        Ok((
                            keys::reconstruct_rewrite_part_key(record, part)?,
                            SnapshotObject::L1 {
                                shard: record.shard,
                                ingest_hour_bucket: record.ingest_hour_bucket,
                                input_set_hash,
                                part_index: part.part_index,
                            },
                            part.object_size,
                        ))
                    })
                    .collect()
            }
        }
    }

    /// The raw-L0 input groups this generation superseded, one per input still
    /// present in the store. Empty unless [`Self::names_raw_l0_inputs`].
    async fn raw_l0_input_groups(
        &self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
    ) -> Result<Vec<SupersededGroup>> {
        match self {
            ChainLink::Compaction(r) => gather_l0_inputs(store, tenant, signal, shard, r).await,
            ChainLink::Rewrite(r) if !r.inputs.is_empty() => {
                gather_l0_inputs(store, tenant, signal, shard, r).await
            }
            ChainLink::Rewrite(_) => Ok(Vec::new()),
        }
    }
}

/// GET, decode, and key-verify the compaction or rewrite record at `key`.
/// `Ok(None)` means the chain is truncated there: the record was swept by a
/// crash-interrupted prior pass, or by an older sweep that did not yet hold a
/// predecessor for its inputs.
async fn load_chain_link(store: &dyn ObjectStoreBackend, key: &str) -> Result<Option<ChainLink>> {
    match keys::partition_bucket_entry(key) {
        Ok(BucketEntry::CompactionRecord(_)) => Ok(get_compaction_record_opt(store, key)
            .await?
            .map(ChainLink::Compaction)),
        Ok(BucketEntry::RewriteRecord(_)) => Ok(get_rewrite_record_opt(store, key)
            .await?
            .map(ChainLink::Rewrite)),
        Ok(BucketEntry::CommitRecord(_) | BucketEntry::Tombstone(_)) => {
            Err(MaintainError::Invariant(format!(
                "rewrite superseded_record_key {key} names a non-compaction, non-rewrite entry"
            )))
        }
        Err(KeyError::UnknownBucketEntryShape(k)) => Err(MaintainError::UnknownBucketEntry(k)),
        Err(e) => Err(MaintainError::Key(e)),
    }
}

/// The most records one supersession chain walk charges, counting the record
/// it is entered from, which is the bound the catalog puts on the same chains.
/// The charge follows the catalog's walks exactly (see
/// [`ChainEntry::charges`]), so a chain is refused here exactly when the
/// catalog refuses it: a stricter bound would hold, and never reclaim, a chain
/// every resolve accepts.
const MAX_CHAIN_DEPTH: usize = 64;

/// Which record a [`gather_superseded_chain`] walk starts from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainEntry {
    /// A rewrite record that superseded a whole record. The group runs down to
    /// the raw L0 inputs at the end of the chain, since the rewrite supersedes
    /// them too.
    Rewrite,
    /// A version 2 compaction record. The group is the records it supersedes
    /// and their parts, and never a raw L0 input: those are the version 2
    /// record's own inputs, which rule 2's compaction arm gates as it gates any
    /// record's.
    Version2,
}

impl ChainEntry {
    fn gathers_raw_l0_inputs(self) -> bool {
        matches!(self, ChainEntry::Rewrite)
    }

    /// Whether a present `link` on this walk counts toward [`MAX_CHAIN_DEPTH`].
    /// An absent record never does. The rewrite chase gives a version 1
    /// compaction record no iteration of its own, since it ends the chase in
    /// the iteration of the record naming it; the version 2 chain walk gives
    /// every present record one, the version 1 record at its end included.
    fn charges(self, link: &ChainLink) -> bool {
        match self {
            ChainEntry::Rewrite => {
                !matches!(link, ChainLink::Compaction(r) if r.superseded_record_key.is_empty())
            }
            ChainEntry::Version2 => true,
        }
    }
}

/// Whether a [`gather_superseded_chain`] walk may step past a version 2
/// compaction record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Version2Links {
    Follow,
    /// The bucket's version 2 supersession does not resolve, so which records
    /// a version 2 record on the chain supersedes is not known: a walk that
    /// meets one gathers nothing, and the caller reports the chain as held in
    /// a truncated bucket. A group is indivisible, so the records above the
    /// link cannot go without it either.
    Refuse,
}

/// What a [`gather_superseded_chain`] walk found.
enum ChainWalk {
    /// At most one group: none when the record the walk was entered at is
    /// absent.
    Gathered(Vec<SupersededGroup>),
    /// The walk gathered nothing and the chain is reported held in a truncated
    /// bucket. Every reason is one the catalog's resolve over the same bucket
    /// also fails on, so the chain is unservable either way, and refusing it
    /// keeps the pass reclaiming and observing the shard's other buckets.
    Refused(ChainRefusal),
}

/// Why a [`gather_superseded_chain`] walk refused its chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainRefusal {
    /// A version 2 record under [`Version2Links::Refuse`].
    Version2Link,
    /// A chain charging more than [`MAX_CHAIN_DEPTH`] records.
    TooDeep,
    /// A chain revisiting a record.
    Cycle,
}

impl ChainRefusal {
    fn reason(self) -> &'static str {
        match self {
            ChainRefusal::Version2Link => {
                "the chain reaches a version 2 record in a bucket whose version 2 supersession \
                 does not resolve"
            }
            ChainRefusal::TooDeep => "the chain is longer than the depth bound",
            ChainRefusal::Cycle => "the chain revisits a record",
        }
    }
}

/// Whether a chain walk that ends at the absent record `key` is cut: the
/// requests that generation applied are named by no surviving record. An absent
/// compaction record applied none, so only another kind of key, or one that
/// does not parse, cuts the chain.
fn absent_link_cuts_chain(key: &str) -> bool {
    !matches!(
        keys::partition_bucket_entry(key),
        Ok(BucketEntry::CompactionRecord(_))
    )
}

/// Gather the deletion targets for a rewrite record that superseded a whole
/// prior compaction/rewrite record: the entire supersession chain behind
/// `predecessor_key`, walked back generation by generation to the raw L0 inputs
/// the oldest generation superseded. Returns at most one group holding, in
/// deletion order, those inputs' commit records, their data objects together
/// with every generation's L1 parts, and last the generations' own records
/// oldest first.
///
/// One group, not one per generation or per part, for two reasons. Deleting a
/// record while a still-named part of its own is held would leave that part
/// unreferenced and rule 3 would collect it on the next pass, undoing the hold.
/// And gating a generation on its own outputs alone is not enough: a HEAD that
/// has not been re-folded since the rewrite still names the raw L0 inputs at
/// the end of the chain, not any generation's parts, so a per-generation gate
/// clears while the inputs stay resolvable. The whole chain shares one gate, so
/// a HEAD naming anything in it holds all of it.
///
/// A chain ending at an absent record yields whatever it reached before the
/// gap. When the absent record is not a compaction record's
/// ([`absent_link_cuts_chain`]) the group is flagged
/// [`SupersededGroup::truncated`], so a hold on it can be reported per bucket
/// rather than per request. An absent `predecessor_key` yields no group at
/// all, and any surviving parts below it are unreferenced under the live
/// record and collected by rule 3. The absent record's key is kept as
/// [`SupersededGroup::absent_end`].
///
/// [`ChainWalk::Refused`] means the walk met a version 2 record under
/// [`Version2Links::Refuse`], a chain past [`MAX_CHAIN_DEPTH`], or a revisited
/// record, and gathered nothing.
///
/// The group also carries every erasure request the generations it covers
/// applied. Those requests' `.dreq`s cannot be retired while the group is
/// held, because the objects in it are the pre-image the requests erased a
/// subject out of.
///
/// A version 2 compaction record on the chain is a link, not an end: the walk
/// continues to the record it re-encodes. If that record is already gone, the
/// version 2 record is the end of the chain, and under
/// [`ChainEntry::Rewrite`] its own raw L0 inputs (its predecessor's, verbatim)
/// join the group; the missing generation is a compaction record, which
/// applied no request, so the chain is not truncated. Under
/// [`ChainEntry::Version2`] no raw L0 input joins the group at all. The walk is
/// bounded by [`MAX_CHAIN_DEPTH`], charged as the catalog charges the same
/// chain ([`ChainEntry::charges`]), and checked for a revisit, so a cycle or an
/// over-deep chain is refused, never a guess.
async fn gather_superseded_chain(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    predecessor_key: &str,
    entry: ChainEntry,
    links: Version2Links,
) -> Result<ChainWalk> {
    walk_superseded_chain(
        store,
        tenant,
        signal,
        shard,
        predecessor_key,
        entry,
        links,
        ChainLoader::Store,
    )
    .await
}

/// Where a [`walk_superseded_chain`] reads the chain's records from.
///
/// An enum rather than a generic async loader: a closure over the borrowed
/// store makes the sweep future's `Send` bound higher-ranked, which the
/// server's `tokio::spawn` of the maintain loop cannot prove.
enum ChainLoader<'a> {
    /// [`load_chain_link`] against the walk's store.
    Store,
    /// Rewrite records served from memory without key verification.
    #[cfg_attr(not(test), allow(dead_code))]
    Memory(&'a HashMap<&'a str, RewriteRecord>),
}

impl ChainLoader<'_> {
    async fn load(&self, store: &dyn ObjectStoreBackend, key: &str) -> Result<Option<ChainLink>> {
        match self {
            ChainLoader::Store => load_chain_link(store, key).await,
            ChainLoader::Memory(records) => Ok(records.get(key).cloned().map(ChainLink::Rewrite)),
        }
    }
}

/// [`gather_superseded_chain`] with the chain's records read through `load`.
/// Content-addressed record keys make a stored chain that revisits a record
/// unconstructible, so only a loader that skips key verification can feed
/// the walk one.
#[allow(clippy::too_many_arguments)]
async fn walk_superseded_chain(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    predecessor_key: &str,
    entry: ChainEntry,
    links: Version2Links,
    load: ChainLoader<'_>,
) -> Result<ChainWalk> {
    let mut chain_record_keys: Vec<String> = Vec::new();
    let mut chain_part_keys: Vec<String> = Vec::new();
    let mut input_record_keys: Vec<String> = Vec::new();
    let mut input_data_keys: Vec<String> = Vec::new();
    let mut data_sizes: HashMap<String, u64> = HashMap::new();
    let mut part_bytes: HashMap<String, u64> = HashMap::new();
    let mut objects: Vec<SnapshotObject> = Vec::new();
    let mut ingest_hour_bucket: Option<u32> = None;
    let mut request_ids: BTreeSet<String> = BTreeSet::new();
    let mut truncated = false;
    let mut absent_end: Option<String> = None;
    let mut seen: HashSet<String> = HashSet::new();
    // The record the chain is entered from is charged, though it is not on
    // this walk.
    let mut depth = 1usize;
    let mut cursor = Some(predecessor_key.to_string());
    // The version 2 record the walk just stepped past, whose raw L0 inputs
    // end the chain if the record it names is gone.
    let mut version_2_above: Option<ChainLink> = None;

    while let Some(key) = cursor {
        if !seen.insert(key.clone()) {
            return Ok(ChainWalk::Refused(ChainRefusal::Cycle));
        }
        let Some(link) = load.load(store, &key).await? else {
            match version_2_above.take() {
                Some(above) => {
                    if entry.gathers_raw_l0_inputs() {
                        for group in above
                            .raw_l0_input_groups(store, tenant, signal, shard)
                            .await?
                        {
                            input_record_keys.extend(group.record_keys);
                            input_data_keys.extend(group.data_keys);
                            data_sizes.extend(group.data_sizes);
                            objects.extend(group.objects);
                        }
                    }
                }
                None => truncated = absent_link_cuts_chain(&key),
            }
            absent_end = Some(key);
            break;
        };
        if links == Version2Links::Refuse && link.is_version_2_compaction() {
            return Ok(ChainWalk::Refused(ChainRefusal::Version2Link));
        }
        if entry.charges(&link) {
            if depth >= MAX_CHAIN_DEPTH {
                return Ok(ChainWalk::Refused(ChainRefusal::TooDeep));
            }
            depth += 1;
        }
        if entry == ChainEntry::Version2 && !matches!(link, ChainLink::Compaction(_)) {
            return Err(MaintainError::Invariant(format!(
                "version 2 compaction supersession chain from {predecessor_key} reaches \
                 rewrite record {key}"
            )));
        }
        // The gate reads one hour's covering snapshot parts, so a chain that
        // spanned two ingest hours could not be gated as one unit. A rewrite
        // record's decode already verifies that its `superseded_record_key`
        // sits in its own bucket, so this is a redundant local check on a
        // durable invariant rather than a new rule.
        match ingest_hour_bucket {
            None => ingest_hour_bucket = Some(link.ingest_hour_bucket()),
            Some(hour) if hour == link.ingest_hour_bucket() => {}
            Some(hour) => {
                return Err(MaintainError::Invariant(format!(
                    "supersession chain from {predecessor_key} spans ingest hours {hour} and {}",
                    link.ingest_hour_bucket()
                )));
            }
        }
        for id in link.applied_request_ids() {
            if !id.is_empty() {
                request_ids.insert(canonical_request_id(id));
            }
        }
        let mut record_part_bytes = 0u64;
        for (part_key, object, size) in link.part_targets()? {
            record_part_bytes = record_part_bytes.saturating_add(size);
            chain_part_keys.push(part_key);
            objects.push(object);
        }
        part_bytes.insert(key.clone(), record_part_bytes);
        chain_record_keys.push(key);
        if link.names_raw_l0_inputs() {
            if entry.gathers_raw_l0_inputs() {
                for group in link
                    .raw_l0_input_groups(store, tenant, signal, shard)
                    .await?
                {
                    input_record_keys.extend(group.record_keys);
                    input_data_keys.extend(group.data_keys);
                    data_sizes.extend(group.data_sizes);
                    objects.extend(group.objects);
                }
            }
            break;
        }
        cursor = link.superseded_record_key().map(str::to_string);
        version_2_above = link.is_version_2_compaction().then_some(link);
    }

    let Some(ingest_hour_bucket) = ingest_hour_bucket else {
        return Ok(ChainWalk::Gathered(Vec::new()));
    };
    // Oldest generation first: a record is deleted only after every object the
    // generations below it superseded, and after the generation it superseded.
    chain_record_keys.reverse();
    input_data_keys.extend(chain_part_keys);
    // The oldest generation's record is the group's identity: it is what every
    // live rewrite over this same predecessor walks down to.
    let identity = chain_record_keys
        .first()
        .cloned()
        .unwrap_or_else(|| predecessor_key.to_string());
    Ok(ChainWalk::Gathered(vec![SupersededGroup {
        ingest_hour_bucket,
        record_keys: input_record_keys,
        data_keys: input_data_keys,
        data_sizes,
        part_bytes,
        chain_record_keys,
        objects,
        request_ids,
        truncated,
        absent_end,
        identity,
    }]))
}

/// A record's `input_set_hash` as the 32-byte array a level-1 snapshot entry
/// carries in its `writer_id` slot. A wrong length is the same fatal invariant
/// breach `reconstruct_l1_part_key` reports for it.
fn input_set_hash_array(bytes: &[u8]) -> Result<[u8; 32]> {
    bytes.try_into().map_err(|_| {
        MaintainError::Invariant(format!(
            "superseded record input_set_hash is {} bytes, expected 32",
            bytes.len()
        ))
    })
}

/// [`get_compaction_record`] tolerant of a NotFound (Ok(None)): the record was
/// swept between this pass's LIST and now (e.g. a superseding rewrite processed
/// earlier in the same pass removed it, or a crash-interrupted prior pass).
async fn get_compaction_record_opt(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<CompactionRecord>> {
    Ok(get_compaction_record_versioned(store, key)
        .await?
        .map(|(record, _)| record))
}

/// [`get_compaction_record_opt`] plus the store version of the bytes read.
async fn get_compaction_record_versioned(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<(CompactionRecord, String)>> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            let record = record::decode_compaction(got.data.as_ref()).map_err(|e| {
                MaintainError::Invariant(format!("compaction record decode failed: {e}"))
            })?;
            keys::verify_compaction_record_key(&record, key)?;
            Ok(Some((record, got.version.0)))
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
    Ok(get_rewrite_record_versioned(store, key)
        .await?
        .map(|(record, _)| record))
}

/// [`get_rewrite_record_opt`] plus the store version of the bytes read.
async fn get_rewrite_record_versioned(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<(RewriteRecord, String)>> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            let record = ravel_commit::erasure::decode_rewrite(got.data.as_ref()).map_err(|e| {
                MaintainError::Invariant(format!("rewrite record decode failed: {e}"))
            })?;
            keys::verify_rewrite_record_key(&record, key)?;
            Ok(Some((record, got.version.0)))
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
/// holds a retention tombstone and no compaction record. A bucket
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
    Ok(
        sweep_unreferenced_parts_impl(store, clock, config, lease, tenant, signal, shard, None)
            .await?
            .0,
    )
}

/// Shared implementation behind [`sweep_unreferenced_parts`] (whole-shard,
/// `hours: None`) and [`sweep_shard_zoned`] (hour-scoped, `hours: Some(_)`).
/// When scoped, both the commit-prefix listing that builds the reference map
/// and the `l1/` listing are restricted to `hours`: an interior bucket this
/// tick's zone recomputation skipped is never listed by either.
///
/// Returns the count of deleted parts and their total bytes, summed from each
/// part's listed [`ObjectMeta::size`] (no extra request; the size comes from the
/// `l1/` LIST this pass already issues).
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
) -> Result<(usize, u64)> {
    let now = clock.now_ns();
    let gate = config.unreferenced_part_age_gate_ns();
    let (referenced, tombstoned) =
        bucket_reference_map_scoped(store, tenant, signal, shard, hours).await?;

    let objects = list_l1_scoped(store, tenant, signal, shard, hours).await?;
    let mut deleted = 0usize;
    let mut deleted_bytes = 0u64;
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
        deleted_bytes = deleted_bytes.saturating_add(meta.size);
    }
    Ok((deleted, deleted_bytes))
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
    /// legal future publish can name this object.
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
/// compaction may still publish a record naming them).
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

// --- Rule 5: unreferenced catalog-object sweep ----------

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
/// age exceeds `config.protection_horizon_ns`.
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
///   is HEAD's `parts[].key`, its optional `postings.key`, and each part's
///   optional per-part column-statistics key (`parts[].column_stats.key`,
///   field 7). An object under the two prefixes but not in that set is
///   superseded or orphaned. ADR-1413 decision 6 retired the whole-object
///   forms, so a legacy v1/v2 `.cstat` is named by no HEAD and is swept.
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
///   `protection_horizon_ns` term is `max_query_duration + grace`: it
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
///   [`LeaseCheck`] hook / future reader-lease work closes; it is
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
/// The production driver is `ravel-server`'s maintenance tick
/// (`services/ravel-server/src/maintain.rs`), which calls this once per
/// (tenant, signal) for every signal, gated on ownership of shard 0 of that
/// pair, alongside [`sweep_idempotency_markers`] and
/// [`sweep_erasure_requests`].
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
    /// `parts[].key`, the optional `postings.key`, and each part's optional
    /// per-part column-statistics key `parts[].column_stats.key`).
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
                HashSet::with_capacity(head.parts.len() * 2 + usize::from(head.postings.is_some()));
            for part in &head.parts {
                referenced.insert(part.key.clone());
                // Additive, ADR-1413. `part.column_stats` (field 7,
                // `SnapshotColumnStatsPartRef`) names a per-part v3
                // column-stats object living under the same `idx/` prefix
                // this sweep lists. It is reachable only through the part
                // that carries it, never through SnapshotHead field 11/13, so
                // it must be added to the set here or a live v3 object goes
                // unreferenced and is swept once its age crosses the
                // protection horizon, even though a sealed part never gets
                // rewritten and the HEAD still names it (issue #1482).
                if let Some(column_stats) = &part.column_stats {
                    referenced.insert(column_stats.key.clone());
                }
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
    /// still inside the post-completion protection horizon, lease/legal-hold
    /// protected, or held off this pass's own observing rule-2 pass
    /// (`held_by_superseded_inputs` below counts that subset).
    pub kept: usize,
    /// The subset of `kept` held past their horizon because this pass's
    /// observing rule-2 pass ([`SweepMode::GateOnly`], over every hour and
    /// chains of any age) held a chain group naming the request, or held a
    /// chain it could not walk to the end anywhere in the signal, which holds
    /// every candidate. Retiring the query-time exclusion filter while such a
    /// chain is held would let a snapshot that still resolves it serve the
    /// erased subject again. The server's maintain loop sums it into
    /// `ravel_maintain_dreq_held_by_superseded_inputs_total`.
    pub held_by_superseded_inputs: usize,
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
/// - rule 2 did not hold anything this request's rewrites superseded. The
///   horizon on its own does not imply that: rule 2 holds an input the live
///   HEAD snapshot still names, an input under a snapshot part it cannot read,
///   and a whole chain any legal hold over the shard's data prefixes touches
///   (such a hold does not cover `del/`, so it does not pin the `.dreq`
///   itself). In every one of those cases a snapshot can still resolve the
///   pre-rewrite object, so retiring the filter would serve the erased subject
///   again. The decision is read off the [`SupersededHolds`] of an observing
///   rule-2 pass this function runs itself ([`SweepMode::GateOnly`], over
///   every shard of the signal): `request_ids` when a held group names the
///   request, and `truncated_buckets` when a held group's chain could not be
///   walked to the end, in which case the requests the missing generation
///   applied are named by no surviving record and the bucket stands in for
///   them. The holds cover every supersession chain in the signal, not only
///   the ones old enough to delete: an object's age says nothing about whether
///   a snapshot still resolves it, and a chain still inside its own protection
///   horizon is the likeliest one a stale HEAD names. That is why no entry
///   takes a deleting pass's holds instead: a deleting pass walks no rewrite
///   still inside its horizon, nor any record below one.
/// - the [`LeaseCheck`] passes: a legal hold over the `del/` keyspace pins the
///   request exactly as it pins any other object.
///
/// A completion's `bucket_drops` is informational only. No part of this rule
/// reads it: not the hold decision, and not the scope of the observation. The
/// field is optional on the wire, is written
/// empty by the production writer, and a writer that does populate it is not
/// obliged to enumerate every bucket it touched, so a present list can be
/// partial. Any truncated bucket in the signal therefore holds any candidate.
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
/// durably complete. ([`sweep_unreferenced_catalog_objects`] runs at the same
/// granularity in that same tick; see its own doc.) A listing entry under
/// `del/` that is neither a `.dreq` nor a `.done` is layout drift and fails
/// the pass loud, matching the resolver's and the rewrite pass's fail-loud
/// discipline for this keyspace.
///
/// This is rule 6's only entry, and `ravel-server`'s maintenance tick calls
/// it. The observation costs one rule-2-shaped pass and nothing more, and runs
/// only when there is a `.dreq` past its horizon to decide about. An ordinary
/// pass, where every request is either incomplete or still inside its
/// horizon, reads nothing but the `del/` listing and the completions.
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

    // One LIST, split into pending requests and their completions. Both
    // suffixes share the `del/` prefix, so this needs no second listing.
    let mut dreq_keys: Vec<(Uuid, String)> = Vec::new();
    let mut completions: HashMap<Uuid, ErasureCompletion> = HashMap::new();
    for meta in &objects {
        if let Ok(parsed) = keys::parse_erasure_request_key(&meta.key) {
            dreq_keys.push((parsed.request_id, meta.key.clone()));
        } else if let Ok(parsed) = keys::parse_erasure_completion_key(&meta.key) {
            let got = store.get(&meta.key, GetRange::Full).await?;
            let completion = ravel_commit::erasure::decode_completion(&got.data).map_err(|e| {
                MaintainError::Invariant(format!("erasure completion decode failed: {e}"))
            })?;
            keys::verify_erasure_completion_key(&completion, &meta.key)?;
            completions.insert(parsed.request_id, completion);
        } else {
            return Err(MaintainError::UnknownBucketEntry(meta.key.clone()));
        }
    }

    // Split the requests into the ones this pass could delete and the ones a
    // cheaper condition already keeps, before any hold is observed: an
    // ordinary pass has no candidate and does no further work.
    let mut candidates: Vec<(&Uuid, &String)> = Vec::new();
    let mut kept = 0usize;
    for (request_id, dreq_key) in &dreq_keys {
        let Some(completion) = completions.get(request_id) else {
            // No `.done`: the erasure is not verified complete, so the request
            // (and its query-time exclusion filter) must stay live.
            kept += 1;
            continue;
        };
        let completed_ns = completion.completed_unix_ns;
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
        candidates.push((request_id, dreq_key));
    }

    if candidates.is_empty() {
        return Ok(ErasureRequestSweepOutcome {
            deleted: 0,
            kept,
            held_by_superseded_inputs: 0,
        });
    }

    let holds = observe_superseded_holds(store, clock, config, lease, tenant, signal).await?;

    let mut deleted = 0usize;
    let mut held_by_superseded_inputs = 0usize;
    for (request_id, dreq_key) in candidates {
        // The horizon has elapsed, but rule 2 may have held an object one of
        // this request's rewrites superseded: an input the live HEAD still
        // names, one under an unreadable snapshot part, or a chain a legal
        // hold over the data prefixes touches. A snapshot can still resolve
        // such an object, so the filter stays.
        // A held chain rule 2 could not walk to the end names requests no
        // surviving record does, so any such bucket stands in for them. The
        // completion's own `bucket_drops` are not consulted: the field is
        // optional on the wire, a production writer leaves it empty, and
        // nothing forces a writer that does fill it to enumerate every bucket
        // it touched. Narrowing on a list that may be partial would release a
        // filter over a bucket the request did touch.
        let request_id_s = request_id.to_string();
        let held = holds.request_ids.contains(&request_id_s) || !holds.truncated_buckets.is_empty();
        if held {
            tracing::warn!(
                tenant_hash = %tenant.to_hex(),
                signal = signal.key_prefix(),
                request_id = %request_id,
                held_requests = holds.request_ids.len(),
                held_truncated_buckets = holds.truncated_buckets.len(),
                "erasure-request sweep: holding a .dreq past its horizon because the \
                 superseded-input sweep held an object its rewrite superseded; the query-time \
                 exclusion filter must outlive it"
            );
            kept += 1;
            held_by_superseded_inputs += 1;
            continue;
        }
        if !config.dry_run {
            store.delete(dreq_key).await?;
        }
        deleted += 1;
    }

    Ok(ErasureRequestSweepOutcome {
        deleted,
        kept,
        held_by_superseded_inputs,
    })
}

/// Observe what rule 2 holds, without deleting anything, for rule 6.
///
/// The observation always covers the whole signal: the commit keyspace is
/// listed once to enumerate its shards, and every shard is observed across
/// every hour. It is never narrowed by a candidate completion's `bucket_drops`.
/// That field is optional on the wire and nothing makes a writer that fills it
/// enumerate every bucket it touched, so a partial list would silently exclude
/// the bucket whose chain holds the request. Shard enumeration is one LIST for
/// the pass, and a shard with no commit key holds nothing, so the whole-signal
/// scope costs listing, never correctness.
///
/// The pass runs only when there is a `.dreq` past its horizon to decide about.
async fn observe_superseded_holds(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<SupersededHolds> {
    // One pass, one reachability cache: HEAD is read at most once no matter how
    // many shards are observed.
    let mut reach = SnapshotReachability::new();
    let mut holds = SupersededHolds::default();

    for shard in signal_shards(store, tenant, signal).await? {
        let (outcome, _data_bytes) = sweep_superseded_impl(
            &mut reach,
            store,
            clock,
            config,
            lease,
            tenant,
            signal,
            shard,
            None,
            SweepMode::GateOnly,
        )
        .await?;
        holds.absorb(&outcome);
    }
    Ok(holds)
}

/// Every shard with at least one commit-prefix key for one `(tenant, signal)`,
/// from one LIST of `t/<tenant_hash_hex>/<signal>/c/`.
///
/// A shard with no commit key holds no supersession chain and so can hold
/// nothing, which is why key presence is the right enumeration and no shard
/// count needs to be configured or guessed.
async fn signal_shards(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Vec<u32>> {
    let prefix = format!("t/{}/{}/c/", tenant.to_hex(), signal.key_prefix());
    let mut shards: BTreeSet<u32> = BTreeSet::new();
    for meta in list_all(store, &prefix).await? {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(BucketEntry::CommitRecord(pk)) => {
                shards.insert(pk.shard);
            }
            Ok(BucketEntry::CompactionRecord(pk)) => {
                shards.insert(pk.shard);
            }
            Ok(BucketEntry::RewriteRecord(pk)) => {
                shards.insert(pk.shard);
            }
            Ok(BucketEntry::Tombstone(pk)) => {
                shards.insert(pk.shard);
            }
            Err(KeyError::UnknownBucketEntryShape(k)) => {
                return Err(MaintainError::UnknownBucketEntry(k));
            }
            Err(e) => return Err(MaintainError::Key(e)),
        }
    }
    Ok(shards.into_iter().collect())
}

// --- shared helpers --------------------------------------------------------

/// List a shard's commit prefix and classify every key by shape, failing loud
/// on any unknown shape. Returns `(key, entry)` pairs.
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
    let record = record::decode_compaction(got.data.as_ref())
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

/// The form a request id takes in the hold set and in the `.dreq` check: the
/// hyphenated UUID text, which is what a parsed `.dreq` key renders. A rewrite
/// record carries the id as the string its writer supplied, and a UUID has
/// more than one accepted text form, so both sides are normalised through the
/// parser before they are compared. A string that is not a UUID is kept as
/// written: normalising it would only make an unrelated pair compare equal.
fn canonical_request_id(id: &str) -> String {
    match Uuid::parse_str(id) {
        Ok(uuid) => uuid.hyphenated().to_string(),
        Err(_) => id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    /// A drop that names its request in the simple UUID form still matches
    /// the hyphenated form a `.dreq` key renders; a non-UUID id is kept as
    /// written. Without the normalisation the first assertion fails: the two
    /// strings differ and the hold set would miss the request.
    #[test]
    fn request_ids_compare_in_the_hyphenated_form() {
        let hyphenated = "6f1c9a2e-0d3b-4b5a-9e7f-1c2d3e4f5a6b";
        let simple = "6f1c9a2e0d3b4b5a9e7f1c2d3e4f5a6b";
        assert_eq!(super::canonical_request_id(simple), hyphenated);
        assert_eq!(super::canonical_request_id(hyphenated), hyphenated);
        assert_eq!(
            super::canonical_request_id(&simple.to_uppercase()),
            hyphenated,
            "case is normalised too"
        );
        assert_eq!(super::canonical_request_id("not-a-uuid"), "not-a-uuid");
    }

    /// Rule 2's phase C tolerates a refusal of one object and fails the pass
    /// on anything else: a retryable error, a read-only store, and a backend
    /// with no delete support. A pass whose every attempted delete was refused
    /// fails too, which the superseding_compaction_record tests cover.
    #[test]
    fn only_a_per_object_refusal_is_tolerated() {
        use ravel_object_store::StoreError;
        for refused in [
            StoreError::AccessDenied("denied".into()),
            StoreError::PreconditionFailed,
            StoreError::Permanent("denied".into()),
        ] {
            assert!(super::delete_refused(&refused), "{refused:?}");
        }
        for fatal in [
            StoreError::Timeout,
            StoreError::Throttled { retry_after_ms: 1 },
            StoreError::Transient("reset".into()),
            StoreError::NotFound,
            StoreError::ReadOnly {
                operation: "delete".into(),
                store: "external".into(),
            },
            StoreError::Unsupported {
                operation: "delete".into(),
            },
        ] {
            assert!(!super::delete_refused(&fatal), "{fatal:?}");
        }
    }

    use bytes::Bytes;
    use ravel_ingest::{IdempotencyReceipt, LookupOutcome, marker_key, read_marker, write_marker};
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault, Sequence,
        SequenceStep,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_proto::catalog::v1::{
        SnapshotColumnStatsPartRef, SnapshotHead, SnapshotPartRef, SnapshotPostingsRef,
    };
    use ravel_proto::commit::v1::CompactionPart;
    use ravel_types::TenantId;

    use super::*;
    use crate::clock::FixedClock;

    fn tenant() -> TenantHash {
        TenantHash([0u8; 16])
    }

    /// deploy/iam/maintain.json's `quarantine/t/*/*/l0/*` and
    /// `quarantine/t/*/a/` grants depend on this key shape;
    /// `QUARANTINE_PREFIX` and `quarantine_key` in
    /// crates/ravel-commit/tests/iam_templates.rs copy it by hand.
    #[test]
    fn quarantine_key_matches_the_iam_template_witness() {
        assert_eq!(QUARANTINE_PREFIX, "quarantine/");
        assert_eq!(
            quarantine_key("t/abab/a/l0/0/obj", 1),
            "quarantine/t/abab/a/l0/0/obj/q00000000000000000001"
        );
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

    /// Issue #1734's acceptance claim at the `SweepReport` level: an
    /// `OrphanPass::Skip` pass reports fresh zeros for every rule-1 figure,
    /// never a value carried over from a prior `Run` pass, while a `Run` pass
    /// over the same live candidates reports exactly the seeded count.
    ///
    /// The `FaultStore` sequence proves *which* passes actually issued the
    /// `l0/` data prefix LIST, rather than trusting the returned counts alone:
    /// registered with `Op::List` and `key_contains("/l0/")`, it also matches
    /// the quarantine reaper's LIST (`quarantine/` + the `l0/` prefix is a
    /// superstring of it, so no substring pattern can separate the two), so
    /// each pass's progress is candidate-selection-lists-or-not plus the
    /// reaper's LIST when it runs: 2 on the `Run` pass below, then a delta of
    /// 0 on the `Skip` pass, which lists neither prefix (the reaper runs on
    /// candidate selection's cadence, see `OrphanPass::Skip`).
    #[tokio::test]
    async fn skip_pass_reports_zero_orphan_figures_while_run_pass_reports_the_seeded_count() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 9;
        const SEEDED_ORPHANS: u64 = 3;

        let mem = MemoryStore::new();
        for seq in 0..SEEDED_ORPHANS {
            put_orphan(&mem, &tenant, signal, shard, seq).await;
        }
        let config = CompactorConfig::default();
        let clock = aged_clock(&config);
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::List)
                .with_key_contains("/l0/")
                .with_steps(vec![SequenceStep::Passthrough; 8]),
        );
        let store = FaultStore::new(mem, plan);

        let (run_report, _holds) = sweep_shard_zoned_with_holds(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            signal,
            shard,
            &[],
            OrphanPass::Run,
        )
        .await
        .expect("run pass");
        assert_eq!(run_report.orphans_deleted, SEEDED_ORPHANS as usize);
        assert_eq!(run_report.orphans_quarantined, SEEDED_ORPHANS as usize);
        assert_eq!(run_report.orphans_quarantine_refused, 0);
        assert!(!run_report.orphan_breaker_tripped);
        assert_eq!(run_report.orphans_withheld, 0);
        assert!(!run_report.orphan_breaker_overridden);
        let progress_after_run = store.sequence_progress(0);
        assert_eq!(
            progress_after_run, 2,
            "Run pass lists the l0 data prefix for candidate selection, plus the \
             quarantine reaper's l0-embedding prefix, once each"
        );

        // Re-seed the same identities so the Skip pass below has live
        // candidates it must not touch: this is what makes the zero figures
        // asserted next mean "did not run", not "nothing there to find".
        for seq in 0..SEEDED_ORPHANS {
            put_orphan(&store, &tenant, signal, shard, seq).await;
        }
        let (skip_report, _holds) = sweep_shard_zoned_with_holds(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            signal,
            shard,
            &[],
            OrphanPass::Skip,
        )
        .await
        .expect("skip pass");
        assert_eq!(skip_report.orphans_deleted, 0);
        assert_eq!(skip_report.orphans_quarantined, 0);
        assert_eq!(skip_report.orphans_quarantine_refused, 0);
        assert!(!skip_report.orphan_breaker_tripped);
        assert_eq!(skip_report.orphans_withheld, 0);
        assert!(!skip_report.orphan_breaker_overridden);
        let progress_after_skip = store.sequence_progress(0);
        assert_eq!(
            progress_after_skip - progress_after_run,
            0,
            "Skip pass issues no l0-embedding LIST at all: no candidate \
             selection, and no reaper either, since the reaper runs on \
             candidate selection's cadence"
        );

        let remaining = list_all(&store, &l0_data_prefix(&tenant, signal, shard).unwrap())
            .await
            .unwrap();
        assert_eq!(
            remaining.len(),
            SEEDED_ORPHANS as usize,
            "the Skip pass left every live candidate untouched"
        );
    }

    // --- Quarantine (ADR-0058 amendment, issue #528) -----------------------

    /// The L0 data key `put_orphan` writes for `seq`, so a test can assert the
    /// exact key set that ended up quarantined.
    fn orphan_data_key(tenant: &TenantHash, signal: Signal, shard: u32, seq: u64) -> String {
        let writer_id = Uuid::from_u128(u128::from(seq) + 1);
        keys::data_key(tenant, signal, shard, writer_id, 1, seq, &[7u8; 32])
            .expect("valid data key")
    }

    /// A referenced (non-orphan) L0 data object: its data object plus a commit
    /// record at the matching identity, so it counts toward `l0_objects_listed`
    /// (the breaker ratio's denominator) but is never an orphan candidate.
    /// `referenced_l0_identities` reads commit-record KEYS only, so an empty
    /// object at a valid commit key is enough to mark the data object
    /// referenced.
    async fn put_referenced(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        seq: u64,
    ) {
        let writer_id = Uuid::from_u128(u128::from(seq) + 1_000_000);
        let data = keys::data_key(tenant, signal, shard, writer_id, 1, seq, &[9u8; 32])
            .expect("valid data key");
        let commit = keys::commit_key(tenant, signal, shard, 0, writer_id, 1, seq)
            .expect("valid commit key");
        store
            .put(&data, Bytes::new(), PutOptions::default())
            .await
            .expect("seed referenced data");
        store
            .put(&commit, Bytes::new(), PutOptions::default())
            .await
            .expect("seed commit record");
    }

    /// Recover the original object key from a quarantine key
    /// (`quarantine/<original>/q<ns>`): strip the prefix and the trailing
    /// `/q<ns>` segment. This is the operator's recovery path, exercised as an
    /// assertion.
    fn recover_original(quarantine_key: &str) -> String {
        let without_prefix = quarantine_key
            .strip_prefix(QUARANTINE_PREFIX)
            .expect("quarantine prefix present");
        let (original, last) = without_prefix
            .rsplit_once('/')
            .expect("trailing timestamp segment present");
        assert!(last.starts_with('q'), "trailing segment is the q<ns> stamp");
        original.to_string()
    }

    async fn keys_under(store: &dyn ObjectStoreBackend, prefix: &str) -> BTreeSet<String> {
        list_all(store, prefix)
            .await
            .expect("list")
            .into_iter()
            .map(|m| m.key)
            .collect()
    }

    /// The ticket's own case: fewer than `orphan_breaker_min_count` orphan
    /// candidates, so the breaker does NOT trip. Pre-fix this deleted the
    /// objects permanently; now they are recoverable from `quarantine/`, pinned
    /// by exact key set.
    #[tokio::test]
    async fn small_loss_below_breaker_is_quarantined_not_deleted() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 7;
        let store = MemoryStore::new();
        for seq in 0..3u64 {
            put_orphan(&store, &tenant, signal, shard, seq).await;
        }
        let config = CompactorConfig::default();
        let clock = aged_clock(&config);
        let now = clock.now_ns();

        let outcome = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("3 candidates is below orphan_breaker_min_count: no trip");
        assert_eq!(outcome.deleted, 3, "all three moved out of the live set");
        assert_eq!(outcome.refused, 0);

        // Nothing is left in the live L0 keyspace.
        assert!(
            keys_under(&store, &l0_data_prefix(&tenant, signal, shard).unwrap())
                .await
                .is_empty(),
            "orphans removed from the live keyspace"
        );

        // All three are recoverable from quarantine, by exact key set.
        let expected_originals: BTreeSet<String> = (0..3u64)
            .map(|seq| orphan_data_key(&tenant, signal, shard, seq))
            .collect();
        let quarantined = keys_under(
            &store,
            &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
        )
        .await;
        assert_eq!(quarantined.len(), 3, "exactly three quarantined");
        let recovered: BTreeSet<String> = quarantined.iter().map(|k| recover_original(k)).collect();
        assert_eq!(
            recovered, expected_originals,
            "the exact orphan keys are recoverable from quarantine"
        );
        // The embedded timestamp is the quarantine instant.
        for k in &quarantined {
            assert_eq!(parse_quarantine_timestamp(k), Some(now));
        }
    }

    /// The thin-spread case: orphan candidates under the ratio on a large
    /// shard, so neither the count nor the ratio condition trips. Same
    /// assertion: recoverable, not deleted.
    #[tokio::test]
    async fn thin_spread_loss_under_ratio_is_quarantined() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 8;
        let store = MemoryStore::new();
        // 5 orphans among 100 listed L0 objects = 5% < 10%, and 5 < 50, so the
        // breaker's ratio condition is what would otherwise matter and it does
        // not trip.
        for seq in 0..95u64 {
            put_referenced(&store, &tenant, signal, shard, seq).await;
        }
        for seq in 1000..1005u64 {
            put_orphan(&store, &tenant, signal, shard, seq).await;
        }
        let config = CompactorConfig::default();
        let clock = aged_clock(&config);

        let outcome = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("5/100 is under the ratio and under the count: no trip");
        assert_eq!(outcome.deleted, 5);
        assert_eq!(outcome.refused, 0);

        let expected_originals: BTreeSet<String> = (1000..1005u64)
            .map(|seq| orphan_data_key(&tenant, signal, shard, seq))
            .collect();
        let quarantined = keys_under(
            &store,
            &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
        )
        .await;
        let recovered: BTreeSet<String> = quarantined.iter().map(|k| recover_original(k)).collect();
        assert_eq!(
            recovered, expected_originals,
            "only the five orphans are quarantined; the 95 referenced objects are untouched"
        );
        // The referenced data objects stay live.
        assert_eq!(
            keys_under(&store, &l0_data_prefix(&tenant, signal, shard).unwrap())
                .await
                .len(),
            95,
            "referenced L0 data objects remain live"
        );
    }

    /// The dangerous half: fail the copy to quarantine and assert nothing was
    /// deleted and the refusal is counted. Copy-first/delete-second means a
    /// failed copy leaves the live object in place.
    #[tokio::test]
    async fn copy_failure_leaves_object_and_counts_refusal() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 9;
        let mem = MemoryStore::new();
        for seq in 0..3u64 {
            put_orphan(&mem, &tenant, signal, shard, seq).await;
        }
        // Every PUT under the quarantine prefix fails: the copy half never
        // completes, so the delete half must never run.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Timeout)
                .with_key_contains(QUARANTINE_PREFIX)
                .with_occurrence(Occurrence::Always),
        );
        let store = FaultStore::new(mem, plan);
        let config = CompactorConfig::default();
        let clock = aged_clock(&config);

        let outcome = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("a copy failure is per-object, not fatal to the pass");
        assert_eq!(outcome.deleted, 0, "nothing was moved out of the live set");
        assert_eq!(outcome.refused, 3, "all three refusals counted");
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Timeout),
            3,
            "the copy PUT faulted for each candidate",
        );

        // Every live object is still present; nothing reached quarantine.
        assert_eq!(
            keys_under(&store, &l0_data_prefix(&tenant, signal, shard).unwrap())
                .await
                .len(),
            3,
            "fail-closed: the live objects are untouched",
        );
        assert!(
            keys_under(
                &store,
                &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
            )
            .await
            .is_empty(),
            "no partial quarantine copy survived the failed put",
        );

        // The ADR-0058 decision-1 `orphans_present` gauge sums the sweep's
        // deleted and withheld counts, and now its refused count too. A
        // refused candidate is still present, and it is refused exactly in
        // the store-fault case the gauge exists for, so the pre-fix sum read
        // zero at the moment the signal mattered. `withheld` lives on the
        // whole-sweep report rather than this per-rule outcome, so the two
        // terms available here are the ones asserted.
        assert_eq!(
            outcome.deleted, 0,
            "the pre-fix sum contributed nothing for these three candidates",
        );
        assert_eq!(
            outcome.deleted + outcome.refused,
            3,
            "all three are still present and must reach the gauge",
        );
    }

    /// The reaper: an object past the second horizon is physically deleted; one
    /// inside it is not. Run under `with_page_size(2)` so listing pagination is
    /// exercised. Exact key sets.
    #[tokio::test]
    async fn reaper_deletes_past_horizon_keeps_within() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 5;
        let store = MemoryStore::with_page_size(2);
        let config = CompactorConfig::default();

        // Quarantine A at t1.
        put_orphan(&store, &tenant, signal, shard, 0).await;
        let t1 = config.orphan_age_gate_ns() + 1;
        let clock = FixedClock::new(t1);
        let out = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("quarantine A");
        assert_eq!(out.deleted, 1);

        // Quarantine B one whole horizon later, at t2 = t1 + horizon.
        put_orphan(&store, &tenant, signal, shard, 1).await;
        let t2 = t1 + config.quarantine_horizon_ns;
        clock.set(t2);
        let out = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("quarantine B");
        assert_eq!(out.deleted, 1);

        // Reap at t2 + 1: A (age horizon + 1) is past the horizon; B (age 1) is
        // not.
        clock.set(t2 + 1);
        let reaped = sweep_quarantine(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("reap");
        assert_eq!(reaped.reaped, 1, "only A, past the horizon");
        assert_eq!(reaped.retained, 1, "B is still inside the horizon");

        let key_b = orphan_data_key(&tenant, signal, shard, 1);
        let remaining = keys_under(
            &store,
            &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
        )
        .await;
        let recovered: BTreeSet<String> = remaining.iter().map(|k| recover_original(k)).collect();
        assert_eq!(
            recovered,
            BTreeSet::from([key_b]),
            "exactly B remains quarantined; A is physically gone"
        );
    }

    /// The reaper reports the exact bytes it reclaimed, summed from each reaped
    /// object's listed size, and counts only objects it actually deletes: an
    /// object still inside the horizon adds neither a count nor a byte (issue
    /// #1729). Flip-line proof: dropping `reaped_bytes += meta.size` leaves the
    /// byte total at `0` while the count still reads `1`.
    #[tokio::test]
    async fn reaper_reports_exact_reclaimed_bytes() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 7;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();

        // Seed one orphan with a known nonzero payload and quarantine it.
        const PAYLOAD_LEN: usize = 24;
        let writer_id = Uuid::from_u128(1);
        let content_hash = [7u8; 32];
        let key = keys::data_key(&tenant, signal, shard, writer_id, 1, 0, &content_hash)
            .expect("valid data key");
        store
            .put(
                &key,
                Bytes::from(vec![0u8; PAYLOAD_LEN]),
                PutOptions::default(),
            )
            .await
            .expect("seed orphan payload");
        let t1 = config.orphan_age_gate_ns() + 1;
        let clock = FixedClock::new(t1);
        let quarantined = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("quarantine");
        assert_eq!(quarantined.deleted, 1);

        // Seed a second orphan that will remain inside the horizon at reap time.
        put_orphan(&store, &tenant, signal, shard, 1).await;
        let t2 = t1 + config.quarantine_horizon_ns;
        clock.set(t2);
        sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("quarantine second");

        // Reap just past the first object's horizon: only the 24-byte object.
        clock.set(t2 + 1);
        let reaped = sweep_quarantine(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("reap");
        assert_eq!(reaped.reaped, 1, "only the object past the horizon");
        assert_eq!(reaped.retained, 1, "the second object is still inside it");
        assert_eq!(
            reaped.reaped_bytes, PAYLOAD_LEN as u64,
            "reaped_bytes is the reaped object's listed size, and excludes the retained one",
        );
    }

    /// A quarantine key whose timestamp segment is unparseable is never reaped
    /// (fail-closed): an unreadable age is treated as not-yet-expired.
    #[tokio::test]
    async fn reaper_never_deletes_malformed_key() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 6;
        let store = MemoryStore::new();
        // A quarantine-prefixed key with no `/q<ns>` stamp.
        let malformed = format!(
            "{}bogus-object",
            quarantine_l0_data_prefix(&tenant, signal, shard).unwrap()
        );
        store
            .put(&malformed, Bytes::new(), PutOptions::default())
            .await
            .expect("seed malformed");
        assert_eq!(parse_quarantine_timestamp(&malformed), None);

        let config = CompactorConfig::default();
        // Clock far past any horizon.
        let clock = FixedClock::new(config.quarantine_horizon_ns * 100);
        let reaped = sweep_quarantine(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("reap");
        assert_eq!(reaped.reaped, 0, "a malformed key is never reaped");
        assert_eq!(reaped.retained, 1);
    }

    /// A legal hold placed after an object was quarantined still stops the
    /// reap. Hold scopes are validated to start with `t/<tenant_hex>/` and
    /// `is_protected` is a prefix match, so the hold is asked about the
    /// recovered original key, not only about the `quarantine/...` key it
    /// could never match.
    ///
    /// Flip to watch it fail: drop the `original_key_from_quarantine` arm of
    /// the reaper's `held` check and the first assertion reaps 1.
    #[tokio::test]
    async fn a_hold_on_the_original_key_stops_the_quarantine_reap() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 12;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();

        put_orphan(&store, &tenant, signal, shard, 0).await;
        let t1 = config.orphan_age_gate_ns() + 1;
        let clock = FixedClock::new(t1);
        sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("quarantine the single candidate");

        // Past the second horizon, so only the hold can save it.
        clock.set(t1 + config.quarantine_horizon_ns + 1);

        // The hold names the live keyspace, which is the only shape a real
        // hold scope can take.
        let hold = HoldPrefix(l0_data_prefix(&tenant, signal, shard).unwrap());
        assert!(
            !hold.is_protected(
                &keys_under(
                    &store,
                    &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
                )
                .await
                .into_iter()
                .next()
                .expect("one quarantined object")
            ),
            "the hold cannot match the quarantine key itself; that is the point"
        );

        let out = sweep_quarantine(&store, &clock, &config, &hold, &tenant, signal, shard)
            .await
            .expect("reap under a hold");
        assert_eq!(out.reaped, 0, "a held original protects its copy");
        assert_eq!(out.retained, 1);

        // Same object, same clock, hold released: it is reaped.
        let out = sweep_quarantine(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("reap without a hold");
        assert_eq!(out.reaped, 1, "nothing else was keeping it");
    }

    /// A pass whose mass-orphan breaker trips reaps nothing, even quarantine
    /// that is past its own horizon. The two horizons are not independent:
    /// reaping while the breaker signals a live record loss destroys the copies
    /// taken before that loss grew, which is this mechanism's own failure mode
    /// one horizon later.
    ///
    /// Both directions are asserted in one test so the gate cannot be dropped
    /// silently. Flip to watch it fail: delete the `orphan_breaker_tripped`
    /// arm in `sweep_shard_with_holds` and the first assertion reaps 1.
    #[tokio::test]
    async fn a_tripped_breaker_holds_the_quarantine_reaper() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 11;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();

        // One object quarantined at t1, well below the breaker's thresholds.
        put_orphan(&store, &tenant, signal, shard, 0).await;
        let t1 = config.orphan_age_gate_ns() + 1;
        let clock = FixedClock::new(t1);
        let out = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("one candidate does not trip the breaker");
        assert_eq!(out.deleted, 1);

        // A whole horizon later that copy is reapable. The loss has also grown:
        // 60 record-less objects now trip the breaker on this pass.
        let t2 = t1 + config.quarantine_horizon_ns + 1;
        clock.set(t2);
        for seq in 1..61u64 {
            put_orphan(&store, &tenant, signal, shard, seq).await;
        }

        let report = sweep_shard(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("a tripped breaker is reported, not an error, at this layer");
        assert!(report.orphan_breaker_tripped, "60 of 60 trips the breaker");
        assert_eq!(
            report.quarantine_reaped, 0,
            "the reaper is held while the breaker signals a live loss"
        );
        let still_quarantined = keys_under(
            &store,
            &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
        )
        .await;
        assert_eq!(
            still_quarantined.len(),
            1,
            "the expired copy survives a tripped-breaker pass"
        );

        // Same store, same clock, same expired copy: once the mass loss is
        // resolved the breaker no longer trips and the reaper collects it.
        for seq in 1..61u64 {
            store
                .delete(&orphan_data_key(&tenant, signal, shard, seq))
                .await
                .expect("clear the mass loss");
        }
        let report = sweep_shard(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("sweep with the breaker no longer tripping");
        assert!(!report.orphan_breaker_tripped);
        assert_eq!(
            report.quarantine_reaped, 1,
            "the same expired copy is reaped once the breaker is quiet"
        );
        assert!(
            keys_under(
                &store,
                &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
            )
            .await
            .is_empty(),
            "nothing left in quarantine"
        );
    }

    /// A pass that did not run candidate selection never reaps quarantine,
    /// however expired the copies are. The breaker's hold on the reaper is the
    /// caller's condition alone, and a `Skip` pass reports not-tripped because
    /// it never evaluated the breaker, so a reaping `Skip` pass 300 s after a
    /// tripped full sweep destroys exactly the copies that trip held.
    ///
    /// The second half asserts the reaper's own horizon semantics are
    /// unchanged: the same store, clock, and copy are reaped by a `Run` pass.
    ///
    /// Flip to watch it fail: change the reaper's condition in
    /// `sweep_shard_zoned_with_holds` from
    /// `orphan_pass == OrphanPass::Run && !orphan_breaker_tripped` back to
    /// `!orphan_breaker_tripped` and the `Skip` pass reaps 1.
    #[tokio::test]
    async fn a_skipped_pass_does_not_reap_quarantine() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 13;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();

        // One object quarantined at t1, then a whole horizon passes: it is
        // reapable by any pass that gets as far as the reaper.
        put_orphan(&store, &tenant, signal, shard, 0).await;
        let t1 = config.orphan_age_gate_ns() + 1;
        let clock = FixedClock::new(t1);
        let out = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("quarantine the single candidate");
        assert_eq!(out.deleted, 1);
        clock.set(t1 + config.quarantine_horizon_ns + 1);

        let (skip_report, _holds) = sweep_shard_zoned_with_holds(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            signal,
            shard,
            &[],
            OrphanPass::Skip,
        )
        .await
        .expect("skipped pass");
        assert!(
            !skip_report.orphan_breaker_tripped,
            "a Skip pass reports not-tripped because it never evaluated the breaker"
        );
        assert_eq!(
            skip_report.quarantine_reaped, 0,
            "a pass that did not run candidate selection reaps nothing"
        );
        assert_eq!(
            keys_under(
                &store,
                &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
            )
            .await
            .len(),
            1,
            "the expired copy survives a skipped pass"
        );

        // Same store, same clock, same expired copy: a selecting pass reaps it,
        // so the horizon itself is unchanged.
        let (run_report, _holds) = sweep_shard_zoned_with_holds(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            signal,
            shard,
            &[],
            OrphanPass::Run,
        )
        .await
        .expect("selecting pass");
        assert_eq!(
            run_report.quarantine_reaped, 1,
            "the reaper's own horizon semantics are unchanged"
        );
        assert!(
            keys_under(
                &store,
                &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
            )
            .await
            .is_empty(),
            "nothing left in quarantine"
        );
    }

    /// The chained interlock across two consecutive passes at this layer: the
    /// selecting pass trips the breaker and holds the reaper, and the skipped
    /// pass that follows it must not reap what the trip held. The tick-path
    /// counterpart (`run_tick_with_clock` driving the same sequence) lives in
    /// `ravel-server`'s `maintain` tests.
    ///
    /// Flip to watch it fail: change the reaper's condition in
    /// `sweep_shard_zoned_with_holds` from
    /// `orphan_pass == OrphanPass::Run && !orphan_breaker_tripped` back to
    /// `!orphan_breaker_tripped` and the skipped pass reaps the copy the
    /// tripped pass just held.
    #[tokio::test]
    async fn a_skipped_pass_after_a_tripped_breaker_keeps_the_held_copies() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 14;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();

        // One object quarantined while the loss was still small.
        put_orphan(&store, &tenant, signal, shard, 0).await;
        let t1 = config.orphan_age_gate_ns() + 1;
        let clock = FixedClock::new(t1);
        let out = sweep_orphans(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("quarantine the single candidate");
        assert_eq!(out.deleted, 1);

        // A horizon later the copy is reapable and the loss has grown past the
        // breaker's thresholds.
        clock.set(t1 + config.quarantine_horizon_ns + 1);
        for seq in 1..61u64 {
            put_orphan(&store, &tenant, signal, shard, seq).await;
        }

        let report = sweep_shard(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect("the selecting pass reports the trip, it is not an error here");
        assert!(report.orphan_breaker_tripped, "60 of 60 trips the breaker");
        assert_eq!(report.quarantine_reaped, 0, "the trip holds the reaper");

        let (skip_report, _holds) = sweep_shard_zoned_with_holds(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            signal,
            shard,
            &[],
            OrphanPass::Skip,
        )
        .await
        .expect("the next tick's skipped pass");
        assert_eq!(
            skip_report.quarantine_reaped, 0,
            "the tick after a trip must not reap what the trip held"
        );
        assert_eq!(
            keys_under(
                &store,
                &quarantine_l0_data_prefix(&tenant, signal, shard).unwrap(),
            )
            .await
            .len(),
            1,
            "the only recovery copy survives both passes"
        );
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
        // tolerance (forward-skew tolerance): a marker
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

    // --- Rule 5: unreferenced catalog-object sweep -----------

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
            column_stats: None,
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

    /// Appends a length-delimited field to an already-encoded protobuf
    /// message. Fields 11 and 13 on `SnapshotHead` (ADR-1413 decision 6,
    /// #1600) are `reserved` and have no struct field to set, so this is the
    /// only way to construct a HEAD that still carries one, the way a HEAD
    /// folded before this change would. `SnapshotHead::decode` (a plain
    /// `prost::Message::decode`) skips a field number it does not recognize,
    /// so appending is equivalent to the field having been encoded in its
    /// original position.
    fn append_raw_field(
        mut message_bytes: Vec<u8>,
        field_number: u32,
        field_value: &impl prost::Message,
    ) -> Vec<u8> {
        prost::encoding::encode_key(
            field_number,
            prost::encoding::WireType::LengthDelimited,
            &mut message_bytes,
        );
        let payload = field_value.encode_to_vec();
        prost::encoding::encode_varint(payload.len() as u64, &mut message_bytes);
        message_bytes.extend_from_slice(&payload);
        message_bytes
    }

    /// Like [`put_head`] but also plants a stale whole-object column-stats ref
    /// on `field_number` (11 or 13, the retired `SnapshotColumnStatsRef`/
    /// `SnapshotColumnStatsPartRef` whole-tenant forms), the way a HEAD folded
    /// before ADR-1413 decision 6 would. Both fields are `reserved` now (no
    /// struct field to set), so the ref is planted with [`append_raw_field`]
    /// after `encode_head` produces well-formed bytes for the rest of the
    /// message. `part_blake3` mirrors the parts' hashes so a decoder that did
    /// still validate the retired field would find it internally consistent.
    async fn put_head_with_stale_whole_object_column_stats(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        parts: Vec<SnapshotPartRef>,
        field_number: u32,
        column_stats_key: &str,
    ) {
        let watermark_hour = parts.iter().map(|p| p.watermark_hour).max().unwrap_or(0);
        let part_blake3: Vec<Vec<u8>> = parts.iter().map(|p| p.blake3.clone()).collect();
        let head = SnapshotHead {
            format_version: 1,
            tenant_hash: tenant.0.to_vec(),
            signal: 0,
            shard_count: 1,
            watermark_hour,
            parts,
            folder_id: vec![0u8; 16],
            created_unix_ns: 0,
            postings: None,
            shard_generation_count: 1,
        };
        let bytes = ravel_catalog::encode_head(&head).expect("valid HEAD encodes");
        let stale_ref = SnapshotColumnStatsPartRef {
            key: column_stats_key.to_string(),
            blake3: [9u8; 32].to_vec(),
            size: 1,
            segment_count: 1,
            part_blake3,
        };
        let bytes = append_raw_field(bytes, field_number, &stale_ref);
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

    /// The acceptance test: a snapshot part named by the current HEAD is
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

    /// ADR-1413 decision 6 (#1600): a stale `.cstat` object named ONLY by the
    /// retired whole-tenant field 11 (`HEAD.column_stats`, `SnapshotColumnStatsRef`,
    /// ADR-0850) is no longer referenced at all -- `read_head_reference` never
    /// reads field 11 (it has no struct field to read; the proto field is
    /// `reserved`), so the object is swept once it crosses the protection
    /// horizon exactly like any other unreferenced `.cstat`, even though a
    /// pre-#1600 HEAD still carries the raw bytes naming it. The live part
    /// survives on its own account (`parts[].key`), unaffected by field 11.
    ///
    /// Prove-the-test: reintroducing a field-11 fallback read in
    /// `read_head_reference` makes this fail: `outcome.deleted` becomes 0 and
    /// the object added back to `referenced` before this fix at
    /// `crates/ravel-maintain/src/sweep.rs:2138` (there is now no code at all
    /// reading `head.column_stats`) would again be found there.
    #[tokio::test]
    async fn catalog_sweep_no_longer_spares_stale_field_eleven_column_stats() {
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
        // A `.cstat` only a stale field-11 ref names, and an unrelated stale
        // one no HEAD field names at all -- both are unreferenced now.
        let field_eleven_cstat = format!(
            "{}20260101T00.cccc.cstat",
            catalog_idx_prefix(&tenant, signal)
        );
        let stale_cstat = format!(
            "{}20251231T00.dddd.cstat",
            catalog_idx_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &part).await;
        put_catalog_object(&store, &field_eleven_cstat).await;
        put_catalog_object(&store, &stale_cstat).await;
        put_head_with_stale_whole_object_column_stats(
            &store,
            &tenant,
            signal,
            vec![part_ref(&part, part_blake3)],
            11,
            &field_eleven_cstat,
        )
        .await;

        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect("sweep must succeed");

        // Exact sets: both unreferenced `.cstat` objects are deleted; only the
        // part survives.
        assert_eq!(
            outcome.deleted, 2,
            "the field-11-only and the wholly-unreferenced column-stats objects"
        );
        assert_eq!(outcome.kept, 1, "the part alone");
        assert!(
            !present(&store, &field_eleven_cstat).await,
            "a retired field-11 ref no longer keeps its object alive"
        );
        assert!(
            present(&store, &part).await,
            "the HEAD-named part is spared"
        );
        assert!(
            !present(&store, &stale_cstat).await,
            "an old unreferenced column-stats object must be swept"
        );
    }

    /// The ADR-0942 part-hash-keyed carrier, field 13 (`SnapshotColumnStatsPartRef`):
    /// a stale `.cstat` named only there is equally unreferenced now, so the
    /// fix does not depend on which retired field a pre-#1600 fold had chosen
    /// to write.
    #[tokio::test]
    async fn catalog_sweep_no_longer_spares_stale_field_thirteen_column_stats() {
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
        let field_thirteen_cstat = format!(
            "{}20260101T00.eeee.cstat",
            catalog_idx_prefix(&tenant, signal)
        );
        put_catalog_object(&store, &part).await;
        put_catalog_object(&store, &field_thirteen_cstat).await;
        put_head_with_stale_whole_object_column_stats(
            &store,
            &tenant,
            signal,
            vec![part_ref(&part, part_blake3)],
            13,
            &field_thirteen_cstat,
        )
        .await;

        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect("sweep must succeed");

        assert_eq!(outcome.deleted, 1, "the field-13-only column-stats object");
        assert_eq!(outcome.kept, 1, "the part alone");
        assert!(
            !present(&store, &field_thirteen_cstat).await,
            "a retired field-13 ref no longer keeps its object alive"
        );
        assert!(
            present(&store, &part).await,
            "the HEAD-named part is spared"
        );
    }

    /// A part ref carrying a per-part v3 column-stats ref (field 7,
    /// `SnapshotColumnStatsPartRef`, ADR-1413).
    fn part_ref_with_v3_stats(
        key: &str,
        blake3: [u8; 32],
        min_hour: u32,
        watermark_hour: u32,
        v3_stats_key: &str,
    ) -> SnapshotPartRef {
        SnapshotPartRef {
            min_hour,
            watermark_hour,
            column_stats: Some(SnapshotColumnStatsPartRef {
                key: v3_stats_key.to_string(),
                blake3: [9u8; 32].to_vec(),
                size: 1,
                segment_count: 1,
                part_blake3: vec![blake3.to_vec()],
            }),
            ..part_ref(key, blake3)
        }
    }

    /// Issue #1482: a per-part v3 column-stats object (`SnapshotPartRef.
    /// column_stats`, field 7, ADR-1413) is reachable only through the part
    /// that carries it, never through `SnapshotHead.column_stats` (field 11)
    /// or `column_stats_part` (field 13). Before the fix, `read_head_reference`
    /// only walked those two HEAD-level fields, so a live v3 object crossed
    /// the protection horizon and was swept out from under a sealed part the
    /// current HEAD still names -- and, unlike a `.csnap` part, a sealed part
    /// is never rewritten, so the object was never recreated. Two parts each
    /// carry a field-7 ref; a third, unrelated `.cstat` is unreferenced. All
    /// three are older than the horizon. Only the unreferenced one is swept.
    #[tokio::test]
    async fn catalog_sweep_spares_referenced_v3_per_part_column_stats() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let store = MemoryStore::new();
        let config = CompactorConfig::default();
        let now_ns = config.protection_horizon_ns.saturating_mul(2);

        let part_a = format!(
            "{}20260101T00.aaaa.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        let part_b = format!(
            "{}20260102T00.bbbb.csnap",
            catalog_snap_prefix(&tenant, signal)
        );
        let v3_stats_a = format!(
            "{}20260101T00.aaaa.cstat",
            catalog_idx_prefix(&tenant, signal)
        );
        let v3_stats_b = format!(
            "{}20260102T00.bbbb.cstat",
            catalog_idx_prefix(&tenant, signal)
        );
        let unreferenced_cstat = format!(
            "{}20251231T00.zzzz.cstat",
            catalog_idx_prefix(&tenant, signal)
        );

        put_catalog_object(&store, &part_a).await;
        put_catalog_object(&store, &part_b).await;
        put_catalog_object(&store, &v3_stats_a).await;
        put_catalog_object(&store, &v3_stats_b).await;
        put_catalog_object(&store, &unreferenced_cstat).await;
        put_head(
            &store,
            &tenant,
            signal,
            vec![
                part_ref_with_v3_stats(&part_a, [1u8; 32], 0, 100, &v3_stats_a),
                part_ref_with_v3_stats(&part_b, [2u8; 32], 101, 200, &v3_stats_b),
            ],
            None,
        )
        .await;

        let clock = FixedClock::new(now_ns);
        let outcome =
            sweep_unreferenced_catalog_objects(&store, &clock, &config, &NoLeases, &tenant, signal)
                .await
                .expect("sweep must succeed");

        // Exact key set: only the unreferenced `.cstat` is deleted; both
        // parts and both field-7-referenced v3 objects survive. Without the
        // field-7 walk this assertion fails: `outcome.deleted == 3`,
        // with `v3_stats_a` and `v3_stats_b` both gone, because
        // `read_head_reference` never walked `part.column_stats` -- only the
        // line adding it to `referenced` inside the `for part in &head.parts`
        // loop distinguishes the two runs.
        assert_eq!(outcome.deleted, 1, "only the unreferenced v3 object");
        assert_eq!(outcome.kept, 4, "both parts and both referenced v3 objects");
        assert!(
            present(&store, &v3_stats_a).await,
            "a v3 object a live part still references (field 7) must survive (#1482)"
        );
        assert!(
            present(&store, &v3_stats_b).await,
            "a v3 object a live part still references (field 7) must survive (#1482)"
        );
        assert!(
            present(&store, &part_a).await,
            "the HEAD-named part a is spared"
        );
        assert!(
            present(&store, &part_b).await,
            "the HEAD-named part b is spared"
        );
        assert!(
            !present(&store, &unreferenced_cstat).await,
            "an old v3 object no part references must still be swept"
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

    /// With no HEAD present, rule 5 sweeps NOTHING, even for an
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

    /// An old, unreferenced object that a concurrent fold adopts
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

    /// The pre-delete re-verify GET of HEAD actually happens. Fault
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

    /// A compaction record stamped a future `format_version` in a bucket is
    /// refused by the superseded sweep's bucket read (ADR-0066 decision 2), not
    /// read as version 1: the pass fails before it deletes anything, so the
    /// record and every input it would have named survive. The record is
    /// otherwise fully self-consistent (its identity fields reconstruct its own
    /// key), so the version gate is the only thing that can reject it. Removing
    /// that gate makes this test fail: the record then decodes as version 1,
    /// the pass proceeds, and `sweep_superseded` returns `Ok`.
    #[tokio::test]
    async fn superseded_sweep_refuses_a_future_version_compaction_record() {
        let tenant = tenant();
        let signal = Signal::Metrics;
        let shard = 0;
        let store = MemoryStore::new();

        let mut record = CompactionRecord {
            format_version: 3,
            tenant_hash: tenant.0.to_vec(),
            signal: ravel_commit::signal::to_proto(signal).into(),
            shard,
            ingest_hour_bucket: 1,
            input_set_hash: vec![0x22; 32],
            ..Default::default()
        };
        record.parts.clear();
        let key = keys::compaction_record_key_for(&record).expect("key");
        store
            .put(
                &key,
                record::encode_compaction(&record),
                PutOptions::default(),
            )
            .await
            .expect("seed put");

        let config = CompactorConfig::default();
        let clock = FixedClock::new(config.orphan_age_gate_ns() + 1);
        let err = sweep_superseded(&store, &clock, &config, &NoLeases, &tenant, signal, shard)
            .await
            .expect_err("a version-3 compaction record must fail the pass, not read as v1");
        match &err {
            MaintainError::Invariant(msg) => assert!(
                msg.contains("format_version") && msg.contains("3"),
                "the failure names the version gate and the version seen: {msg}"
            ),
            other => panic!("expected Invariant from the version gate, got {other:?}"),
        }
        assert!(
            present(&store, &key).await,
            "a failed superseded pass deletes nothing from the bucket"
        );
    }

    /// What a depth-boundary chain ends in.
    #[derive(Debug, Clone, Copy)]
    enum ChainEnd {
        PresentVersion1,
        Absent,
    }

    const DEPTH_SHARD: u32 = 0;

    /// The version 1 record at the bottom of a depth-boundary chain, put and
    /// added to `present` unless `end` is [`ChainEnd::Absent`]. Returns its key
    /// and the record.
    async fn put_chain_bottom(
        store: &MemoryStore,
        end: ChainEnd,
        present: &mut Vec<(String, CompactionRecord)>,
    ) -> (String, CompactionRecord) {
        let record = CompactionRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Logs).into(),
            shard: DEPTH_SHARD,
            ingest_hour_bucket: 1,
            level: 1,
            inputs: vec![CompactionInputIdentity {
                writer_id: Uuid::from_u128(1).to_string(),
                writer_epoch: 1,
                writer_seq: 1,
            }],
            input_set_hash: vec![0x11; 32],
            ..Default::default()
        };
        let key = keys::compaction_record_key_for(&record).expect("key");
        if matches!(end, ChainEnd::PresentVersion1) {
            store
                .put(
                    &key,
                    record::encode_compaction(&record),
                    PutOptions::default(),
                )
                .await
                .expect("seed put");
            present.push((key.clone(), record.clone()));
        }
        (key, record)
    }

    /// `rewrites` rewrite records, each naming the one below, over a chain
    /// bottom. Returns the rewrites bottom first and the compaction records
    /// present.
    async fn put_rewrite_chain(
        store: &MemoryStore,
        rewrites: usize,
        end: ChainEnd,
    ) -> (
        Vec<(String, RewriteRecord)>,
        Vec<(String, CompactionRecord)>,
    ) {
        let mut compactions = Vec::new();
        let (below, _) = put_chain_bottom(store, end, &mut compactions).await;
        (put_rewrites_over(store, below, rewrites).await, compactions)
    }

    /// `rewrites` rewrite records, each naming the one below, the bottom one
    /// naming `below`. Returns them bottom first.
    async fn put_rewrites_over(
        store: &MemoryStore,
        mut below: String,
        rewrites: usize,
    ) -> Vec<(String, RewriteRecord)> {
        let mut out = Vec::with_capacity(rewrites);
        for i in 0..rewrites {
            let request_id = Uuid::from_u128(i as u128 + 1).to_string();
            let record = RewriteRecord {
                format_version: 1,
                tenant_hash: tenant().0.to_vec(),
                signal: ravel_commit::signal::to_proto(Signal::Logs).into(),
                shard: DEPTH_SHARD,
                ingest_hour_bucket: 1,
                inputs: Vec::new(),
                input_set_hash: ravel_commit::erasure::compute_rewrite_input_set_hash(
                    &[],
                    Some(&below),
                    std::slice::from_ref(&request_id),
                )
                .to_vec(),
                parts: Vec::new(),
                drops: vec![ravel_proto::commit::v1::RewriteDrop {
                    request_id,
                    dropped_count: 1,
                }],
                created_unix_ns: 0,
                superseded_record_key: below.clone(),
            };
            let key = keys::rewrite_record_key_for(&record).expect("key");
            store
                .put(
                    &key,
                    ravel_commit::erasure::encode_rewrite(&record),
                    PutOptions::default(),
                )
                .await
                .expect("seed put");
            below = key.clone();
            out.push((key, record));
        }
        out
    }

    /// `version_2s` version 2 records, each naming the one below, over a
    /// chain bottom. Returns every present compaction record, bottom first.
    async fn put_version_2_chain(
        store: &MemoryStore,
        version_2s: usize,
        end: ChainEnd,
    ) -> Vec<(String, CompactionRecord)> {
        let mut out = Vec::new();
        let (mut below, bottom) = put_chain_bottom(store, end, &mut out).await;
        for _ in 0..version_2s {
            let record = CompactionRecord {
                format_version: 2,
                input_set_hash:
                    ravel_commit::erasure::compute_superseding_compaction_input_set_hash(
                        &bottom.inputs,
                        &below,
                    )
                    .to_vec(),
                superseded_record_key: below.clone(),
                ..bottom.clone()
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
            below = key.clone();
            out.push((key, record));
        }
        out
    }

    /// A dominated record no gathered group holds is counted, once, unless a
    /// rewrite in its bucket was left for its horizon; one a group holds is
    /// not. Flipped line: the `attached.contains(key.as_str())` skip in
    /// `Version2Groups::count_unattached`; without it the held record counts.
    #[test]
    fn unattached_dominated_records_are_counted() {
        let groups = Version2Groups {
            dominated: HashMap::from([
                (1, vec!["held".to_string(), "orphan".to_string()]),
                (2, vec!["waiting".to_string()]),
            ]),
            ..Default::default()
        };
        let group = SupersededGroup {
            chain_record_keys: vec!["held".to_string()],
            ..SupersededGroup::over_absent_predecessor(1, "absent")
        };
        let young = HashSet::from([2]);
        let count = groups.count_unattached(&[group], &young, &tenant(), Signal::Logs, DEPTH_SHARD);
        assert_eq!(count, 1);
        let count = groups.count_unattached(&[], &HashSet::new(), &tenant(), Signal::Logs, 0);
        assert_eq!(count, 3);
    }

    /// Recorded sizes of the two L1 parts [`put_sized_part_chain`] writes.
    /// Distinct, so charging one part's size for both cannot pass.
    const PART_SIZES: [u64; 2] = [1_111, 20_202];

    /// A version 1 compaction record whose two L1 parts carry [`PART_SIZES`],
    /// an object at each part key, and one rewrite over the record. Returns
    /// the compaction record's key and the part keys.
    async fn put_sized_part_chain(store: &MemoryStore) -> (String, Vec<String>) {
        let record = CompactionRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Logs).into(),
            shard: DEPTH_SHARD,
            ingest_hour_bucket: 1,
            level: 1,
            inputs: vec![CompactionInputIdentity {
                writer_id: Uuid::from_u128(1).to_string(),
                writer_epoch: 1,
                writer_seq: 1,
            }],
            input_set_hash: vec![0x11; 32],
            parts: PART_SIZES
                .iter()
                .zip(0u8..)
                .map(|(&object_size, index)| CompactionPart {
                    part_index: u32::from(index),
                    content_hash: vec![0x40 + index; 32],
                    object_size,
                    ..Default::default()
                })
                .collect(),
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
        let mut part_keys = Vec::new();
        for part in &record.parts {
            let part_key = keys::reconstruct_l1_part_key(&record, part).expect("part key");
            store
                .put(
                    &part_key,
                    Bytes::from_static(b"part"),
                    PutOptions::default(),
                )
                .await
                .expect("seed part");
            part_keys.push(part_key);
        }
        put_rewrites_over(store, key.clone(), 1).await;
        (key, part_keys)
    }

    /// One deleting rule 2 pass over [`put_sized_part_chain`]'s shard, past the
    /// rewrite's protection horizon, returning the outcome and the bytes it
    /// charged.
    async fn sized_chain_pass(
        store: &dyn ObjectStoreBackend,
    ) -> Result<(SupersededSweepOutcome, u64)> {
        let config = CompactorConfig::default();
        let clock = FixedClock::new(config.protection_horizon_ns + 1);
        sweep_superseded_impl(
            &mut SnapshotReachability::new(),
            store,
            &clock,
            &config,
            &NoLeases,
            &tenant(),
            Signal::Logs,
            DEPTH_SHARD,
            None,
            SweepMode::Delete,
        )
        .await
    }

    /// Issue #2073: a superseded L1 part is charged at the `object_size` its
    /// record carries, part by part. Flipped line: `part.object_size` in
    /// `ChainLink::part_targets` replaced with `0`; "each part at its recorded
    /// size" reads left 0, right 21313.
    #[tokio::test]
    async fn superseded_parts_are_charged_at_their_recorded_sizes() {
        let store = MemoryStore::new();
        let (record_key, part_keys) = put_sized_part_chain(&store).await;

        let (outcome, bytes) = sized_chain_pass(&store).await.expect("pass");

        assert_eq!(outcome.data_deleted, 2);
        assert_eq!(outcome.records_deleted, 1);
        assert_eq!(bytes, 21_313, "each part at its recorded size");
        for key in part_keys.iter().chain([&record_key]) {
            assert!(!present(&store, key).await, "{key} deleted");
        }
    }

    /// Issue #2073: a pass that deletes a chain's parts but is refused the
    /// record naming them charges nothing for those parts; the next pass
    /// rebuilds the part keys from the surviving record, deletes them again
    /// (a missing key is a successful delete) and deletes the record, and it
    /// is the one that charges them. Across both passes each part is charged
    /// exactly once. Flipped lines: the part charge moved back to each part's
    /// own delete (each part's size inserted into `data_sizes` in
    /// `walk_superseded_chain`, and the `part_bytes` charge on the chain-record
    /// delete removed); "a refused record charges none of its parts" reads
    /// left 21313, right 0.
    #[tokio::test]
    async fn a_refused_chain_record_delete_charges_its_parts_once() {
        let inner = MemoryStore::new();
        let (record_key, part_keys) = put_sized_part_chain(&inner).await;
        let store = FaultStore::new(
            inner,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Delete, ScriptedFault::Permanent("denied".into()))
                    .with_key_contains(record_key.clone())
                    .with_occurrence(Occurrence::Nth(1)),
            ),
        );

        let (first, first_bytes) = sized_chain_pass(&store).await.expect("pass 1");
        assert_eq!(
            store.fault_count(Op::Delete, FaultKind::Permanent),
            1,
            "the record's delete was refused"
        );
        assert_eq!(first.deletes_refused, 1);
        assert_eq!(first.data_deleted, 2, "pass 1 deleted both parts");
        assert_eq!(first.records_deleted, 0);
        assert_eq!(first_bytes, 0, "a refused record charges none of its parts");
        assert!(present(&store, &record_key).await);
        for key in &part_keys {
            assert!(!present(&store, key).await, "{key} deleted by pass 1");
        }

        let (second, second_bytes) = sized_chain_pass(&store).await.expect("pass 2");
        assert_eq!(store.fault_count(Op::Delete, FaultKind::Permanent), 1);
        assert_eq!(second.deletes_refused, 0);
        assert_eq!(second.data_deleted, 2, "pass 2 deleted the parts again");
        assert_eq!(second.records_deleted, 1);
        assert_eq!(
            second_bytes, 21_313,
            "the record's delete charges its parts"
        );
        assert!(!present(&store, &record_key).await);
        assert_eq!(first_bytes + second_bytes, PART_SIZES.iter().sum::<u64>());
    }

    /// Two parts per record, with sizes distinct across both records, so a
    /// total missing either record's parts cannot pass.
    fn sized_parts(sizes: [u64; 2]) -> Vec<CompactionPart> {
        sizes
            .iter()
            .zip(0u8..)
            .map(|(&object_size, index)| CompactionPart {
                part_index: u32::from(index),
                content_hash: vec![0x60 + index; 32],
                object_size,
                ..Default::default()
            })
            .collect()
    }

    /// Issue #2073 review: a superseded rewrite record's parts and an
    /// erasure-dominated version 2 compaction record's parts are each charged
    /// at their recorded sizes. A version 1 record C1 holds no parts; R1, a
    /// rewrite over C1 with parts of 300 and 4,000 bytes, is superseded by R2;
    /// C2, a version 2 record over C1 with parts of 50,000 and 600,000 bytes,
    /// is dominated by R2's chain and joins its group. Flipped lines:
    /// `part.object_size` in the `ChainLink::Rewrite` arm of
    /// `ChainLink::part_targets` replaced with `0` reads left 650000, right
    /// 654300; `record_part_bytes` in `Version2Groups::join_dominated`'s
    /// `part_bytes.insert` replaced with `0` reads left 4300, right 654300.
    #[tokio::test]
    async fn superseded_rewrite_and_dominated_version_2_parts_are_charged_at_their_sizes() {
        let store = MemoryStore::new();
        let (c1_key, c1) =
            put_chain_bottom(&store, ChainEnd::PresentVersion1, &mut Vec::new()).await;
        let mut part_keys = Vec::new();

        let request_id = Uuid::from_u128(0x51).to_string();
        let r1 = RewriteRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Logs).into(),
            shard: DEPTH_SHARD,
            ingest_hour_bucket: 1,
            inputs: Vec::new(),
            input_set_hash: ravel_commit::erasure::compute_rewrite_input_set_hash(
                &[],
                Some(&c1_key),
                std::slice::from_ref(&request_id),
            )
            .to_vec(),
            parts: sized_parts([300, 4_000]),
            drops: vec![ravel_proto::commit::v1::RewriteDrop {
                request_id,
                dropped_count: 1,
            }],
            created_unix_ns: 0,
            superseded_record_key: c1_key.clone(),
        };
        let r1_key = keys::rewrite_record_key_for(&r1).expect("key");
        store
            .put(
                &r1_key,
                ravel_commit::erasure::encode_rewrite(&r1),
                PutOptions::default(),
            )
            .await
            .expect("seed rewrite");
        for part in &r1.parts {
            part_keys.push(keys::reconstruct_rewrite_part_key(&r1, part).expect("part key"));
        }
        put_rewrites_over(&store, r1_key.clone(), 1).await;

        let c2 = CompactionRecord {
            format_version: 2,
            input_set_hash: ravel_commit::erasure::compute_superseding_compaction_input_set_hash(
                &c1.inputs, &c1_key,
            )
            .to_vec(),
            superseded_record_key: c1_key.clone(),
            parts: sized_parts([50_000, 600_000]),
            ..c1.clone()
        };
        let c2_key = keys::compaction_record_key_for(&c2).expect("key");
        store
            .put(
                &c2_key,
                record::encode_compaction(&c2),
                PutOptions::default(),
            )
            .await
            .expect("seed version 2");
        for part in &c2.parts {
            part_keys.push(keys::reconstruct_l1_part_key(&c2, part).expect("part key"));
        }
        for key in &part_keys {
            store
                .put(key, Bytes::from_static(b"part"), PutOptions::default())
                .await
                .expect("seed part");
        }

        let (outcome, bytes) = sized_chain_pass(&store).await.expect("pass");

        assert_eq!(outcome.data_deleted, 4, "R1's and C2's parts");
        assert_eq!(outcome.records_deleted, 3, "C1, R1 and C2");
        assert_eq!(bytes, 654_300, "each part at its recorded size");
        for key in part_keys.iter().chain([&c1_key, &r1_key, &c2_key]) {
            assert!(!present(&store, key).await, "{key} deleted");
        }
    }

    fn assert_refused_as_too_deep(result: Result<ChainWalk>, case: &str) {
        match result {
            Ok(ChainWalk::Refused(reason)) => {
                assert_eq!(reason, ChainRefusal::TooDeep, "{case}")
            }
            Ok(ChainWalk::Gathered(_)) => {
                panic!("{case}: expected the depth refusal, the walk was accepted")
            }
            Err(error) => panic!("{case}: expected the depth refusal, got {error:?}"),
        }
    }

    fn followed(result: Result<ChainWalk>, case: &str) -> Vec<SupersededGroup> {
        match result {
            Ok(ChainWalk::Gathered(groups)) => groups,
            Ok(ChainWalk::Refused(reason)) => panic!("{case}: refused as {reason:?}"),
            Err(error) => panic!("{case}: {error:?}"),
        }
    }

    /// The chain walk's depth bound accepts and refuses exactly the chains the
    /// catalog's walks do, at the last accepted depth and one past it: a
    /// rewrite chain as `resolve_rewrite_supersession` charges it (the
    /// entered-from rewrite and every rewrite below it; neither the version 1
    /// record ending it nor an absent record), and a version 2 chain as the
    /// selector charges it (the head and every present record below it, the
    /// version 1 record included; not an absent one), and a rewrite chain over
    /// a version 2 link over a version 1 record as the rewrite chase charges it
    /// (the version 2 link included). Each case asks the catalog too, so the
    /// expectation is the catalog's answer and not only this test's reading of
    /// it.
    ///
    /// Flipped line: the depth check in `gather_superseded_chain` restored to
    /// `if seen.len() >= MAX_CHAIN_DEPTH` before the record is loaded. The
    /// rewrite chain ending in a version 1 record is then refused at its last
    /// accepted depth, as is each chain ending in an absent record. For the
    /// mixed chain: the `ChainEntry::Rewrite` arm of `ChainEntry::charges`
    /// charging no compaction record at all, which accepts 64 rewrites over the
    /// version 2 record.
    #[tokio::test]
    async fn chain_walk_depth_bound_matches_the_catalog_exactly() {
        for end in [ChainEnd::PresentVersion1, ChainEnd::Absent] {
            // R plus 63 rewrites plus the end: 64 rewrites charged.
            for (rewrites, accepted) in [(MAX_CHAIN_DEPTH, true), (MAX_CHAIN_DEPTH + 1, false)] {
                let case = format!("{rewrites} rewrites over {end:?}");
                let store = MemoryStore::new();
                let (chain, compactions) = put_rewrite_chain(&store, rewrites, end).await;
                let (top_key, top) = chain.last().expect("a rewrite");
                let compaction_by_key: HashMap<&str, &CompactionRecord> =
                    compactions.iter().map(|(k, r)| (k.as_str(), r)).collect();
                let rewrite_by_key: HashMap<&str, &RewriteRecord> =
                    chain.iter().map(|(k, r)| (k.as_str(), r)).collect();
                let catalog = ravel_catalog::resolve_rewrite_supersession(
                    top_key,
                    top,
                    "bucket",
                    &compaction_by_key,
                    &rewrite_by_key,
                    &mut HashSet::new(),
                    &mut HashSet::new(),
                );
                assert_eq!(catalog.is_ok(), accepted, "catalog, {case}: {catalog:?}");
                let walked = gather_superseded_chain(
                    &store,
                    &tenant(),
                    Signal::Logs,
                    DEPTH_SHARD,
                    &top.superseded_record_key,
                    ChainEntry::Rewrite,
                    Version2Links::Follow,
                )
                .await;
                if accepted {
                    let groups = followed(walked, &case);
                    assert_eq!(groups.len(), 1, "{case}");
                    let expected = rewrites - 1 + compactions.len();
                    assert_eq!(groups[0].chain_record_keys.len(), expected, "{case}");
                } else {
                    assert_refused_as_too_deep(walked, &case);
                }
            }

            // The head plus 63 present records below it, with an absent
            // record past them or not.
            let last_accepted = match end {
                ChainEnd::PresentVersion1 => MAX_CHAIN_DEPTH - 1,
                ChainEnd::Absent => MAX_CHAIN_DEPTH,
            };
            for (version_2s, accepted) in [(last_accepted, true), (last_accepted + 1, false)] {
                let case = format!("{version_2s} version 2 records over {end:?}");
                let store = MemoryStore::new();
                let records = put_version_2_chain(&store, version_2s, end).await;
                let catalog = select_authoritative_compaction_records(&records);
                assert_eq!(
                    catalog.is_ok(),
                    accepted,
                    "catalog, {case}: {:?}",
                    catalog.err()
                );
                let (_, head) = records.last().expect("a version 2 record");
                let walked = gather_superseded_chain(
                    &store,
                    &tenant(),
                    Signal::Logs,
                    DEPTH_SHARD,
                    &head.superseded_record_key,
                    ChainEntry::Version2,
                    Version2Links::Follow,
                )
                .await;
                if accepted {
                    let groups = followed(walked, &case);
                    assert_eq!(groups.len(), 1, "{case}");
                    assert_eq!(
                        groups[0].chain_record_keys.len(),
                        records.len() - 1,
                        "{case}"
                    );
                } else {
                    assert_refused_as_too_deep(walked, &case);
                }
            }
        }

        // Rewrites over a version 2 record over a version 1 record: the
        // rewrite chase charges the entered-from rewrite, every rewrite below
        // it and the version 2 link, and not the version 1 record.
        for (rewrites, accepted) in [(MAX_CHAIN_DEPTH - 1, true), (MAX_CHAIN_DEPTH, false)] {
            let case = format!("{rewrites} rewrites over a version 2 record");
            let store = MemoryStore::new();
            let compactions = put_version_2_chain(&store, 1, ChainEnd::PresentVersion1).await;
            let (version_2_key, _) = compactions.last().expect("the version 2 record");
            let chain = put_rewrites_over(&store, version_2_key.clone(), rewrites).await;
            let (top_key, top) = chain.last().expect("a rewrite");
            let compaction_by_key: HashMap<&str, &CompactionRecord> =
                compactions.iter().map(|(k, r)| (k.as_str(), r)).collect();
            let rewrite_by_key: HashMap<&str, &RewriteRecord> =
                chain.iter().map(|(k, r)| (k.as_str(), r)).collect();
            let catalog = ravel_catalog::resolve_rewrite_supersession(
                top_key,
                top,
                "bucket",
                &compaction_by_key,
                &rewrite_by_key,
                &mut HashSet::new(),
                &mut HashSet::new(),
            );
            assert_eq!(catalog.is_ok(), accepted, "catalog, {case}: {catalog:?}");
            let walked = gather_superseded_chain(
                &store,
                &tenant(),
                Signal::Logs,
                DEPTH_SHARD,
                &top.superseded_record_key,
                ChainEntry::Rewrite,
                Version2Links::Follow,
            )
            .await;
            if accepted {
                let groups = followed(walked, &case);
                assert_eq!(groups.len(), 1, "{case}");
                assert_eq!(groups[0].chain_record_keys.len(), rewrites + 1, "{case}");
            } else {
                assert_refused_as_too_deep(walked, &case);
            }
        }
    }

    /// A chain that revisits a record is refused as a cycle and gathers
    /// nothing, whether it loops back through another record or names itself,
    /// while the same loader over the same records without the loop gathers
    /// the chain. The records are served unverified, since content-addressed
    /// keys make a stored cycle unconstructible.
    ///
    /// Flipped line: the `ChainRefusal::Cycle` return in
    /// `walk_superseded_chain` turned into an `Err(MaintainError::Invariant)`;
    /// both cyclic cases then fail.
    #[tokio::test]
    async fn chain_walk_refuses_a_chain_that_revisits_a_record() {
        fn rewrite(request: u128, superseded: &str) -> RewriteRecord {
            RewriteRecord {
                format_version: 1,
                tenant_hash: tenant().0.to_vec(),
                signal: ravel_commit::signal::to_proto(Signal::Logs).into(),
                shard: DEPTH_SHARD,
                ingest_hour_bucket: 1,
                input_set_hash: vec![request as u8; 32],
                drops: vec![ravel_proto::commit::v1::RewriteDrop {
                    request_id: Uuid::from_u128(request).to_string(),
                    dropped_count: 1,
                }],
                superseded_record_key: superseded.to_string(),
                ..Default::default()
            }
        }
        let cases = [
            (
                "a loop through another record",
                vec![("a", rewrite(1, "b")), ("b", rewrite(2, "a"))],
                Some(ChainRefusal::Cycle),
            ),
            (
                "a record naming itself",
                vec![("a", rewrite(1, "a"))],
                Some(ChainRefusal::Cycle),
            ),
            (
                "the same records without the loop",
                vec![("a", rewrite(1, "b")), ("b", rewrite(2, ""))],
                None,
            ),
        ];
        let store = MemoryStore::new();
        for (case, chain, refusal) in cases {
            let records: HashMap<&str, RewriteRecord> = chain.into_iter().collect();
            let walked = walk_superseded_chain(
                &store,
                &tenant(),
                Signal::Logs,
                DEPTH_SHARD,
                "a",
                ChainEntry::Rewrite,
                Version2Links::Follow,
                ChainLoader::Memory(&records),
            )
            .await;
            match (walked, refusal) {
                (Ok(ChainWalk::Refused(reason)), Some(expected)) => {
                    assert_eq!(reason, expected, "{case}")
                }
                (Ok(ChainWalk::Gathered(groups)), None) => {
                    assert_eq!(groups.len(), 1, "{case}");
                    assert_eq!(groups[0].chain_record_keys, ["b", "a"], "{case}");
                    assert_eq!(groups[0].request_ids.len(), 2, "{case}");
                }
                (Ok(ChainWalk::Refused(reason)), None) => panic!("{case}: refused as {reason:?}"),
                (Ok(ChainWalk::Gathered(_)), Some(_)) => {
                    panic!("{case}: expected the cycle refusal, the walk was accepted")
                }
                (Err(error), _) => panic!("{case}: {error:?}"),
            }
        }
    }
}
