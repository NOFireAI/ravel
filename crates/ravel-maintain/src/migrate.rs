//! Format-floor migration driver.
//!
//! This is the resumable driver that walks one `(tenant, signal, format
//! family)`, rewrites every live record still below a target format version up
//! to it via the rewrite primitive ([`crate::rewrite::migrate_bucket_format`]),
//! and -- only once a fresh re-audit confirms nothing below the target survives
//! -- raises the recorded format floor
//! ([`ravel_catalog::raise_format_floor`]).
//!
//! It reimplements neither the codec-level rewrite (that is
//! [`migrate_bucket_format`], called per bucket) nor the floor history (that is
//! the append-only CAS machinery, called once at the end). It contributes
//! three things on top of them:
//!
//! 1. **A resumable cursor.** Progress is a `(shard, ingest_hour)` position
//!    persisted in object storage under the advisory-CAS cursor pattern
//!    [`crate::scan`] established (ADR-0018 §Decision 7): losing or corrupting
//!    it costs a rescan, never correctness. The cursor lets a huge migration
//!    run across many bounded invocations, each resuming exactly where the last
//!    stopped, never reprocessing an already-migrated bucket (the pre-migration
//!    per-bucket raw-served check makes reprocessing a no-op even if the cursor
//!    is lost: the migration's own record names every input it rewrote) and
//!    never skipping one (the walk order is total and the
//!    cursor only ever moves forward). The cursor exists only to carry an
//!    unfinished walk across invocations, so a walk that drains clears it: it
//!    is a position in one walk, not a permanent high-water mark, and a
//!    surviving one would make every bucket of every lower shard unreachable
//!    to the next invocation.
//!
//! 2. **A budget.** One invocation migrates at most [`MigrateBudget::max_records`]
//!    L0 records before persisting the cursor and returning control, so a very
//!    large migration never holds a lock or a long-lived process. The unit is
//!    records rather than wall time because the rewrite primitive is
//!    bucket-atomic (it rewrites a whole bucket's live L0 set in one publish and
//!    cannot be interrupted partway); a record count is therefore both the
//!    natural granularity at which the driver can yield (a bucket boundary) and
//!    the only one that is deterministic under the injected [`Clock`] every
//!    test in this crate relies on.
//!
//! 3. **Verify-then-raise, closing the landing race.** After the walk drains
//!    with budget to spare, the driver re-runs the audit enumeration *fresh*
//!    (not from any cached walk state; [`count_below_target`]) and raises the
//!    floor only if that re-audit finds zero records below the target. If even
//!    one straggler is found -- data that landed below the target between the
//!    walk finishing and the floor being raised -- the floor is left untouched
//!    and the driver reports the stragglers, so a floor is never CAS-appended
//!    over a stale audit.
//!
//! The floor is a claim about *every* live object of the family, so the
//! re-audit deliberately counts every live commit and compaction record
//! regardless of seal state: a fresh, still-unsealed below-target record the
//! walk could not yet migrate still blocks the raise, which is exactly the
//! guarantee the floor must carry. "Live" excludes an L0 commit record that an
//! AUTHORITATIVE compaction record, or a rewrite record, names in its input
//! list ([`count_below_target`], keyed on the same predicate sweep rule 2
//! deletes by): those are sweepable leftovers of a rewrite this same walk may
//! just have performed, not stragglers, so migrating a bucket and then
//! re-auditing it in the same invocation converges without needing an
//! interleaved `sweep` in between. The exclusion asks the input-set question
//! directly rather than "does this record's bucket carry a compaction record",
//! so the re-audit confirms coverage instead of assuming the seal invariant
//! that makes the two equivalent.
//!
//! Authority is what makes that exclusion sound when two compaction records
//! over one bucket overlap: they resolve to one winner per overlap component
//! ([`ravel_catalog::select_authoritative_compaction_records`], the rule the
//! resolver, the index fold, the sweep and the erasure completion gate share),
//! the losers' parts are served from nowhere, and an input only a loser names
//! is served raw by `Catalog::resolve`. Both the walk's eligibility check and
//! the re-audit ask the question through that rule, so such an input is
//! visited by one and counted by the other, rather than being invisible to
//! both -- which is the false durable claim the floor must never make.

use std::collections::{BTreeMap, HashMap, HashSet};

use ravel_catalog::{
    current_floor_from_store, erasure_dominated_compaction_records, resolve_rewrite_supersession,
    select_authoritative_compaction_records,
};
use ravel_commit::{keys, record};
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, UploadChecksum, Version,
    list_all,
};
use ravel_proto::commit::v1::{CompactionRecord, RewriteRecord};
use ravel_types::{Signal, TenantHash};

use crate::bucket::Bucket;
use crate::claim_guard::{Checkpoint, ClaimSkipReason};
use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::publish::PublishOutcome;
use crate::read::{BucketListing, list_bucket, load_inputs};
use crate::rewrite::{
    MigrateOutcome, ReencodeOutcome, current_part_version, migrate_bucket_format,
    reencode_compaction_parts,
};
use crate::sweep::superseded_input_commit_keys;

/// One-byte version tag on the advisory migrate-cursor payload. As with
/// [`crate::scan`]'s compaction cursor, this is not a frozen format: the tag
/// only lets a future encoding change be recognized and treated as "no usable
/// cursor" (rescan from the start), never silently misread.
const MIGRATE_CURSOR_TAG: u8 = 1;

/// Length of a well-formed cursor payload: tag + shard (u32 LE) + hour (u32 LE).
const MIGRATE_CURSOR_LEN: usize = 9;

/// The per-invocation work budget for [`migrate_family`], counted in L0 records:
/// those an L0 migration rewrote, and the inputs a re-encoded compaction record
/// names, for every rewrite that built its parts and then published, converged,
/// abandoned at its deadline, or stopped at a changed record set. A budget bounds how much one invocation does before
/// persisting its cursor and returning control, so a large migration runs
/// across many invocations without a lock or a long-lived process. It does not
/// bound request cost: the walk reads the compaction and rewrite records of
/// every sealed, untombstoned bucket it passes, one with no L0 commit record
/// included, and a
/// [`FamilyMigrateReport::reencode_blocked`] bucket spends no budget.
///
/// Because the underlying rewrite is bucket-atomic, the budget is a soft cap: an
/// invocation always finishes the bucket it is in the middle of (it never
/// interrupts a publish), then stops once the cumulative record count reaches
/// the budget. A bucket therefore never straddles two invocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrateBudget {
    /// Maximum L0 records to migrate before yielding. `0` means unlimited:
    /// drain the entire walk in one invocation.
    pub max_records: u64,
}

impl MigrateBudget {
    /// Drain the whole walk in one invocation (no per-invocation bound).
    pub fn unlimited() -> Self {
        MigrateBudget { max_records: 0 }
    }

    /// Yield after migrating `n` L0 records (rounded up to the enclosing
    /// bucket boundary, since a bucket rewrite is atomic).
    pub fn records(n: u64) -> Self {
        MigrateBudget { max_records: n }
    }

    /// Whether `spent` records has reached this budget. An unlimited budget is
    /// never exhausted.
    fn is_exhausted(&self, spent: u64) -> bool {
        self.max_records != 0 && spent >= self.max_records
    }
}

/// Why a bucket cannot be migrated by this job, whatever the operator runs next
/// (ADR-1331 decision 2). Every variant is permanent in the sense the
/// maintenance guide gives the word: re-running `migrate` reports the same
/// bucket again. What clears each one is on the variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockedReason {
    /// A live rewrite record (selective erasure, ADR-0064) carries
    /// `below_target` surviving parts below the target version. The migration
    /// job never rewrites them: a second record set over inputs a rewrite
    /// already covers resurrects the records that rewrite deliberately dropped,
    /// unless the same `drops` are re-applied, which is an erasure rewrite with
    /// ADR-0064's request-binding obligations rather than a migration
    /// (ADR-1331 decision 1).
    ///
    /// `below_target` sums the below-target parts of every rewrite record the
    /// bucket LISTS, which is not the same as its live ones: a predecessor a
    /// later rewrite superseded stays listed until `sweep` deletes it, and its
    /// parts keep counting until then. So a bucket whose superseding rewrite is
    /// already at the current output version stays blocked, by its
    /// predecessor's parts, until that sweep runs. The block clears when
    /// retention ages the bucket out, subject to the ADR-0066 #530 version
    /// hold, or when a later erasure request supersedes the record at the
    /// current output version AND a sweep removes the superseded predecessor
    /// (ADR-1331 decision 3).
    RewriteParts { below_target: usize },
    /// Overlapping compaction records resolve to a winner and a loser, and a
    /// raw L0 input only the loser names is still served raw and below the
    /// target. A new record over that subset joins the same overlap component
    /// and loses to the existing winner, so no rewrite migrates it. This is
    /// about the loser's INPUTS; the loser's own output parts are
    /// [`Self::LosingRecordParts`].
    ///
    /// Nothing in this build clears it except retention aging those inputs out,
    /// subject to the ADR-0066 #530 version hold. A "later authoritative
    /// compaction covering those inputs" would, but no path publishes one:
    /// [`crate::compact::compact_bucket`] and
    /// [`crate::rewrite::migrate_bucket_format`] both refuse a bucket that
    /// already carries a compaction record, so the bucket's record set is
    /// closed.
    LoserOnlyInputs,
    /// Every authoritative compaction record of the bucket has all of its parts
    /// at or above the target, and the records that lost their overlap
    /// component carry `below_target` output parts below it, summed over those
    /// losers (ADR-0066 force 2 amendment, item 9). Nothing serves a loser's
    /// parts, but they still count in [`BelowTargetReport::l1`]: a build that
    /// predates the authoritative-selection rule may still serve them, so they
    /// are no safe basis for raising the floor. A record a present rewrite
    /// record supersedes is left out of both sides of this test, whether the
    /// selector reads it as a winner or as a loser.
    ///
    /// Not this reason: a bucket whose authoritative records are themselves
    /// below the target (its winner has not converged either); a bucket whose
    /// rewrite record parts are below the target, which is named
    /// [`Self::RewriteParts`] alone (a bucket whose rewrite record parts are
    /// all at the target is named this way when its losers qualify); a record
    /// a present rewrite record supersedes, which `sweep` deletes with its
    /// parts as that rewrite's chain group, and whose parts count in `l1`
    /// until then without naming the bucket; and a record a present version 2
    /// record supersedes or a version 2 record a live rewrite dominates, which
    /// nothing in this build reclaims either.
    ///
    /// A loser may also name raw L0 inputs that only it covers; those count in
    /// [`BelowTargetReport::l0`], and when the walk named the bucket
    /// [`Self::LoserOnlyInputs`] for them this entry replaces that one, since
    /// the two clear the same way.
    ///
    /// Neither `migrate` nor `sweep` reclaims the parts this entry counts, so
    /// re-running them does not clear it. Retention does, when it ages the
    /// bucket out, subject to the ADR-0066 #530 version hold. A `sweep` can
    /// still change the entry of a bucket that lists a rewrite record: once it
    /// deletes a winner that rewrite superseded, a loser that overlapped only
    /// that winner becomes authoritative, so the bucket is no longer named for
    /// it while its below-target parts still count in `l1`.
    LosingRecordParts { below_target: usize },
}

/// One bucket the migration job cannot migrate, named with its reason
/// (ADR-1331 decision 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedBucket {
    pub shard: u32,
    pub ingest_hour: u32,
    pub reason: BlockedReason,
}

/// Why the walk did not re-encode a bucket whose authoritative compaction
/// records hold parts below the target (ADR-0066 force 2). Re-running `migrate`
/// reports the same bucket again; what clears each one is on the variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReencodeBlockedReason {
    /// The bucket is the case force 2 exists for: one compaction record
    /// survives supersession and `below_target` of its parts are below the
    /// target. The re-encode is available but
    /// [`CompactorConfig::reencode_writer_enabled`] is off, so nothing was
    /// written. Turning the switch on clears it, subject to the force 2
    /// amendment's rollout rule (item 8).
    WriterDisabled { below_target: usize },
    /// An overlap component of the bucket holds `largest_component` compaction
    /// records once the records a version 2 record supersedes are set aside, so
    /// the bucket is not re-encoded (force 2 amendment, item 4). Only retention
    /// clears it, subject to the ADR-0066 #530 version hold.
    ContestedOverlap { largest_component: usize },
    /// `records` compaction records survive supersession, each alone in its own
    /// overlap component, and the re-encode rewrites a bucket's one record
    /// only. Only retention clears it, subject to the ADR-0066 #530 version
    /// hold.
    MultipleRecords { records: usize },
}

/// One bucket the walk found below the target in its compaction parts and did
/// not re-encode, named with its reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReencodeBlockedBucket {
    pub shard: u32,
    pub ingest_hour: u32,
    pub reason: ReencodeBlockedReason,
}

/// Which of the walk's two rewrites a [`NotMigratedBucket`] was dispatched to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPath {
    /// The L0 migration ([`migrate_bucket_format`]).
    L0Migration,
    /// The force 2 re-encode of a compaction record's parts
    /// ([`reencode_compaction_parts`]).
    Reencode,
}

/// Why a rewrite the walk dispatched published nothing. The bucket is retried,
/// planned again from scratch, by the next walk that reaches it: the next run
/// when this one drained the walk (a drained walk clears the cursor), and after
/// a budget stop the first run after the walk drains, since the persisted
/// cursor is past it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotMigratedReason {
    /// The bucket's claim was not available, so the rewrite built nothing
    /// (ADR-1029, the 2026-10-03 amendment).
    ClaimSkipped { reason: ClaimSkipReason },
    /// The rewrite took the bucket's claim, lost it, and cancelled at `at`
    /// before its record PUT.
    Cancelled { at: Checkpoint },
    /// The pre-publish re-list found a record set other than the one the
    /// rewrite planned from.
    RecordSetChanged,
    /// The rewrite passed its `max_compaction_lifetime_ns` deadline before its
    /// record PUT and abandoned the publish.
    PublishAbandoned,
}

/// One bucket the walk dispatched a rewrite for that published nothing, named
/// with the path and the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotMigratedBucket {
    pub shard: u32,
    pub ingest_hour: u32,
    pub path: MigrationPath,
    pub reason: NotMigratedReason,
}

/// What one [`count_below_target`] pass found below the target, with the three
/// sources kept apart (ADR-1331 decision 1).
///
/// The split is load-bearing rather than cosmetic: the three sources reach the
/// target by different routes. The walk rewrites below-target L0 records. A
/// below-target compaction part is re-encoded only through ADR-0066 force 2
/// ([`reencode_compaction_parts`]), only when its record is the one compaction
/// record of its bucket and the writer switch is on, and its predecessor's
/// parts leave this figure only when `sweep` deletes them. Nothing rewrites a
/// rewrite record's parts, by decision (ADR-1331 decision 1). Summing the three
/// into one figure would hide which part of it a re-run could move.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BelowTargetReport {
    /// Below-target L0 commit records still live. A bucket's pre-rewrite commit
    /// records, once an authoritative compaction record or a rewrite record
    /// names them as inputs, are excluded rather than counted here.
    pub l0: usize,
    /// Below-target parts of compaction records only, authoritative or not.
    /// They refuse the floor raise. A bucket whose authoritative records are
    /// all at the target and whose overlap losers carry below-target parts is
    /// named in [`Self::blocked`] as [`BlockedReason::LosingRecordParts`], with
    /// the losers' share of this figure; a bucket whose authoritative records
    /// are themselves below the target is not named here. The walk re-encodes
    /// such a bucket when force 2 applies, or names it in
    /// [`FamilyMigrateReport::reencode_blocked`] when it does not.
    ///
    /// Like [`Self::rewrite_parts`], this is a count over LISTED records: a
    /// compaction record that a later rewrite record or a version 2 compaction
    /// record superseded stays listed until `sweep` deletes it and its parts,
    /// so its parts are in this figure while it is there. A bucket the walk
    /// re-encoded therefore keeps its predecessor's parts in this figure until
    /// that sweep, and the floor is raised by the first `migrate` run after it.
    pub l1: usize,
    /// Below-target parts of the rewrite records a bucket LISTS, which this job
    /// never migrates. A superseded predecessor stays listed until `sweep`
    /// deletes it, so its parts are in this figure alongside its successor's.
    pub rewrite_parts: usize,
    /// The buckets whose `rewrite_parts` block the floor, one entry per bucket
    /// with its exact count, summed over every rewrite record that bucket
    /// lists, and the buckets held below the target only by overlap losers'
    /// parts ([`BlockedReason::LosingRecordParts`]), whose count is also in
    /// `l1`. A bucket whose rewrite parts are below the target is named
    /// `RewriteParts` only; one whose rewrite parts are all at the target can
    /// be named `LosingRecordParts`. The walk contributes the
    /// [`BlockedReason::LoserOnlyInputs`] entries separately, so this pass
    /// reports only what it can see for itself. Sorted by
    /// `(shard, ingest_hour)`.
    pub blocked: Vec<BlockedBucket>,
}

impl BelowTargetReport {
    /// Every object that still exists below the target, some of which queries
    /// still read: a nonzero total refuses the floor raise. Not every entry is
    /// live to a reader. Both part figures are counts over LISTED records:
    /// `rewrite_parts` includes a superseded predecessor a later rewrite
    /// replaced, and `l1` includes a compaction record a later rewrite
    /// superseded. The resolver serves neither, and `sweep` deletes both, but
    /// they are in the total for as long as the bucket lists them.
    pub fn total(&self) -> usize {
        self.l0 + self.l1 + self.rewrite_parts
    }
}

/// The verification result recorded on a [`FamilyMigrateReport`] once the walk
/// has drained: the driver re-audits fresh and either raises the floor or
/// refuses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verification {
    /// The fresh re-audit found zero records below the target, so the floor was
    /// raised to (or was already at or above) `floor_version`.
    FloorRaised { floor_version: u32 },
    /// The fresh re-audit found at least one record below the target -- data
    /// that landed after the walk passed, or that the walk could not migrate --
    /// so the floor was left untouched. `l0` counts below-target commit records
    /// still live (a bucket's pre-rewrite commit records already superseded by
    /// an authoritative compaction record, or by a rewrite record, are
    /// excluded, not counted here: [`count_below_target`]), `l1` counts
    /// below-target compaction parts of every listed compaction record,
    /// including a predecessor a version 2 record re-encoded until `sweep`
    /// deletes it, and `rewrite_parts` counts below-target parts of the
    /// rewrite records each bucket lists, which this job never migrates.
    /// `blocked` names, with its reason, each bucket whose refusal no re-run
    /// clears THAT THIS INVOCATION EXAMINED (ADR-1331 decision 2); it is the
    /// same list [`FamilyMigrateReport::blocked_buckets`] carries, and carries
    /// that list's resume-window caveat too. The buckets the walk did not
    /// re-encode, and those whose rewrite published nothing, are on
    /// [`FamilyMigrateReport::reencode_blocked`] and
    /// [`FamilyMigrateReport::not_migrated`].
    Stragglers {
        l0: usize,
        l1: usize,
        rewrite_parts: usize,
        blocked: Vec<BlockedBucket>,
    },
}

impl Verification {
    /// Whether the re-audit refused the raise because stragglers were found.
    pub fn refused(&self) -> bool {
        matches!(self, Verification::Stragglers { .. })
    }
}

/// The outcome of one [`migrate_family`] invocation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FamilyMigrateReport {
    /// Buckets examined this invocation (past the cursor, in the walk range).
    pub buckets_examined: usize,
    /// Buckets this invocation published a migration for: a live L0 set
    /// rewritten to the target whose record was published or converged, or a
    /// compaction record's parts re-encoded whose version 2 record this run
    /// published. A rewrite whose publish was abandoned is not counted; it is
    /// on [`Self::not_migrated`].
    pub buckets_migrated: usize,
    /// L0 records whose data the buckets in [`Self::buckets_migrated`] carried
    /// to the target, counted differently on each path. An L0 migration counts
    /// the bucket's raw-served L0 records that were below the target, not the
    /// at-target records its rewrite also carried into the new record. A
    /// re-encode counts every input the re-encoded compaction record names,
    /// whatever version each was written at.
    pub records_migrated: u64,
    /// Every bucket this invocation found permanently blocked, each named with
    /// its `(shard, ingest_hour)` and the reason no re-run clears it
    /// (ADR-1331 decision 2).
    ///
    /// Three conditions land here and none is a transient refusal. A bucket
    /// whose overlapping compaction records resolve to a winner and a loser
    /// leaves every input only the loser names served as a raw L0 segment: it
    /// is live, and rewriting that subset cannot migrate it, because a new
    /// record over it would join the same overlap component and lose to the
    /// existing winner ([`BlockedReason::LoserOnlyInputs`], found by the walk).
    /// A bucket whose live rewrite record carries parts below the target is one
    /// this job never rewrites at all ([`BlockedReason::RewriteParts`], found by
    /// the re-audit). A bucket whose authoritative compaction records are at
    /// the target while an overlap loser's own parts are below it is held down
    /// by parts nothing in this build reclaims
    /// ([`BlockedReason::LosingRecordParts`], found by the re-audit). Each
    /// makes [`count_below_target`] report stragglers on every invocation, so
    /// the family's floor stays unraised until the condition itself resolves;
    /// naming the bucket and the reason is what keeps an operator from
    /// re-running a job that cannot converge.
    ///
    /// The two passes have different scopes and the list is their union, so it
    /// is not a census of the family. The re-audit covers every shard, so its
    /// [`BlockedReason::RewriteParts`] and
    /// [`BlockedReason::LosingRecordParts`] entries are complete. The walk covers
    /// only the buckets PAST THE CURSOR that this invocation examined, so a
    /// walk resumed mid-family omits the [`BlockedReason::LoserOnlyInputs`]
    /// buckets an earlier invocation found and does not re-report them.
    ///
    /// Sorted by `(shard, ingest_hour)`, one entry per bucket. Where both
    /// passes name the same bucket the re-audit's entry is the one kept, since
    /// the re-audit's reasons carry a count and the walk's does not.
    pub blocked_buckets: Vec<BlockedBucket>,
    /// Every bucket the walk found held below the target by its authoritative
    /// compaction records' parts and did not re-encode (ADR-0066 force 2), in
    /// walk order, with the reason. The re-audit counts those parts in `l1`.
    /// Like the walk's [`BlockedReason::LoserOnlyInputs`] entries, it covers
    /// only the buckets this invocation examined.
    pub reencode_blocked: Vec<ReencodeBlockedBucket>,
    /// Every bucket the walk dispatched a rewrite for that published nothing,
    /// in walk order, with the path and the reason: a claim another process
    /// held, a claim this run lost, a record set that changed before the
    /// publish, or a publish abandoned at the deadline. None of these counts in
    /// [`Self::buckets_migrated`], and the fresh re-audit still counts what each
    /// left below the target unless another writer carried it there first.
    ///
    /// The cursor advances past these buckets like any other examined one, so
    /// a held claim never stalls the walk. When the walk drains it clears the
    /// cursor and the next run retries every one; after a budget stop the next
    /// run resumes past them, and they are retried by the first run after the
    /// walk drains.
    pub not_migrated: Vec<NotMigratedBucket>,
    /// The `(shard, ingest_hour)` the cursor was persisted at, when this
    /// invocation stopped on its budget. `None` once the walk completes: a
    /// drained walk clears the cursor rather than leaving a position behind.
    pub cursor_advanced_to: Option<(u32, u32)>,
    /// The walk reached its end within budget this invocation. When `true` the
    /// driver ran the verify-and-raise step and set [`Self::verification`]; when
    /// `false` the budget was exhausted and the caller should re-invoke.
    pub walk_complete: bool,
    /// The budget was reached this invocation and the walk stopped early.
    pub budget_exhausted: bool,
    /// The verify-then-raise outcome, present only when [`Self::walk_complete`]
    /// is `true`.
    pub verification: Option<Verification>,
}

impl FamilyMigrateReport {
    /// Whether this invocation completed the walk but the fresh re-audit refused
    /// to raise the floor because stragglers remain. The CLI uses this to exit
    /// nonzero.
    pub fn stragglers_found(&self) -> bool {
        matches!(&self.verification, Some(v) if v.refused())
    }

    /// How many buckets are permanently blocked: the length of
    /// [`Self::blocked_buckets`] (ADR-1331 decision 2). Its documented meaning,
    /// "re-running is not the remedy", covers both permanent cases.
    pub fn buckets_blocked(&self) -> usize {
        self.blocked_buckets.len()
    }
}

/// The advisory migrate-cursor key for one `(tenant, signal, family)`. It lives
/// beside [`keys::maint_cursor_key`]'s compaction cursor under the same
/// `maint/` directory but on a disjoint path -- `maint/migrate/<family>/cursor`
/// vs the compaction cursor's `maint/<shard>/cursor` -- so the two never
/// collide (a shard segment is always numeric; `migrate` never is) and neither
/// is ever listed as the other's sibling. Both are advisory CAS-mutable state,
/// exempt from the object-immutability rule exactly as the ADR-0003 catalog HEAD
/// pointer is (ADR-0018 §Decision 7).
///
/// `t/<tenant_hash_hex>/<signal>/maint/migrate/<family>/cursor`
fn migrate_cursor_key(tenant_hash: &TenantHash, signal: Signal, family: &str) -> String {
    format!(
        "t/{}/{}/{}/migrate/{}/cursor",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        keys::MAINT_DIR,
        family,
    )
}

/// Read the advisory migrate cursor. Returns the recorded `(shard, hour)`
/// position (the last bucket the walk finished) and the object version for the
/// next CAS. A decode failure or unrecognized payload is treated as "no usable
/// position" (`None`, rescan from the start) while keeping the version so the
/// stale bytes can be CAS-overwritten cleanly; store faults propagate.
async fn read_cursor(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<(Option<(u32, u32)>, Option<Version>)> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            let data = got.data;
            if data.len() == MIGRATE_CURSOR_LEN && data[0] == MIGRATE_CURSOR_TAG {
                let shard = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                let hour = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);
                Ok((Some((shard, hour)), Some(got.version)))
            } else {
                // Unrecognized payload: ignore the position (full rescan) but
                // keep the version so the next write CAS-overwrites it cleanly.
                Ok((None, Some(got.version)))
            }
        }
        Err(StoreError::NotFound) => Ok((None, None)),
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// Persist the advisory migrate cursor at `(shard, hour)`. First write is
/// `CreateIfAbsent`; a subsequent write CAS-updates against the version read.
/// A lost race (`AlreadyExists`/`PreconditionFailed`) is not an error: the
/// cursor is advisory and a concurrent migrator's update is equally valid, so
/// the loser simply proceeds (and re-reads the cursor on its next invocation).
async fn write_cursor(
    store: &dyn ObjectStoreBackend,
    key: &str,
    shard: u32,
    hour: u32,
    prev_version: Option<Version>,
) -> Result<()> {
    let mut payload = Vec::with_capacity(MIGRATE_CURSOR_LEN);
    payload.push(MIGRATE_CURSOR_TAG);
    payload.extend_from_slice(&shard.to_le_bytes());
    payload.extend_from_slice(&hour.to_le_bytes());
    let mode = match prev_version {
        Some(v) => PutMode::CasVersion(v),
        None => PutMode::CreateIfAbsent,
    };
    let opts = PutOptions {
        mode,
        checksum: Some(UploadChecksum::Crc32c(crc32c::crc32c(&payload))),
    };
    match store.put(key, payload.into(), opts).await {
        Ok(_) | Err(StoreError::AlreadyExists) | Err(StoreError::PreconditionFailed) => Ok(()),
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// The generation-aware shard scan range for one `(tenant, signal)`: the union
/// of every shard-generation's range, i.e. the largest `shard_count` across the
/// tenant's history (ADR-0052 section 4). Identical in intent to the CLI's
/// `audit-versions` scan range and the server maintain loop's, so a migration
/// visits every shard any generation ever wrote and never silently skips live
/// objects in a post-reshard range. Fail-closed on a read error: a migration
/// over a possibly-truncated shard range could raise a floor while stragglers
/// hide in an unscanned shard, so this refuses rather than guessing. An absent
/// record is the single implicit generation at `configured_shards`.
async fn scan_shard_count(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    configured_shards: u32,
) -> Result<u32> {
    match ravel_catalog::read_generations_from_store(store, tenant_hash, signal).await {
        Ok(Some(generations)) => Ok(generations
            .iter()
            .map(|g| g.shard_count)
            .max()
            .unwrap_or(configured_shards)),
        Ok(None) => Ok(configured_shards),
        Err(err) => Err(MaintainError::Provisioning(format!(
            "shard-generation history read failed for signal {signal:?}: {err}; refusing to \
             migrate a possibly-truncated shard range (ADR-0052 section 4)"
        ))),
    }
}

/// List every ingest-hour bucket present under one `(tenant, signal, shard)`,
/// ascending. Mirrors [`crate::scan`]'s private `list_shard_hours`; a non-hour
/// common prefix under the shard is layout drift and errors rather than being
/// silently skipped.
async fn list_shard_hours(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<Vec<u32>> {
    let shard_prefix = keys::commit_shard_prefix(tenant_hash, signal, shard)?;
    let listed = store.list_delimited(&shard_prefix).await?;
    let mut hours: Vec<u32> = Vec::new();
    for common in &listed.common_prefixes {
        let rest = common
            .strip_prefix(&shard_prefix)
            .and_then(|r| r.strip_suffix('/'))
            .unwrap_or("");
        match keys::parse_ingest_hour_string(rest) {
            Ok(hour) => hours.push(hour),
            Err(e) => return Err(MaintainError::Key(e)),
        }
    }
    hours.sort_unstable();
    Ok(hours)
}

/// Count live records below `target_version` for one `(tenant, signal)` across
/// `scan_shards`, read fresh and uncached: below-target L0 commit records and
/// below-target L1 compaction parts, mirroring the `audit-versions`
/// enumeration's liveness definition (a surviving commit/compaction record is
/// the evidence of a live object; data objects are never listed directly). This
/// is the verification pass; a nonzero total refuses the floor raise.
///
/// An L0 commit record is excluded once some compaction or rewrite record in
/// the same shard names it as an input: the record's own parts are then the
/// live successor of that commit record, which is a pre-rewrite leftover,
/// sweepable but not live. Without this exclusion a migrate
/// invocation that just rewrote a bucket would count its own superseded L0
/// records as stragglers and refuse the floor raise it earned, converging only
/// after an unrelated sweep physically deletes them.
///
/// The exclusion is keyed on the superseding record's explicit `inputs` list
/// (via [`crate::sweep::superseded_input_commit_keys`], the same predicate
/// sweep rule 2 deletes by), not on membership of the record's ingest-hour
/// bucket. Bucket membership gives the same answer today, since
/// compaction and rewrite refuse an unsealed bucket and a sealed bucket's L0
/// set is frozen, so any record over a bucket covers that bucket's whole L0
/// set. But this re-audit exists to verify the walk independently, and keying
/// it on membership would make it inherit that seal invariant as a premise
/// instead of confirming coverage. If a partial-coverage record ever exists
/// (naming some but not all of its bucket's L0 set), membership would exclude
/// the still-live, un-migrated remainder and raise the floor over data below
/// the target, a false claim about durable state; the input-set predicate
/// excludes exactly the records that were actually superseded.
///
/// Only an AUTHORITATIVE compaction record's inputs are excluded. Compaction
/// records whose input sets overlap resolve to one winner per overlap
/// component ([`select_authoritative_compaction_records`], the rule the
/// resolver, the index fold, the superseded-input sweep and the erasure
/// completion gate all share); a loser's parts are served from nowhere, so an
/// input only a loser names is still served as a raw L0 segment by
/// `Catalog::resolve` and is exactly the live, below-target object the floor
/// would be falsely raised over. Excluding a loser's whole input set is
/// therefore the same false durable claim this predicate exists to prevent,
/// one step further in: the record would be neither migrated by the walk nor
/// counted here.
///
/// Rewrite records are not part of that selection: ADR-0064 decision 3 point 5
/// keeps one record set per bucket, so a live rewrite record has no overlapping
/// peer to lose to and its whole input list supersedes.
///
/// A rewrite record's parts are counted apart from compaction parts
/// (ADR-1331 decision 1) and their bucket is named in
/// [`BelowTargetReport::blocked`]. They still refuse the floor raise -- they are
/// exactly the live below-target objects a raised floor would deny the
/// existence of -- but nothing in this crate ever migrates them, and folding
/// them into `l1` left the report unable to name the bucket holding the floor
/// down or say how many of its parts did it.
///
/// The count is over LISTED records, not live ones. A rewrite record a later
/// rewrite superseded stays listed until `sweep` deletes it, so its parts are
/// summed into its bucket's figure alongside the successor's for as long as it
/// is there.
///
/// A losing compaction record's parts count in `l1` like any other compaction
/// part (ADR-0066 force 2 amendment, item 9): an older build may still serve
/// them, so the floor is refused over them. When every authoritative record of
/// the bucket is at the target, the losers' parts are what holds its compaction
/// parts below (a loser's raw L0 inputs, if any, count in `l0`), and the
/// bucket is named [`BlockedReason::LosingRecordParts`] with the losers'
/// below-target part count, from the same [`authoritative_compaction_records`]
/// selection that decides which inputs are superseded. A bucket that lists a
/// rewrite record is named this way too when the rewrite record's parts are all
/// at the target; one whose rewrite parts are below the target keeps its
/// [`BlockedReason::RewriteParts`] entry alone. A record a present rewrite
/// record supersedes is left out of both the authoritative and the losing
/// side of that test: `sweep` deletes it and its parts with the rewrite's
/// chain group, so its parts count in `l1` until then and name nothing. Nothing
/// in this build reclaims the remaining losers' parts, so only retention clears
/// that entry, subject to the format-version hold.
pub async fn count_below_target(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    scan_shards: u32,
    target_version: u32,
) -> Result<BelowTargetReport> {
    let mut report = BelowTargetReport::default();
    for shard in 0..scan_shards {
        let family = read_shard_family(
            store,
            tenant_hash,
            signal,
            shard,
            ShardReader::MigrateReaudit,
        )
        .await?;
        // A bucket can list more than one rewrite record (a superseding pass
        // and the predecessor it supersedes, until a sweep removes the latter),
        // so the below-target parts are summed per bucket before the bucket is
        // named: the operator reads one line per blocked bucket carrying its
        // whole count, not one line per record.
        let mut rewrite_below_by_hour: BTreeMap<u32, usize> = BTreeMap::new();
        for rec in &family.records {
            let below = rec
                .part_versions
                .iter()
                .filter(|v| **v < target_version)
                .count();
            match rec.kind {
                RecordKind::Compaction => report.l1 += below,
                RecordKind::Rewrite => {
                    report.rewrite_parts += below;
                    if below > 0 {
                        *rewrite_below_by_hour
                            .entry(rec.ingest_hour_bucket)
                            .or_default() += below;
                    }
                }
            }
        }
        let mut blocked_by_hour: BTreeMap<u32, BlockedReason> = rewrite_below_by_hour
            .into_iter()
            .map(|(hour, below_target)| (hour, BlockedReason::RewriteParts { below_target }))
            .collect();
        // A losing compaction record's below-target parts are already in `l1`
        // above. The bucket is named for them only when its authoritative
        // records are all at the target, so the losers are what holds it
        // below; a bucket already named `RewriteParts` keeps that entry alone.
        for (hour, parts) in &family.compaction_authority {
            if blocked_by_hour.contains_key(hour) {
                continue;
            }
            if parts.authoritative.iter().any(|v| *v < target_version) {
                continue;
            }
            let below_target = parts.losing.iter().filter(|v| **v < target_version).count();
            if below_target > 0 {
                blocked_by_hour.insert(*hour, BlockedReason::LosingRecordParts { below_target });
            }
        }
        for (ingest_hour, reason) in blocked_by_hour {
            report.blocked.push(BlockedBucket {
                shard,
                ingest_hour,
                reason,
            });
        }
        for key in family.commit_keys {
            if family.superseded_commits.contains(&key) {
                continue;
            }
            let got = store.get(&key, GetRange::Full).await?;
            let rec = record::decode(&got.data)?;
            if rec.segment_format_version < target_version {
                report.l0 += 1;
            }
        }
    }
    Ok(report)
}

/// The live commit-family population of one `(tenant, signal)` by
/// `segment_format_version`, from one enumeration: the liveness
/// [`count_below_target`] verifies a floor raise with, bucketed by version so
/// any number of floors is classified against it without re-listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FamilyCensus {
    /// Version -> L0 commit records no authoritative compaction record and no
    /// rewrite record names as an input.
    pub live_l0: BTreeMap<u32, usize>,
    /// Version -> L0 commit records still listed although an authoritative
    /// compaction or rewrite record supersedes them (sweepable, not live).
    pub superseded_l0: BTreeMap<u32, usize>,
    /// Version -> parts of every compaction and rewrite record.
    pub parts: BTreeMap<u32, usize>,
    /// Newest `created_unix_ns` among the live commit records and every
    /// compaction and rewrite record; `None` when there are none.
    pub newest_live_created_unix_ns: Option<i64>,
}

impl FamilyCensus {
    /// Live entries below `target_version`: [`BelowTargetReport::total`] for
    /// that target, as a prefix sum over the live histograms. The census keeps
    /// no compaction/rewrite split, so this is the sum of all three of
    /// [`count_below_target`]'s figures, not any one of them.
    pub fn live_below(&self, target_version: u32) -> usize {
        let below = |hist: &BTreeMap<u32, usize>| -> usize {
            hist.range(..target_version).map(|(_, n)| *n).sum()
        };
        below(&self.live_l0) + below(&self.parts)
    }

    fn saw_live_created(&mut self, created_unix_ns: i64) {
        self.newest_live_created_unix_ns = Some(
            self.newest_live_created_unix_ns
                .map_or(created_unix_ns, |n| n.max(created_unix_ns)),
        );
    }
}

/// Enumerate every commit-family record of a `(tenant, signal)` across
/// `scan_shards` once, read fresh: one LIST pass per shard and one GET per
/// record, superseded commit records included so the census can report them
/// apart from the live population.
pub async fn census_family(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    scan_shards: u32,
) -> Result<FamilyCensus> {
    let mut census = FamilyCensus::default();
    for shard in 0..scan_shards {
        let family =
            read_shard_family(store, tenant_hash, signal, shard, ShardReader::Census).await?;
        for rec in &family.records {
            for version in &rec.part_versions {
                *census.parts.entry(*version).or_default() += 1;
            }
            census.saw_live_created(rec.created_unix_ns);
        }
        let label = ShardReader::Census.label();
        for key in family.commit_keys {
            let got = read_named_record(store, &key, "commit record", label).await?;
            let rec = record::decode(&got.data).map_err(|err| {
                MaintainError::Invariant(format!(
                    "commit record {key} is corrupt during {label}: {err}"
                ))
            })?;
            if family.superseded_commits.contains(&key) {
                *census
                    .superseded_l0
                    .entry(rec.segment_format_version)
                    .or_default() += 1;
            } else {
                *census
                    .live_l0
                    .entry(rec.segment_format_version)
                    .or_default() += 1;
                census.saw_live_created(rec.created_unix_ns);
            }
        }
    }
    Ok(census)
}

/// Which caller is reading a shard's records: it names the pass in a
/// corrupt-record error and decides how strictly a rewrite record is decoded.
#[derive(Debug, Clone, Copy)]
enum ShardReader {
    /// [`count_below_target`]: decodes a rewrite record's protobuf only.
    MigrateReaudit,
    /// [`census_family`]: validates a rewrite record as `audit-versions`
    /// always has.
    Census,
}

impl ShardReader {
    fn label(self) -> &'static str {
        match self {
            ShardReader::MigrateReaudit => "migration re-audit",
            ShardReader::Census => "format census",
        }
    }
}

/// GET one record for a population read, naming its key and the reader in the
/// error so an operator can find the object that failed.
async fn read_named_record(
    store: &dyn ObjectStoreBackend,
    key: &str,
    kind: &str,
    label: &str,
) -> Result<ravel_object_store::GetOutcome> {
    store.get(key, GetRange::Full).await.map_err(|err| {
        let why = match err {
            StoreError::NotFound => "no longer present (deleted after the listing)".to_string(),
            other => other.to_string(),
        };
        MaintainError::Invariant(format!(
            "{kind} {key} could not be read during {label}: {why}"
        ))
    })
}

/// Which kind of record contributed parts. The re-audit keeps them apart rather
/// than summing them because they are blocked for different reasons and clear
/// differently (ADR-1331 decision 1): a below-target rewrite part is never
/// migrated by decision, while a below-target compaction part converges through
/// ADR-0066 decision 4 force 2's re-encode when its record is its bucket's one
/// compaction record and the writer switch is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordKind {
    Compaction,
    Rewrite,
}

/// One compaction or rewrite record's contribution to the population.
struct RecordParts {
    kind: RecordKind,
    ingest_hour_bucket: u32,
    part_versions: Vec<u32>,
    created_unix_ns: i64,
}

/// One shard's commit-family listing with every compaction and rewrite record
/// read and authority resolved.
struct ShardFamily {
    /// Every commit record key the shard lists, in listing order, unread.
    commit_keys: Vec<String>,
    /// The subset of `commit_keys` (and possibly keys no longer listed) an
    /// authoritative compaction record or a rewrite record names as an input.
    superseded_commits: HashSet<String>,
    records: Vec<RecordParts>,
    /// Per ingest hour holding compaction records, the part versions of its
    /// authoritative records and of its overlap losers.
    compaction_authority: BTreeMap<u32, BucketPartVersions>,
}

/// The part versions of one bucket's compaction records, split by
/// [`BucketAuthority`].
struct BucketPartVersions {
    authoritative: Vec<u32>,
    losing: Vec<u32>,
}

async fn read_shard_family(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    reader: ShardReader,
) -> Result<ShardFamily> {
    use prost::Message;

    let prefix = keys::commit_shard_prefix(tenant_hash, signal, shard)?;
    let metas = list_all(store, &prefix).await?;

    // First pass: classify every key by shape (parsing a key's shape is free,
    // it only inspects the filename), read each compaction and rewrite record,
    // keep its parts' versions, and collect the commit keys it explicitly
    // supersedes. Commit records are left to the caller because the full
    // supersession set is only known once every record in the shard has been
    // seen. Compaction records are additionally held per ingest-hour bucket
    // rather than resolved inline: an overlap component is a property of one
    // bucket (the unit the resolver reads), so which of them are authoritative
    // is only decidable once the whole bucket's set is in hand.
    let mut commit_keys = Vec::with_capacity(metas.len());
    let mut superseded_commits: HashSet<String> = HashSet::new();
    let mut records = Vec::new();
    let mut compaction_by_bucket: HashMap<u32, Vec<(String, CompactionRecord)>> = HashMap::new();
    let mut rewrite_by_bucket: HashMap<u32, Vec<(String, RewriteRecord)>> = HashMap::new();
    for meta in metas {
        let entry = keys::partition_bucket_entry(&meta.key).map_err(MaintainError::Key)?;
        let key = meta.key;
        match entry {
            keys::BucketEntry::CommitRecord(_) => commit_keys.push(key),
            keys::BucketEntry::CompactionRecord(_) => {
                let got =
                    read_named_record(store, &key, "compaction record", reader.label()).await?;
                let rec = record::decode_compaction(got.data.as_ref()).map_err(|err| {
                    MaintainError::Invariant(format!(
                        "compaction record {key} is corrupt during {}: {err}",
                        reader.label()
                    ))
                })?;
                records.push(RecordParts {
                    kind: RecordKind::Compaction,
                    ingest_hour_bucket: rec.ingest_hour_bucket,
                    part_versions: rec.parts.iter().map(|p| p.segment_format_version).collect(),
                    created_unix_ns: rec.created_unix_ns,
                });
                compaction_by_bucket
                    .entry(rec.ingest_hour_bucket)
                    .or_default()
                    .push((key, rec));
            }
            // A rewrite record (selective erasure, ADR-0064) carries the same
            // CompactionPart parts as a compaction record; its surviving parts
            // can sit below the target version and must be counted, or a
            // "migration complete" claim could pass over unmigrated rewritten
            // objects. They are counted under their own [`RecordKind`] rather
            // than with L1 parts, because nothing migrates them
            // (ADR-1331). Its `inputs` list supersedes raw L0
            // exactly as a compaction record's does; a predecessor rewrite
            // (empty `inputs`, superseding a whole prior compaction or rewrite
            // record instead) supersedes no L0 record directly, and the
            // predecessor record it names still carries the L0 input list for
            // as long as that record exists.
            keys::BucketEntry::RewriteRecord(_) => {
                let got = read_named_record(store, &key, "rewrite record", reader.label()).await?;
                let rec = match reader {
                    ShardReader::MigrateReaudit => {
                        RewriteRecord::decode(got.data.as_ref()).map_err(|err| err.to_string())
                    }
                    ShardReader::Census => ravel_commit::erasure::decode_rewrite(got.data.as_ref())
                        .map_err(|err| err.to_string()),
                }
                .map_err(|err| {
                    MaintainError::Invariant(format!(
                        "rewrite record {key} is corrupt during {}: {err}",
                        reader.label()
                    ))
                })?;
                records.push(RecordParts {
                    kind: RecordKind::Rewrite,
                    ingest_hour_bucket: rec.ingest_hour_bucket,
                    part_versions: rec.parts.iter().map(|p| p.segment_format_version).collect(),
                    created_unix_ns: rec.created_unix_ns,
                });
                superseded_commits.extend(superseded_input_commit_keys(
                    tenant_hash,
                    signal,
                    shard,
                    &rec,
                )?);
                rewrite_by_bucket
                    .entry(rec.ingest_hour_bucket)
                    .or_default()
                    .push((key, rec));
            }
            keys::BucketEntry::Tombstone(_) => {}
        }
    }

    // Second pass: resolve each bucket's compaction records to one
    // authoritative record per overlap component and take only a winner's
    // inputs as superseded. An input a loser alone names has no live successor
    // -- the loser's parts are ignored -- so it is still served raw and still
    // counts as live. The same holds for a record a present version 2 record
    // supersedes and for a version 2 record a live rewrite dominates.
    let mut compaction_authority = BTreeMap::new();
    for (hour, bucket_records) in &compaction_by_bucket {
        let bucket = Bucket::new(*tenant_hash, signal, shard, *hour);
        let rewrites = rewrite_by_bucket
            .get(hour)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let authority = authoritative_compaction_records(&bucket, bucket_records, rewrites)?;
        for (_, rec) in &authority.authoritative {
            superseded_commits.extend(superseded_input_commit_keys(
                tenant_hash,
                signal,
                shard,
                rec,
            )?);
        }
        // A record a present rewrite supersedes is in neither list: its parts
        // belong to that rewrite's chain, which `sweep` reclaims, so it cannot
        // name the bucket. Its parts still count in `l1` while it is listed.
        let part_versions = |records: &[&(String, CompactionRecord)]| -> Vec<u32> {
            records
                .iter()
                .filter(|(key, _)| !authority.rewrite_superseded.contains(key.as_str()))
                .flat_map(|(_, rec)| rec.parts.iter().map(|p| p.segment_format_version))
                .collect()
        };
        compaction_authority.insert(
            *hour,
            BucketPartVersions {
                authoritative: part_versions(&authority.authoritative),
                losing: part_versions(&authority.losing),
            },
        );
    }

    Ok(ShardFamily {
        commit_keys,
        superseded_commits,
        records,
        compaction_authority,
    })
}

/// One bucket's compaction records as [`authoritative_compaction_records`]
/// resolved them. A record in neither list is one a present version 2 record
/// supersedes, or a version 2 record a live rewrite dominates: neither is an
/// overlap loser, and nothing in this build reclaims them either.
struct BucketAuthority<'a> {
    /// The records whose parts are served and whose inputs are superseded.
    authoritative: Vec<&'a (String, CompactionRecord)>,
    /// The records that lost their overlap component to another record
    /// ([`ravel_catalog::AuthoritativeSelection::losing`]). Nothing serves
    /// their parts, and nothing but retention reclaims a loser no present
    /// rewrite record supersedes.
    losing: Vec<&'a (String, CompactionRecord)>,
    /// The keys of the bucket's compaction records a present rewrite record
    /// supersedes, directly or through its chain
    /// ([`resolve_rewrite_supersession`]). Such a record can still be in
    /// either list above, since the selector does not see rewrite records;
    /// `sweep` deletes it and its parts with the rewrite's chain group.
    rewrite_superseded: HashSet<&'a str>,
}

/// The compaction records of one bucket whose inputs the resolver treats as
/// superseded: a version 2 record a live rewrite dominates is dropped
/// ([`ravel_catalog::erasure_dominated_compaction_records`]), and the selector
/// then excludes every record a present version 2 record supersedes and every
/// overlap loser. The overlap losers are returned apart, so the re-audit can
/// name a bucket held below the target by a loser's parts alone, and so are the
/// records a present rewrite supersedes, which it leaves out of that naming.
/// Both halves of this module ask the question here, so the walk and the
/// re-audit agree with `Catalog::resolve` and with each other.
fn authoritative_compaction_records<'a>(
    bucket: &Bucket,
    compaction_records: &'a [(String, CompactionRecord)],
    rewrite_records: &[(String, RewriteRecord)],
) -> Result<BucketAuthority<'a>> {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )?;
    let unresolvable = |err: ravel_catalog::CatalogError| {
        MaintainError::Invariant(format!(
            "bucket {prefix} has an unresolvable supersession chain: {err}"
        ))
    };
    let dominated =
        erasure_dominated_compaction_records(compaction_records, rewrite_records, &prefix)
            .map_err(unresolvable)?;
    let candidates: Vec<&(String, CompactionRecord)> = compaction_records
        .iter()
        .filter(|(key, _)| !dominated.contains(key.as_str()))
        .collect();
    let candidate_pairs: Vec<(&str, &CompactionRecord)> = candidates
        .iter()
        .map(|(key, rec)| (key.as_str(), rec))
        .collect();
    let selection =
        select_authoritative_compaction_records(&candidate_pairs).map_err(unresolvable)?;
    let mut rewrite_superseded_keys: HashSet<String> = HashSet::new();
    if !rewrite_records.is_empty() {
        let compaction_by_key: HashMap<&str, &CompactionRecord> = compaction_records
            .iter()
            .map(|(key, rec)| (key.as_str(), rec))
            .collect();
        let rewrite_by_key: HashMap<&str, &RewriteRecord> = rewrite_records
            .iter()
            .map(|(key, rec)| (key.as_str(), rec))
            .collect();
        let mut inputs = HashSet::new();
        for (key, rec) in rewrite_records {
            resolve_rewrite_supersession(
                key,
                rec,
                &prefix,
                &compaction_by_key,
                &rewrite_by_key,
                &mut inputs,
                &mut rewrite_superseded_keys,
            )
            .map_err(unresolvable)?;
        }
    }
    let mut authority = BucketAuthority {
        authoritative: Vec::new(),
        losing: Vec::new(),
        rewrite_superseded: compaction_records
            .iter()
            .map(|(key, _)| key.as_str())
            .filter(|key| rewrite_superseded_keys.contains(*key))
            .collect(),
    };
    for candidate in candidates {
        let key = candidate.0.as_str();
        if selection.losing().contains(key) {
            authority.losing.push(candidate);
        } else if !selection.is_excluded(key) {
            authority.authoritative.push(candidate);
        }
    }
    Ok(authority)
}

/// The most compaction records any one overlap component of a bucket holds,
/// counted after excluding every record a present version 2 record supersedes
/// (ADR-0066 force 2 amendment, item 4). A predecessor and the version 2
/// record that re-encodes it are one record here, not two; a bucket answering
/// more than one holds genuinely contested records and is not re-encoded,
/// because a new record's hash could lose the tie-break to the old loser.
/// A cycle or an over-deep chain of version 2 records, or a version 2 record
/// whose inputs differ from the record it names, is an
/// [`MaintainError::Invariant`] naming the selector's typed error.
pub fn largest_overlap_component(records: &[(String, CompactionRecord)]) -> Result<usize> {
    select_authoritative_compaction_records(records)
        .map(|selection| selection.largest_component())
        .map_err(|err| {
            MaintainError::Invariant(format!(
                "compaction records have an unresolvable supersession chain: {err}"
            ))
        })
}

/// The commit keys of `listing`'s L0 records that its compaction and rewrite
/// records leave served RAW, in listing order: the ones `Catalog::resolve`
/// still returns as L0 segments because no authoritative record covers them.
///
/// This is the walk's eligibility question, asked with the same predicate
/// [`count_below_target`] verifies with, rather than "does this bucket carry a
/// compaction record at all". The two differ exactly where two overlapping
/// compaction records resolve to a winner and a loser
/// ([`select_authoritative_compaction_records`]): an input only the loser names
/// is served raw, so a bucket carrying compaction records can still hold live,
/// un-migrated L0. Skipping the whole bucket on record presence hides that
/// record from the walk while the re-audit counts it, which is a migration
/// that can never converge; counting it in neither is the false floor raise.
///
/// A bucket with no compaction or rewrite record answers from the listing
/// alone, so the ordinary path pays no extra store read; a bucket that carries
/// records pays one GET per record, which is what deciding authority requires.
/// Whether a rewrite refusal on this bucket is permanent, asked against
/// CURRENT state rather than inferred from the refusal.
///
/// `AlreadyCompacted` and `RewritePresent` are returned on nothing more than
/// the bucket carrying a record when the rewrite re-lists, so the outcome
/// alone cannot separate a permanent overlap from a concurrent compaction or
/// erasure that landed underneath the walk and converges on a later run.
///
/// Permanence needs BOTH halves and neither implies the other:
///
/// - a record actually READ, not merely listed. If retention removed every
///   record in the meantime, `raw_served_commit_keys` returns every commit as
///   served raw, which is indistinguishable from the overlap case by inputs
///   alone -- yet that bucket migrates on the next run, because the rewrite
///   finds nothing to refuse on. A listing does not settle this: a record can
///   go between the list and the GET, and the helper skips a vanished record
///   rather than failing the walk, so `records_read` is the only honest
///   evidence that a refusal cause survived;
/// - an input that record leaves served raw and below the target. A bucket
///   whose records supersede everything is refused for reasons that do not
///   block the floor.
async fn refusal_is_permanent(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    config: &CompactorConfig,
    target_version: u32,
) -> Result<bool> {
    let fresh = list_bucket(store, bucket).await?;
    let served = raw_served_commit_keys(store, bucket, &fresh).await?;
    if served.records_read == 0 {
        return Ok(false);
    }
    Ok(
        load_inputs(store, bucket, &served.keys, config.input_read_concurrency)
            .await?
            .iter()
            .any(|i| i.record.segment_format_version < target_version),
    )
}

/// What [`raw_served_commit_keys`] resolved, plus how many records it actually
/// READ.
///
/// The read count is not a statistic. A listing is not evidence that a record
/// still exists: retention can delete one between the list and the GET, and
/// this helper skips a vanished record rather than failing the walk. So a
/// caller asking "is a record still here to refuse the next rewrite" must ask
/// this, not the listing it passed in.
#[derive(Debug)]
struct RawServed {
    keys: Vec<String>,
    records_read: usize,
    /// The compaction records read, by key, for the walk's force 2 question.
    compaction_records: Vec<(String, CompactionRecord)>,
}

async fn raw_served_commit_keys(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    listing: &BucketListing,
) -> Result<RawServed> {
    use prost::Message;

    if listing.compaction_record_keys.is_empty() && listing.rewrite_record_keys.is_empty() {
        return Ok(RawServed {
            keys: listing.commit_keys.clone(),
            records_read: 0,
            compaction_records: Vec::new(),
        });
    }
    let mut records_read = 0usize;

    let mut compaction_records: Vec<(String, CompactionRecord)> =
        Vec::with_capacity(listing.compaction_record_keys.len());
    for key in &listing.compaction_record_keys {
        // Retention can delete a record between the listing and this read.
        // Propagating NotFound would abort the whole walk before the cursor
        // advances, losing a long migration's progress to an unrelated
        // concurrent pass; the rest of this crate already treats a vanished
        // object as absent rather than as an error. Skipping is also the
        // fail-safe direction here: a record that is gone supersedes nothing,
        // so its inputs stay counted as served raw and the floor raise is
        // refused rather than wrongly allowed.
        let got = match store.get(key, GetRange::Full).await {
            Ok(got) => got,
            Err(ravel_object_store::StoreError::NotFound) => continue,
            Err(err) => return Err(err.into()),
        };
        let rec = record::decode_compaction(got.data.as_ref()).map_err(|err| {
            MaintainError::Invariant(format!(
                "compaction record {key} is corrupt during the migrate walk: {err}"
            ))
        })?;
        records_read += 1;
        compaction_records.push((key.clone(), rec));
    }

    // A rewrite record has no overlapping peer to lose to (ADR-0064 decision 3
    // point 5 keeps one record set per bucket), so its whole input list
    // supersedes, exactly as the re-audit treats it.
    let mut rewrite_records: Vec<(String, RewriteRecord)> =
        Vec::with_capacity(listing.rewrite_record_keys.len());
    for key in &listing.rewrite_record_keys {
        // Same race and same reasoning as the compaction records above.
        let got = match store.get(key, GetRange::Full).await {
            Ok(got) => got,
            Err(ravel_object_store::StoreError::NotFound) => continue,
            Err(err) => return Err(err.into()),
        };
        let rec = RewriteRecord::decode(got.data.as_ref()).map_err(|err| {
            MaintainError::Invariant(format!(
                "rewrite record {key} is corrupt during the migrate walk: {err}"
            ))
        })?;
        records_read += 1;
        rewrite_records.push((key.clone(), rec));
    }

    let mut superseded: HashSet<String> = HashSet::new();
    for (_, rec) in authoritative_compaction_records(bucket, &compaction_records, &rewrite_records)?
        .authoritative
    {
        superseded.extend(superseded_input_commit_keys(
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
            rec,
        )?);
    }
    for (_, rec) in &rewrite_records {
        superseded.extend(superseded_input_commit_keys(
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
            rec,
        )?);
    }

    Ok(RawServed {
        keys: listing
            .commit_keys
            .iter()
            .filter(|key| !superseded.contains(*key))
            .cloned()
            .collect(),
        records_read,
        compaction_records,
    })
}

/// The walk's answer for a bucket that serves no below-target L0 record raw
/// and holds no rewrite record (ADR-0066 force 2).
#[derive(Debug)]
enum Force2 {
    /// The bucket is not re-encoded, for this reason.
    Blocked(ReencodeBlockedReason),
    /// One compaction record survives supersession and `below_target` of its
    /// parts are below the target; it names `inputs` L0 records.
    Reencode { below_target: usize, inputs: u64 },
}

/// Whether `records`, a bucket's compaction records, hold the bucket below
/// `target_version` through the parts of its authoritative records, and if so
/// whether force 2 can re-encode it, asked with the shared selector exactly as
/// [`reencode_compaction_parts`] asks it. `None` when no authoritative part is
/// below the target: a bucket whose overlap losers alone are below it is the
/// re-audit's [`BlockedReason::LosingRecordParts`].
///
/// A part counts as below the target only when it is also below the version
/// the current writer emits for the signal, since a re-encode can carry it no
/// further.
fn force2_case(
    signal: Signal,
    records: &[(String, CompactionRecord)],
    target_version: u32,
) -> Result<Option<Force2>> {
    let Ok(current) = current_part_version(signal) else {
        return Ok(None);
    };
    let threshold = target_version.min(current);
    let below = |rec: &CompactionRecord| {
        rec.parts
            .iter()
            .filter(|p| p.segment_format_version < threshold)
            .count()
    };
    let selection = select_authoritative_compaction_records(records).map_err(|err| {
        MaintainError::Invariant(format!(
            "compaction records have an unresolvable supersession chain: {err}"
        ))
    })?;
    let live: Vec<&CompactionRecord> = records
        .iter()
        .filter(|(key, _)| !selection.is_excluded(key))
        .map(|(_, rec)| rec)
        .collect();
    if live.iter().all(|rec| below(rec) == 0) {
        return Ok(None);
    }
    if selection.largest_component() > 1 {
        return Ok(Some(Force2::Blocked(
            ReencodeBlockedReason::ContestedOverlap {
                largest_component: selection.largest_component(),
            },
        )));
    }
    if live.len() > 1 {
        return Ok(Some(Force2::Blocked(
            ReencodeBlockedReason::MultipleRecords {
                records: live.len(),
            },
        )));
    }
    let [only] = live.as_slice() else {
        return Ok(None);
    };
    Ok(Some(Force2::Reencode {
        below_target: below(only),
        inputs: only.inputs.len() as u64,
    }))
}

/// Record what [`reencode_compaction_parts`] did to the bucket at
/// `(shard, ingest_hour)` on `report`, and return the budget it spent. Only a
/// version 2 record this run published counts as migrated; the format floor
/// moves only through the fresh re-audit.
fn record_reencode(
    report: &mut FamilyMigrateReport,
    shard: u32,
    ingest_hour: u32,
    below_target: usize,
    inputs: u64,
    outcome: ReencodeOutcome,
) -> u64 {
    let blocked = |report: &mut FamilyMigrateReport, reason: ReencodeBlockedReason| {
        report.reencode_blocked.push(ReencodeBlockedBucket {
            shard,
            ingest_hour,
            reason,
        });
    };
    let not_migrated = |report: &mut FamilyMigrateReport, reason: NotMigratedReason| {
        report.not_migrated.push(NotMigratedBucket {
            shard,
            ingest_hour,
            path: MigrationPath::Reencode,
            reason,
        });
    };
    match outcome {
        ReencodeOutcome::Reencoded {
            publish: PublishOutcome::Published,
            ..
        } => {
            report.buckets_migrated += 1;
            report.records_migrated += inputs;
            inputs
        }
        // A racing run published the same version 2 record: the bucket
        // converged, but not by this run.
        ReencodeOutcome::Reencoded {
            publish: PublishOutcome::Converged { .. },
            ..
        } => inputs,
        ReencodeOutcome::Reencoded {
            publish: PublishOutcome::Abandoned,
            ..
        } => {
            not_migrated(report, NotMigratedReason::PublishAbandoned);
            inputs
        }
        ReencodeOutcome::RecordSetChanged => {
            not_migrated(report, NotMigratedReason::RecordSetChanged);
            inputs
        }
        ReencodeOutcome::SkippedClaimed { reason } => {
            not_migrated(report, NotMigratedReason::ClaimSkipped { reason });
            0
        }
        ReencodeOutcome::Cancelled { at } => {
            not_migrated(report, NotMigratedReason::Cancelled { at });
            0
        }
        ReencodeOutcome::WriterDisabled => {
            blocked(
                report,
                ReencodeBlockedReason::WriterDisabled { below_target },
            );
            0
        }
        ReencodeOutcome::ContestedOverlap { largest_component } => {
            blocked(
                report,
                ReencodeBlockedReason::ContestedOverlap { largest_component },
            );
            0
        }
        ReencodeOutcome::MultipleRecords { records } => {
            blocked(report, ReencodeBlockedReason::MultipleRecords { records });
            0
        }
        // A rewrite record landed after the walk read the bucket: the re-audit
        // names it as `RewriteParts` when its parts are below the target
        // (ADR-1331). The rest changed under the walk and leave nothing to
        // re-encode; the re-audit counts whatever remains.
        ReencodeOutcome::RewritePresent
        | ReencodeOutcome::Tombstoned
        | ReencodeOutcome::NoCompactionRecord
        | ReencodeOutcome::UpToDate => 0,
    }
}

/// Migrate one `(tenant, signal, family)` toward `target_version`, resuming from
/// the durable cursor, bounded by `budget`, and -- once the walk drains --
/// verifying fresh and raising the format floor.
///
/// One invocation:
///
/// 1. resolves the generation-aware shard range (fail-closed) and reads the
///    advisory cursor;
/// 2. walks buckets in `(shard, ingest_hour)` order, skipping everything at or
///    below the cursor, and for each sealed, un-tombstoned bucket that still
///    serves a below-target L0 record raw ([`raw_served_commit_keys`], not the
///    presence of a compaction record), rewrites its whole live L0 set to the
///    target via [`migrate_bucket_format`] (the rewrite primitive), advancing
///    the cursor past every examined bucket. A bucket the primitive refuses
///    because it already carries a record set, and whose refusal cause survives
///    a re-read, is named in [`FamilyMigrateReport::blocked_buckets`]. A sealed,
///    un-tombstoned bucket that serves nothing below the target raw, holds no
///    rewrite record, and whose authoritative compaction records carry parts
///    below it is ADR-0066 force 2's: when exactly one compaction record
///    survives supersession it is re-encoded through
///    [`reencode_compaction_parts`] (a no-op reported as blocked while
///    [`CompactorConfig::reencode_writer_enabled`] is off), and otherwise it is
///    named in [`FamilyMigrateReport::reencode_blocked`]. A dispatched rewrite
///    that published nothing is named in [`FamilyMigrateReport::not_migrated`];
/// 3. stops early once `budget` is spent (persisting the cursor and returning
///    with `walk_complete == false` so the caller re-invokes), or runs the
///    verify-and-raise step once the walk reaches its end within budget;
/// 4. in the verify step, re-audits fresh via [`count_below_target`] and raises
///    the floor to `target_version` (via [`ravel_catalog::raise_format_floor`],
///    `raised_by` recorded on the entry) only if zero records remain below the
///    target; otherwise leaves the floor untouched and reports the stragglers.
///
/// `raise_format_floor` refuses a raise that is not strictly above the current
/// floor, so if the floor is already at or above `target_version` (a redundant
/// run, or two migrators racing the final raise) a clean re-audit still reports
/// [`Verification::FloorRaised`] with the existing floor rather than surfacing
/// that refusal as an error.
///
/// This is the entry point `ravel-cli maintain migrate` calls (the server
/// maintain loop does not run migrations); it takes only a store, an injected
/// clock, and plain parameters, matching the per-`(tenant, signal)` shape of
/// the crate's other maintenance drivers ([`crate::scan::scan_and_maintain`]).
#[allow(clippy::too_many_arguments)]
pub async fn migrate_family(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    tenant_hash: TenantHash,
    signal: Signal,
    family: &str,
    target_version: u32,
    configured_shards: u32,
    budget: MigrateBudget,
    raised_by: &str,
) -> Result<FamilyMigrateReport> {
    let now = clock.now_ns();
    let scan_shards = scan_shard_count(store, &tenant_hash, signal, configured_shards).await?;

    let cursor_key = migrate_cursor_key(&tenant_hash, signal, family);
    let (start_after, cursor_version) = read_cursor(store, &cursor_key).await?;

    let mut report = FamilyMigrateReport::default();
    let mut spent = 0u64;
    let mut position: Option<(u32, u32)> = start_after;

    'walk: for shard in 0..scan_shards {
        let hours = list_shard_hours(store, &tenant_hash, signal, shard).await?;
        for hour in hours {
            // Skip everything at or below the resume position. The walk order
            // is a total order over (shard, hour), so this never skips an
            // unprocessed bucket and never revisits a processed one.
            if start_after.is_some_and(|after| (shard, hour) <= after) {
                continue;
            }

            let bucket = Bucket::new(tenant_hash, signal, shard, hour);
            report.buckets_examined += 1;

            // Discover eligibility exactly as the audit walk does: from the
            // commit records' recorded format version, never a data-object GET.
            // Only a sealed, un-tombstoned bucket that still serves at least
            // one below-target L0 record RAW is a candidate here; a compacted
            // bucket's L1 parts are the rewrite-on-touch scope, and the
            // re-audit below still refuses the floor raise if any such L1
            // straggler survives.
            //
            // "Still served raw" is asked through the shared authority rule
            // ([`raw_served_commit_keys`]), not answered from the presence of a
            // compaction record. A bucket whose overlapping compaction records
            // resolve to a winner and a loser leaves every loser-only input
            // served as a raw L0 segment, so record presence would skip a live
            // below-target record that the re-audit rightly counts.
            let listing = list_bucket(store, &bucket).await?;
            if bucket.is_sealed(now, config) && listing.tombstone_key.is_none() {
                let served = raw_served_commit_keys(store, &bucket, &listing).await?;
                let l0_below = if served.keys.is_empty() {
                    0
                } else {
                    load_inputs(store, &bucket, &served.keys, config.input_read_concurrency)
                        .await?
                        .iter()
                        .filter(|i| i.record.segment_format_version < target_version)
                        .count() as u64
                };
                let not_migrated = |report: &mut FamilyMigrateReport, reason| {
                    report.not_migrated.push(NotMigratedBucket {
                        shard,
                        ingest_hour: hour,
                        path: MigrationPath::L0Migration,
                        reason,
                    });
                };
                if l0_below > 0 {
                    match migrate_bucket_format(store, clock, config, &bucket, target_version)
                        .await?
                    {
                        // The run built its parts and published nothing: the
                        // pre-publish re-list found the record set changed, or
                        // the deadline passed. The re-audit still counts the
                        // bucket's records and a later run retries it.
                        MigrateOutcome::Rewritten {
                            publish: PublishOutcome::Abandoned,
                            ..
                        } => {
                            not_migrated(&mut report, NotMigratedReason::PublishAbandoned);
                            spent += l0_below;
                        }
                        MigrateOutcome::RecordSetChanged => {
                            not_migrated(&mut report, NotMigratedReason::RecordSetChanged);
                            spent += l0_below;
                        }
                        MigrateOutcome::Rewritten { .. } => {
                            report.buckets_migrated += 1;
                            report.records_migrated += l0_below;
                            spent += l0_below;
                        }
                        // The rewrite primitive serves one record set per
                        // bucket, so it refuses a bucket that already carries
                        // compaction or rewrite records. Two different things
                        // reach this arm and only one of them is permanent:
                        // a concurrent compaction or erasure rewrite published
                        // between our listing and the call's own listing or its
                        // pre-publish re-list, and the migration published
                        // nothing (the bucket claim and that re-list are what
                        // stop a compaction record built from unerased inputs
                        // landing beside an erasure rewrite record, which would
                        // serve the erased rows again); or the bucket holds overlapping
                        // compaction records whose loser-only inputs are still
                        // served raw and below the target, which no rewrite can
                        // migrate, because a new record over those inputs joins
                        // the same overlap component and loses to the existing
                        // winner.
                        //
                        // The outcome alone cannot tell them apart: both are
                        // returned on nothing more than the bucket carrying a
                        // record at re-list time. So ask the question again
                        // against current state rather than counting the
                        // refusal. `blocked_buckets` claims the permanent case
                        // on both the CLI and in the guide, and a list that also
                        // named a raced-past bucket would send an operator
                        // looking for an overlap that is not there.
                        MigrateOutcome::AlreadyCompacted | MigrateOutcome::RewritePresent => {
                            if refusal_is_permanent(store, &bucket, config, target_version).await? {
                                report.blocked_buckets.push(BlockedBucket {
                                    shard,
                                    ingest_hour: hour,
                                    reason: BlockedReason::LoserOnlyInputs,
                                });
                            }
                        }
                        // A concurrent tombstone or a bucket already at the
                        // target: nothing to migrate and nothing to report.
                        MigrateOutcome::NotSealed
                        | MigrateOutcome::Tombstoned
                        | MigrateOutcome::UpToDate => {}
                        // Another pass held the bucket's claim, or this one lost
                        // it: nothing was published and the bucket is not
                        // migrated. The fresh re-audit still counts its
                        // below-target records, so the floor stays unraised
                        // until a later invocation migrates it.
                        MigrateOutcome::SkippedClaimed { reason } => {
                            not_migrated(&mut report, NotMigratedReason::ClaimSkipped { reason });
                        }
                        MigrateOutcome::Cancelled { at } => {
                            not_migrated(&mut report, NotMigratedReason::Cancelled { at });
                        }
                    }
                } else if listing.rewrite_record_keys.is_empty() {
                    // ADR-0066 force 2: the bucket serves nothing below the
                    // target raw, so what can hold it there is the parts of
                    // its compaction records. A bucket with a rewrite record
                    // is the re-audit's `RewriteParts` (ADR-1331).
                    match force2_case(bucket.signal, &served.compaction_records, target_version)? {
                        None => {}
                        Some(Force2::Blocked(reason)) => {
                            report.reencode_blocked.push(ReencodeBlockedBucket {
                                shard,
                                ingest_hour: hour,
                                reason,
                            });
                        }
                        Some(Force2::Reencode {
                            below_target,
                            inputs,
                        }) => {
                            let outcome =
                                reencode_compaction_parts(store, clock, config, &bucket).await?;
                            spent += record_reencode(
                                &mut report,
                                shard,
                                hour,
                                below_target,
                                inputs,
                                outcome,
                            );
                        }
                    }
                }
            }

            // The bucket is fully examined; advance the resume position past it.
            position = Some((shard, hour));
            report.cursor_advanced_to = position;

            if budget.is_exhausted(spent) {
                report.budget_exhausted = true;
                break 'walk;
            }
        }
    }

    report.walk_complete = !report.budget_exhausted;

    if !report.walk_complete {
        // Budget exhausted mid-walk: persist the advisory cursor at the last
        // examined bucket (best-effort CAS) and return control; the caller
        // re-invokes and resumes from here.
        if let Some((shard, hour)) = position {
            write_cursor(store, &cursor_key, shard, hour, cursor_version).await?;
        }
        return Ok(report);
    }

    // The walk drained, so there is nothing left to resume: clear the cursor.
    // Leaving it at the final position would strand work, because the skip
    // predicate is lexicographic over `(shard, hour)` -- a cursor at
    // `(last_shard, last_hour)` skips every bucket of every lower shard at any
    // hour. A later invocation could then never migrate a bucket that landed
    // below that position, which is precisely the re-run the straggler path
    // tells the operator to perform. Deleting is idempotent and the cursor is
    // advisory, so losing it only ever costs a rescan.
    report.cursor_advanced_to = None;
    store.delete(&cursor_key).await?;

    // The walk drained within budget. Re-audit FRESH before raising the floor:
    // this is the race close. A record that landed below the target between the
    // walk finishing and now -- including a still-unsealed one the walk could
    // not migrate -- is counted here and refuses the raise, so a floor is never
    // asserted over a stale audit.
    // Re-resolve the shard range too, for the same reason the audit itself is
    // re-run: `scan_shards` was resolved before a walk that can run for a long
    // time, and resharding is online (ADR-0052 section 3 has no quiescence
    // requirement), so a generation appended during the walk is invisible to
    // that stale value. Auditing the old, narrower range would come back clean
    // while stragglers sit in a shard it never listed, and the floor would be
    // raised over an under-scanned audit. The range is a max over an
    // append-only generation list, so re-resolving can only widen it.
    let verify_shards = scan_shard_count(store, &tenant_hash, signal, configured_shards).await?;
    let mut audit =
        count_below_target(store, &tenant_hash, signal, verify_shards, target_version).await?;

    // The permanent cases are found by different passes -- the walk sees a
    // surviving overlap's raw inputs, the re-audit sees a rewrite record's or a
    // losing compaction record's parts -- so the one list an operator reads is
    // their union. A bucket both passes name (partial coverage under a rewrite
    // record, or a loser whose inputs and parts are both below the target) is
    // one blocked bucket, not two: `buckets_blocked` counts buckets.
    //
    // On that collision the re-audit's entry REPLACES the walk's rather than
    // being dropped. Both reasons are true of the bucket and neither is cleared
    // by a re-run, so the line says the same thing about what to do next
    // either way; but only the re-audit's reasons carry a count, and dropping
    // one left the operator with no figure at all for the parts holding that
    // bucket's floor down.
    for blocked in std::mem::take(&mut audit.blocked) {
        match report
            .blocked_buckets
            .iter_mut()
            .find(|seen| (seen.shard, seen.ingest_hour) == (blocked.shard, blocked.ingest_hour))
        {
            Some(seen) => *seen = blocked,
            None => report.blocked_buckets.push(blocked),
        }
    }
    report
        .blocked_buckets
        .sort_by_key(|blocked| (blocked.shard, blocked.ingest_hour));

    if audit.total() > 0 {
        report.verification = Some(Verification::Stragglers {
            l0: audit.l0,
            l1: audit.l1,
            rewrite_parts: audit.rewrite_parts,
            blocked: report.blocked_buckets.clone(),
        });
        return Ok(report);
    }

    // Zero stragglers: raise the floor. If a concurrent run (or a prior one)
    // already raised it to or past the target, `raise_format_floor` would refuse
    // the non-strict raise; treat that as success, since the invariant the floor
    // asserts already holds.
    let current = current_floor_from_store(store, &tenant_hash, signal, family)
        .await
        .map_err(|err| MaintainError::Provisioning(err.to_string()))?;
    match current {
        Some(cur) if cur >= target_version => {
            report.verification = Some(Verification::FloorRaised { floor_version: cur });
        }
        _ => {
            let outcome = ravel_catalog::raise_format_floor(
                store,
                &tenant_hash,
                signal,
                family,
                target_version,
                raised_by,
                now,
            )
            .await
            .map_err(|err| MaintainError::Provisioning(err.to_string()))?;
            report.verification = Some(Verification::FloorRaised {
                floor_version: outcome.floor_version,
            });
        }
    }

    Ok(report)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    //! Exercises the driver end to end on a `MemoryStore` over real v7 RSEG
    //! objects: the resumable cursor and budget (a migration interrupted after
    //! partial progress resumes exactly where it stopped, migrating every bucket
    //! exactly once), the floor-raise race close (a fresh straggler between the
    //! walk and the re-audit refuses the raise), and the basic end-to-end raise
    //! (a drained walk with no stragglers raises the floor and a subsequent
    //! audit finds nothing below it).

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use bytes::Bytes;
    use ravel_commit::keys;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, ListPage, ObjectMeta, ObjectStoreBackend,
        PageToken, PutOptions, PutOutcome,
    };
    use ravel_segment::{
        IngestBounds, SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues, VERSION_V7,
    };
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantHash, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::{CompactorConfig, FixedClock};

    const TENANT: &str = "acme";
    const FAMILY: &str = "rseg";
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;
    /// A version above the current writer's output, so every real v7 object
    /// counts as "below target" and the rewrite path runs. Stands in for the
    /// N-1 version an actual reader window would supply.
    const FUTURE_VERSION: u32 = VERSION_V7 as u32 + 1;

    fn tenant_hash() -> TenantHash {
        TenantId::new(TENANT).hash()
    }

    fn labels(metric: &str) -> LabelSet {
        LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels")
    }

    fn series_id(metric: &str) -> SeriesId {
        SeriesId::compute(&TenantId::new(TENANT), metric, &labels(metric)).expect("series id")
    }

    fn series(metric: &str, samples: &[(i64, f64)]) -> SeriesInputV3 {
        SeriesInputV3 {
            series_id: series_id(metric),
            labels: labels(metric),
            values: SeriesValues::Scalar(
                samples
                    .iter()
                    .map(|(ts_ns, value)| Sample {
                        ts_ns: *ts_ns,
                        value: *value,
                    })
                    .collect(),
            ),
        }
    }

    /// Now-ns at which `hour` (and every earlier hour) is well past the seal
    /// margin under default config, so a bucket at `hour` counts as sealed.
    fn sealed_now_ns_for(hour: u32) -> i64 {
        (i64::from(hour) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
    }

    /// Seed one L0 input (data object + commit record) at `(shard, hour)` via
    /// the production flush writer, recording `segment_format_version` in the
    /// commit record as given. The object bytes are always real v7; only the
    /// metadata a caller reads without decoding can be made to disagree, which
    /// is how a "below target" or "straggler" record is fabricated over a real
    /// object. A distinct `seq` keeps each seeded object's key unique.
    async fn seed_at(
        store: &dyn ObjectStoreBackend,
        shard: u32,
        hour: u32,
        seq: u64,
        metric: &str,
        segment_format_version: u32,
    ) {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(u128::from(seq));
        let created = i64::from(hour) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        let identity = SegmentIdentity {
            tenant_hash: th.0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
        };
        let written = SegmentWriter::write_histograms_with_exemplars(
            vec![series(metric, &[(created, 1.0)])],
            identity,
            bounds,
            Vec::new(),
        )
        .expect("write L0");
        let content_hash = written.summary.blake3;
        let data_key = keys::data_key(
            &th,
            Signal::Metrics,
            shard,
            writer_id,
            EPOCH,
            seq,
            &content_hash,
        )
        .expect("data key");
        store
            .put(&data_key, written.bytes.clone(), PutOptions::default())
            .await
            .expect("put data object");

        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: seq,
            object_size: written.bytes.len() as u64,
            content_hash,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version,
            created_unix_ns: created,
            ingest_hour_bucket: hour,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
    }

    /// Count compaction records physically present under one `(shard, hour)`
    /// bucket: proof a bucket was (or was not) migrated, and that it was
    /// migrated exactly once.
    async fn compaction_record_count(
        store: &dyn ObjectStoreBackend,
        shard: u32,
        hour: u32,
    ) -> usize {
        let bucket = Bucket::new(tenant_hash(), Signal::Metrics, shard, hour);
        list_bucket(store, &bucket)
            .await
            .expect("list bucket")
            .compaction_record_keys
            .len()
    }

    /// Provision the tenant/signal so a floor can be raised: `raise_format_floor`
    /// requires an existing provisioning record.
    async fn provision(store: &dyn ObjectStoreBackend, shards: u32) {
        ravel_catalog::validate_or_adopt(
            store,
            &tenant_hash(),
            Signal::Metrics,
            shards,
            0,
            ravel_catalog::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision tenant/signal");
    }

    /// Resumability: a migration interrupted after partial progress via the
    /// budget resumes exactly where it stopped and completes, with every bucket
    /// migrated exactly once -- no duplicate, no skip. Three sealed single-record
    /// buckets across two shards, a one-record budget, driven one invocation at
    /// a time until the walk completes.
    #[tokio::test]
    async fn budget_interrupts_and_resume_covers_every_bucket_exactly_once() {
        let store = MemoryStore::new();
        provision(&store, 2).await;
        // Shard 0 hours 100, 101; shard 1 hour 100. All real v7 objects.
        seed_at(&store, 0, 100, 1, "alpha", VERSION_V7 as u32).await;
        seed_at(&store, 0, 101, 2, "beta", VERSION_V7 as u32).await;
        seed_at(&store, 1, 100, 3, "gamma", VERSION_V7 as u32).await;

        let clock = FixedClock::new(sealed_now_ns_for(101));
        let config = CompactorConfig::default();
        let budget = MigrateBudget::records(1);

        let mut invocations = 0usize;
        let mut total_migrated = 0usize;
        let mut interrupted_at_least_once = false;
        loop {
            let report = migrate_family(
                &store,
                &clock,
                &config,
                tenant_hash(),
                Signal::Metrics,
                FAMILY,
                FUTURE_VERSION,
                2,
                budget,
                "test",
            )
            .await
            .expect("migrate invocation");
            invocations += 1;
            total_migrated += report.buckets_migrated;
            if report.budget_exhausted {
                interrupted_at_least_once = true;
                assert!(
                    !report.walk_complete,
                    "an exhausted budget did not complete"
                );
            }
            if report.walk_complete {
                break;
            }
            assert!(invocations < 10, "resume loop failed to converge");
        }

        assert!(
            interrupted_at_least_once,
            "a one-record budget over three buckets must interrupt at least once"
        );
        assert_eq!(
            total_migrated, 3,
            "every seeded bucket migrated exactly once across the resumed invocations"
        );
        // Each bucket carries exactly one compaction record: migrated once,
        // never duplicated, never skipped.
        assert_eq!(compaction_record_count(&store, 0, 100).await, 1);
        assert_eq!(compaction_record_count(&store, 0, 101).await, 1);
        assert_eq!(compaction_record_count(&store, 1, 100).await, 1);
    }

    /// Race safety: the floor raise refuses
    /// when a fresh below-target straggler is present at re-audit time that the
    /// walk did not migrate. Everything sealed is already at the target, so the
    /// walk drains with nothing to rewrite and would raise the floor -- but an
    /// unsealed below-target record (data that just landed, too fresh to seal
    /// and migrate) is counted by the fresh re-audit and blocks the raise. A
    /// floor CAS-appended here would be a false assertion.
    #[tokio::test]
    async fn floor_raise_refuses_when_a_fresh_straggler_survives_the_walk() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        let target = VERSION_V7 as u32;

        // A sealed, at-target bucket: the walk confirms it, nothing to rewrite.
        seed_at(&store, 0, 100, 1, "alpha", target).await;
        // A straggler recorded below the target in a still-unsealed recent hour:
        // the walk examines but cannot migrate it (unsealed), the fresh re-audit
        // counts it.
        let now = sealed_now_ns_for(100);
        let unsealed_hour = u32::try_from(now.div_euclid(NS_PER_HOUR)).expect("hour fits");
        seed_at(&store, 0, unsealed_hour, 2, "beta", target - 1).await;

        let clock = FixedClock::new(now);
        let report = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            target,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate");

        assert!(report.walk_complete, "the walk drained within budget");
        assert!(
            report.stragglers_found(),
            "the fresh straggler must refuse the raise, got {:?}",
            report.verification
        );
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 1,
                l1: 0,
                rewrite_parts: 0,
                blocked: Vec::new()
            }),
            "exactly the one below-target straggler is reported, and it is not a blocked \
             bucket: an unsealed record migrates on a later run"
        );
        // The floor was NOT raised: no floor was ever recorded for the family.
        let floor = current_floor_from_store(&store, &tenant_hash(), Signal::Metrics, FAMILY)
            .await
            .expect("read floor");
        assert_eq!(floor, None, "a refused verify raises no floor");
    }

    /// End to end: with every record at the target and no straggler, a drained
    /// walk raises the floor, and a subsequent fresh audit finds nothing below
    /// the new floor.
    #[tokio::test]
    async fn drained_walk_with_no_stragglers_raises_the_floor() {
        let store = MemoryStore::new();
        provision(&store, 2).await;
        let target = VERSION_V7 as u32;
        seed_at(&store, 0, 100, 1, "alpha", target).await;
        seed_at(&store, 0, 101, 2, "beta", target).await;
        seed_at(&store, 1, 100, 3, "gamma", target).await;

        let clock = FixedClock::new(sealed_now_ns_for(101));
        let report = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            target,
            2,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate");

        assert!(report.walk_complete);
        assert!(
            !report.stragglers_found(),
            "no stragglers, got {:?}",
            report.verification
        );
        assert_eq!(
            report.verification,
            Some(Verification::FloorRaised {
                floor_version: target
            }),
            "a clean re-audit raises the floor to the target"
        );

        // The floor is durably recorded at the target.
        let floor = current_floor_from_store(&store, &tenant_hash(), Signal::Metrics, FAMILY)
            .await
            .expect("read floor");
        assert_eq!(floor, Some(target));

        // A fresh audit finds nothing below the new floor.
        let audit = count_below_target(&store, &tenant_hash(), Signal::Metrics, 2, target)
            .await
            .expect("re-audit");
        assert_eq!(audit, BelowTargetReport::default());
    }

    /// Slice B end to end (ADR-0066 decision 5): a below-output-recorded RSEG
    /// input migrates to the current version and the format floor is then
    /// raised. Before slice B this was unreachable for RSEG -- its rewrite
    /// could only re-publish at the current writer version, so reaching
    /// "below target" eligibility required a target *above* the writer
    /// ([`FUTURE_VERSION`]), which then made the rewrite's own output count as
    /// below target and blocked the floor. Now, targeting the current version,
    /// an input recorded at `VERSION_V7 - 1` (real v7 bytes, older recorded
    /// version -- the synthetic-N-1 shape, since no real N-1 RSEG version has
    /// shipped) is decoded and re-encoded to `VERSION_V7 == target`, the fresh
    /// re-audit finds nothing below the target, and the floor is raised to
    /// `VERSION_V7`.
    #[tokio::test]
    async fn rseg_below_output_input_migrates_and_raises_floor() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        let target = VERSION_V7 as u32;
        // Real v7 bytes recorded below the target: eligible for the walk, and now
        // genuinely rewritable up to the target rather than refused.
        seed_at(&store, 0, 100, 1, "alpha", target - 1).await;

        let clock = FixedClock::new(sealed_now_ns_for(100));
        let report = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            target,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate");

        assert!(report.walk_complete, "the walk drained within budget");
        assert_eq!(
            report.buckets_migrated, 1,
            "the below-target bucket was rewritten"
        );
        assert!(
            !report.stragglers_found(),
            "the rewrite lifted the only input to the target, so no straggler remains, got {:?}",
            report.verification
        );
        assert_eq!(
            report.verification,
            Some(Verification::FloorRaised {
                floor_version: target
            }),
            "a clean re-audit raises the floor to the current version"
        );

        // The floor is durably recorded at the target, and a fresh audit finds
        // nothing below it: the pre-rewrite L0 record is superseded (excluded)
        // and the rewritten L1 part is at the target.
        let floor = current_floor_from_store(&store, &tenant_hash(), Signal::Metrics, FAMILY)
            .await
            .expect("read floor");
        assert_eq!(floor, Some(target));
        let audit = count_below_target(&store, &tenant_hash(), Signal::Metrics, 1, target)
            .await
            .expect("re-audit");
        assert_eq!(audit, BelowTargetReport::default());
    }

    /// The L1 part keys the migration published for one `(shard, hour)` bucket,
    /// read out of its compaction record, so the test can open the rewrite's own
    /// output through the production RSEG reader rather than a copy of it.
    async fn migrated_part_keys(
        store: &dyn ObjectStoreBackend,
        shard: u32,
        hour: u32,
    ) -> Vec<String> {
        let bucket = Bucket::new(tenant_hash(), Signal::Metrics, shard, hour);
        let listing = list_bucket(store, &bucket).await.expect("list bucket");
        let mut parts = Vec::new();
        for key in &listing.compaction_record_keys {
            let got = store
                .get(key, GetRange::Full)
                .await
                .expect("get compaction record");
            let rec =
                record::decode_compaction(got.data.as_ref()).expect("decode compaction record");
            for part in &rec.parts {
                parts
                    .push(keys::reconstruct_l1_part_key(&rec, part).expect("reconstruct part key"));
            }
        }
        parts
    }

    /// The two #530 fix-shape bullets that had not landed (issue #1775): one
    /// migration exercised end to end, and the ordering guarantee that a format
    /// floor rises only once *every* bucket has converted. Three sealed buckets
    /// across two shards are recorded one below the target over real v7 bytes
    /// (the synthetic-N-1 record shape the other migrate tests use, since no real
    /// N-1 RSEG *object* version has shipped -- ADR-0092 decision 7 keeps the
    /// reader window single-version, so "below target" is a commit-record fact
    /// over a genuine current-version object, not an older trailer). Driven one
    /// bucket at a time with a one-record budget: the floor stays unraised on
    /// every partial invocation and is raised to the target only on the
    /// invocation whose walk drains, i.e. strictly after the last bucket
    /// converted. Every migrated bucket's published L1 output is then opened
    /// through the production RSEG reader and admitted at the target version.
    #[tokio::test]
    async fn floor_rises_only_after_every_bucket_converts_and_outputs_read_at_the_target() {
        let store = MemoryStore::new();
        provision(&store, 2).await;
        let target = VERSION_V7 as u32;
        let buckets = [(0u32, 100u32), (0, 101), (1, 100)];
        seed_at(&store, 0, 100, 1, "alpha", target - 1).await;
        seed_at(&store, 0, 101, 2, "beta", target - 1).await;
        seed_at(&store, 1, 100, 3, "gamma", target - 1).await;

        let clock = FixedClock::new(sealed_now_ns_for(101));
        let config = CompactorConfig::default();
        let budget = MigrateBudget::records(1);

        let mut invocations = 0usize;
        let mut buckets_migrated = 0usize;
        let mut partial_invocations = 0usize;
        loop {
            let report = migrate_family(
                &store,
                &clock,
                &config,
                tenant_hash(),
                Signal::Metrics,
                FAMILY,
                target,
                2,
                budget,
                "test",
            )
            .await
            .expect("migrate invocation");
            invocations += 1;
            buckets_migrated += report.buckets_migrated;

            let floor = current_floor_from_store(&store, &tenant_hash(), Signal::Metrics, FAMILY)
                .await
                .expect("read floor");
            if report.walk_complete {
                assert_eq!(
                    floor,
                    Some(target),
                    "the drained walk raised the floor to the target"
                );
                assert_eq!(
                    report.verification,
                    Some(Verification::FloorRaised {
                        floor_version: target
                    }),
                );
                break;
            }
            // The floor rises only on the invocation whose walk drains. On the
            // last partial pass every bucket has already converted and none is
            // below the target, so the reason the floor is still unraised is
            // the undrained walk, not a straggler.
            partial_invocations += 1;
            assert_eq!(
                floor, None,
                "the floor rises only once the walk drains, never on a partial invocation"
            );
            assert!(invocations < 10, "resume loop failed to converge");
        }

        assert!(
            partial_invocations >= 1,
            "a one-record budget over three buckets must interrupt before draining"
        );
        assert_eq!(
            buckets_migrated, 3,
            "every seeded bucket converted exactly once"
        );

        // Record axis: nothing is left below the target for the family.
        let audit = count_below_target(&store, &tenant_hash(), Signal::Metrics, 2, target)
            .await
            .expect("re-audit");
        assert_eq!(audit, BelowTargetReport::default());

        // Reader axis: every migrated bucket's own published output opens through
        // the production RSEG reader and is admitted at the target version.
        let mut outputs_read = 0usize;
        for (shard, hour) in buckets {
            let part_keys = migrated_part_keys(&store, shard, hour).await;
            assert!(
                !part_keys.is_empty(),
                "bucket ({shard},{hour}) published an L1 part"
            );
            for key in part_keys {
                let got = store
                    .get(&key, GetRange::Full)
                    .await
                    .expect("get migrated L1 part");
                let loc = ravel_segment::open_from_full(
                    got.data.as_ref(),
                    ravel_segment::ReaderLimits::default(),
                )
                .expect("migration output is a readable RSEG segment");
                // Entailed by the expect above while WINDOW holds one version:
                // open_from_full refuses anything outside it. Kept for the day
                // the window widens, when it starts pinning that the migration
                // published at the target rather than at some other admitted
                // version.
                assert_eq!(
                    u32::from(loc.version),
                    target,
                    "the migrated object is admitted at the target version"
                );
                outputs_read += 1;
            }
        }
        assert!(
            outputs_read >= 3,
            "each converted bucket contributed at least one readable output"
        );
    }

    /// What an [`InjectingStore`] writes when it fires, i.e. what "lands"
    /// during the window between the walk finishing and the re-audit reading.
    #[derive(Debug, Clone, Copy)]
    enum Injection {
        /// A below-target commit record in the already-walked shard 0.
        Straggler,
        /// An online reshard (ADR-0052) widening the tenant to two shards, plus
        /// a below-target commit record in the newly added shard 1.
        ReshardWithStragglerInNewShard,
    }

    /// When an [`InjectingStore`] fires, expressed as a listing call the driver
    /// makes at a known point. The two listing methods separate the two phases
    /// cleanly: the walk reaches for `list_delimited` (its per-shard hour
    /// listing) and for `list` on the strictly longer per-hour bucket prefix,
    /// while `count_below_target` is the only caller that `list`s exactly the
    /// commit *shard* prefix.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Trigger {
        /// The first `list_delimited` of the shard prefix: the walk's own hour
        /// listing, so the write lands while the walk is still running.
        DuringWalk,
        /// The first `list` of exactly the shard prefix: the opening read of
        /// the verification re-audit, so the write lands strictly after the
        /// walk finished and before the re-audit has read anything.
        AfterWalk,
    }

    /// A store decorator that performs `injection` exactly once, immediately
    /// before the listing call named by `trigger` is served. This gives a
    /// genuine interleaving rather than a pre-seeded stand-in: the injected
    /// state does not exist while the earlier phase runs.
    struct InjectingStore {
        inner: Arc<MemoryStore>,
        trigger_prefix: String,
        trigger: Trigger,
        injection: Injection,
        fired: AtomicBool,
    }

    impl InjectingStore {
        fn new(
            inner: Arc<MemoryStore>,
            trigger_prefix: String,
            trigger: Trigger,
            injection: Injection,
        ) -> Self {
            InjectingStore {
                inner,
                trigger_prefix,
                trigger,
                injection,
                fired: AtomicBool::new(false),
            }
        }

        /// Run the injection if `op` is the configured trigger, `prefix`
        /// matches, and it has not already fired.
        async fn maybe_fire(&self, op: Trigger, prefix: &str) {
            if op != self.trigger
                || prefix != self.trigger_prefix
                || self.fired.swap(true, Ordering::SeqCst)
            {
                return;
            }
            let inner = self.inner.as_ref();
            match self.injection {
                Injection::Straggler => {
                    seed_at(inner, 0, 105, 42, "straggler", VERSION_V7 as u32 - 1).await;
                }
                Injection::ReshardWithStragglerInNewShard => {
                    ravel_catalog::append_generation(
                        inner,
                        &tenant_hash(),
                        Signal::Metrics,
                        2,
                        1,
                        0,
                    )
                    .await
                    .expect("append shard generation mid-flight");
                    seed_at(inner, 1, 105, 43, "straggler", VERSION_V7 as u32 - 1).await;
                }
            }
        }

        /// Whether the injection actually ran. Asserted by every test using
        /// this decorator, so a trigger prefix that stopped matching (a key
        /// layout change, a different list shape) fails loudly instead of
        /// turning the test vacuous.
        fn fired(&self) -> bool {
            self.fired.load(Ordering::SeqCst)
        }
    }

    /// The object-store trait's own result type. Spelled out because the
    /// crate's `Result` alias is in scope here via `use super::*`.
    type StoreResult<T> = std::result::Result<T, StoreError>;

    #[async_trait::async_trait]
    impl ObjectStoreBackend for InjectingStore {
        async fn put(&self, key: &str, data: Bytes, opts: PutOptions) -> StoreResult<PutOutcome> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> StoreResult<GetOutcome> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> StoreResult<ObjectMeta> {
            self.inner.head(key).await
        }

        async fn list(&self, prefix: &str, page: Option<PageToken>) -> StoreResult<ListPage> {
            self.maybe_fire(Trigger::AfterWalk, prefix).await;
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> StoreResult<DelimitedList> {
            self.maybe_fire(Trigger::DuringWalk, prefix).await;
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> StoreResult<()> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            // multipart: false to match the refusing default `put_multipart`
            // this double inherits; the corpora here are tiny.
            Capabilities {
                multipart: false,
                ..self.inner.capabilities()
            }
        }
    }

    /// Race safety, the real interleaving.
    /// Unlike the pre-seeded variant above, the straggler here does not exist
    /// while the walk runs: it is written by the store decorator at the instant
    /// the verification re-audit issues its first list, so it lands strictly
    /// between "the walk finished" and "the re-audit read anything". The floor
    /// must not be raised. This is the case where a cached or walk-derived
    /// enumeration would come back clean and CAS-append a false floor.
    #[tokio::test]
    async fn a_straggler_landing_after_the_walk_refuses_the_floor_raise() {
        let inner = Arc::new(MemoryStore::new());
        provision(inner.as_ref(), 1).await;
        let target = VERSION_V7 as u32;
        seed_at(inner.as_ref(), 0, 100, 1, "alpha", target).await;

        let trigger =
            keys::commit_shard_prefix(&tenant_hash(), Signal::Metrics, 0).expect("shard prefix");
        let store = InjectingStore::new(
            Arc::clone(&inner),
            trigger,
            Trigger::AfterWalk,
            Injection::Straggler,
        );

        let clock = FixedClock::new(sealed_now_ns_for(100));
        let report = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            target,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate");

        assert!(
            store.fired(),
            "the injection never ran; the test is vacuous"
        );
        assert!(report.walk_complete, "the walk drained within budget");
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 1,
                l1: 0,
                rewrite_parts: 0,
                blocked: Vec::new()
            }),
            "a record that landed after the walk must refuse the raise, and it is not a \
             blocked bucket: the next run migrates it"
        );
        let floor =
            current_floor_from_store(inner.as_ref(), &tenant_hash(), Signal::Metrics, FAMILY)
                .await
                .expect("read floor");
        assert_eq!(floor, None, "a refused verify raises no floor");
    }

    /// The verification re-audit must resolve its own shard range, not reuse
    /// the one resolved before the walk. Resharding is online (ADR-0052), so a
    /// generation can be appended while a walk is in flight; auditing the stale
    /// narrower range would come back clean while stragglers sit in a shard it
    /// never listed, and the floor would be raised over an under-scanned audit.
    #[tokio::test]
    async fn a_reshard_during_the_walk_widens_the_verification_audit() {
        let inner = Arc::new(MemoryStore::new());
        provision(inner.as_ref(), 1).await;
        let target = VERSION_V7 as u32;
        seed_at(inner.as_ref(), 0, 100, 1, "alpha", target).await;

        let trigger =
            keys::commit_shard_prefix(&tenant_hash(), Signal::Metrics, 0).expect("shard prefix");
        let store = InjectingStore::new(
            Arc::clone(&inner),
            trigger,
            Trigger::DuringWalk,
            Injection::ReshardWithStragglerInNewShard,
        );

        let clock = FixedClock::new(sealed_now_ns_for(100));
        let report = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            target,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate");

        assert!(
            store.fired(),
            "the injection never ran; the test is vacuous"
        );
        assert!(report.walk_complete, "the walk drained within budget");
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 1,
                l1: 0,
                rewrite_parts: 0,
                blocked: Vec::new()
            }),
            "the straggler in the shard added mid-walk must refuse the raise"
        );
        let floor =
            current_floor_from_store(inner.as_ref(), &tenant_hash(), Signal::Metrics, FAMILY)
                .await
                .expect("read floor");
        assert_eq!(
            floor, None,
            "no floor may be raised over an audit that never listed the new shard"
        );
    }

    /// A completed walk must not strand later work. The skip predicate is
    /// lexicographic over `(shard, hour)`, so a cursor left at the last bucket
    /// of the last shard would skip every bucket of every lower shard at any
    /// hour, forever. That is exactly the re-run the straggler path instructs
    /// the operator to perform, so a surviving cursor makes the documented
    /// remediation unable to converge.
    #[tokio::test]
    async fn a_completed_walk_does_not_strand_later_buckets_in_lower_shards() {
        let store = MemoryStore::new();
        provision(&store, 2).await;
        seed_at(&store, 0, 100, 1, "alpha", VERSION_V7 as u32).await;
        seed_at(&store, 1, 100, 2, "beta", VERSION_V7 as u32).await;

        let config = CompactorConfig::default();
        let first = migrate_family(
            &store,
            &FixedClock::new(sealed_now_ns_for(101)),
            &config,
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            FUTURE_VERSION,
            2,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("first migrate");
        assert!(first.walk_complete, "the first walk drained");
        assert_eq!(first.buckets_migrated, 2, "both seeded buckets migrated");

        // A new sealed bucket lands in shard 0, lexicographically *below* the
        // position the completed walk ended at (shard 1, hour 100).
        seed_at(&store, 0, 200, 3, "gamma", VERSION_V7 as u32).await;

        let second = migrate_family(
            &store,
            &FixedClock::new(sealed_now_ns_for(201)),
            &config,
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            FUTURE_VERSION,
            2,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("second migrate");

        assert_eq!(
            second.buckets_migrated, 1,
            "a later run must still reach a bucket in a lower shard than where the last walk ended"
        );
        assert_eq!(
            compaction_record_count(&store, 0, 200).await,
            1,
            "the newly landed bucket was migrated exactly once"
        );
        // The already-migrated buckets are not touched again: each still
        // carries exactly one compaction record.
        assert_eq!(compaction_record_count(&store, 0, 100).await, 1);
        assert_eq!(compaction_record_count(&store, 1, 100).await, 1);
    }

    /// A redundant run after the floor already sits at the target reports
    /// success (the floor invariant already holds) rather than surfacing
    /// `raise_format_floor`'s non-strict-raise refusal as an error.
    #[tokio::test]
    async fn rerun_after_floor_reached_is_idempotent() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        let target = VERSION_V7 as u32;
        seed_at(&store, 0, 100, 1, "alpha", target).await;
        let clock = FixedClock::new(sealed_now_ns_for(100));
        let config = CompactorConfig::default();

        let first = migrate_family(
            &store,
            &clock,
            &config,
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            target,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("first migrate");
        assert_eq!(
            first.verification,
            Some(Verification::FloorRaised {
                floor_version: target
            })
        );

        let second = migrate_family(
            &store,
            &clock,
            &config,
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            target,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("second migrate");
        assert_eq!(
            second.verification,
            Some(Verification::FloorRaised {
                floor_version: target
            }),
            "a second run over an already-raised floor still reports the floor, not an error"
        );
    }

    /// Regression: a bucket the walk itself just rewrote must
    /// not make its own pre-rewrite L0 commit record look like a straggler.
    /// The one seeded record is below target, so [`migrate_bucket_format`]
    /// rewrites it into a superseding compaction record; the pre-rewrite
    /// commit record is still physically present afterward (only a `sweep`
    /// deletes it) but must no longer be live.
    ///
    /// This asserts against [`count_below_target`] directly rather than
    /// `migrate_family`'s end-to-end `Verification::FloorRaised`, because of the
    /// target this particular test uses, not any RSEG limitation. Since ADR-0066
    /// decision 5 (slice B) RSEG genuinely decodes and re-encodes an
    /// older-recorded input to the current version, so it *can* drive a
    /// `FloorRaised` end to end when the target is the current version -- see
    /// [`rseg_below_output_input_migrates_and_raises_floor`]. This test instead
    /// seeds an at-`VERSION_V7` record and uses `FUTURE_VERSION` (a target above
    /// the writer, the trick that makes an at-current record count as "below
    /// target" and eligible for the walk); that same fictional target then makes
    /// the rewrite's own L1 output (recorded at `VERSION_V7`, genuinely
    /// `< FUTURE_VERSION`) count as below target too, so a `FloorRaised` result
    /// is structurally unreachable in *this* setup, orthogonal to the
    /// just-rewrote case. Calling `count_below_target` directly isolates exactly
    /// the mechanism the fix changes (the L0 branch) from that unrelated,
    /// expected L1 count.
    ///
    /// Before the supersession exclusion, `count_below_target`'s L0 branch
    /// counted every commit record's `segment_format_version` unconditionally,
    /// with no check of whether any compaction/rewrite record superseded it.
    /// The `if superseded_commits.contains(&key) { continue; }` guard is the
    /// flipped line: delete it and the assertion below sees `(l0, l1) == (1, 1)`
    /// instead of `(0, 1)`, i.e. the bucket's own
    /// pre-rewrite record counts as a straggler alongside the genuinely
    /// below-target L1 part, refusing a raise it just earned until an
    /// unrelated sweep clears the leftover record.
    #[tokio::test]
    async fn a_bucket_migrated_this_walk_does_not_straggler_on_its_own_pre_rewrite_record() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        // Real v7 bytes recorded at the current version, so RsegCodec's
        // validate_rewrite_inputs (which refuses an input genuinely below the
        // *current writer* version, since RSEG copies pages verbatim and
        // cannot really decode-and-reencode an older layout) accepts it; using
        // FUTURE_VERSION as the migration target is what makes this same
        // record count as "below target" and eligible for the walk to
        // rewrite, exactly as the other tests in this module do.
        seed_at(&store, 0, 100, 1, "alpha", VERSION_V7 as u32).await;

        let clock = FixedClock::new(sealed_now_ns_for(100));
        let bucket = Bucket::new(tenant_hash(), Signal::Metrics, 0, 100);
        let outcome = migrate_bucket_format(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket,
            FUTURE_VERSION,
        )
        .await
        .expect("migrate bucket");
        assert!(
            matches!(outcome, MigrateOutcome::Rewritten { .. }),
            "the below-target bucket must be rewritten, got {outcome:?}"
        );
        assert_eq!(
            compaction_record_count(&store, 0, 100).await,
            1,
            "the rewrite published exactly one compaction record"
        );

        // No interleaved sweep ran, so the pre-rewrite commit record is still
        // physically present: the exclusion below is about liveness, not
        // absence.
        let l0_unfiltered = list_bucket(&store, &bucket)
            .await
            .expect("list bucket")
            .commit_keys
            .len();
        assert_eq!(
            l0_unfiltered, 1,
            "the pre-rewrite commit record was never deleted; only the re-audit's liveness \
             filter, not physical absence, keeps it out of the straggler count"
        );

        let audit = count_below_target(&store, &tenant_hash(), Signal::Metrics, 1, FUTURE_VERSION)
            .await
            .expect("re-audit");
        assert_eq!(
            (audit.l0, audit.l1, audit.rewrite_parts),
            (0, 1, 0),
            "the dead pre-rewrite L0 record must not count as a straggler (l0 == 0); \
             the freshly rewritten L1 part, genuinely below the fictional FUTURE_VERSION \
             target, still correctly counts (l1 == 1) -- the fix excludes exactly the \
             superseded L0 record, not the bucket's live output"
        );
        assert!(
            audit.blocked.is_empty(),
            "only a rewrite record's parts put a bucket on this list; a below-target \
             compaction part is counted in l1 and blocks the floor just the same, but \
             naming it is issue #2093's decision, not this pass's: {:?}",
            audit.blocked
        );
    }

    /// Regression: the re-audit's supersession exclusion must be
    /// keyed on the superseding record's explicit input set, not on membership
    /// of its ingest-hour bucket. A commit record that no compaction or rewrite
    /// record names is live, however many such records its bucket carries.
    ///
    /// The bucket here holds two L0 commit records but a compaction record over
    /// only one of them: `alpha` is seeded, migrated (which publishes a
    /// compaction record naming `alpha` alone), and only then is `beta` seeded
    /// into the same bucket. Sealing is what makes that ordering impossible in
    /// production today, which is exactly why the re-audit must not depend on
    /// it: the re-audit's job is to verify the walk independently, and its
    /// answer here has to come from the record's `inputs` list, the same
    /// predicate sweep rule 2 deletes by.
    ///
    /// `beta` is below the target and superseded by nothing, so it must be
    /// counted: `l0_below == 1` refuses the floor raise. The flipped line is
    /// `count_below_target`'s exclusion key. Replace the input-set set with the
    /// bucket-membership set the fix removed (collect `ingest_hour_bucket` from
    /// each compaction/rewrite key and test `superseded_hours.contains(&parsed
    /// .ingest_hour_bucket)` on the commit branch) and this test fails with
    /// `l0_below == 0`: `beta` is excluded because a *different* record's
    /// compaction record shares its bucket, and migrate raises the format floor
    /// over live data still below the target. That is the durability regression
    /// the input-set key removes; the assertion is not vacuous.
    #[tokio::test]
    async fn partial_coverage_record_does_not_exclude_the_l0_records_it_never_named() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        let bucket = Bucket::new(tenant_hash(), Signal::Metrics, 0, 100);
        let clock = FixedClock::new(sealed_now_ns_for(100));

        // One L0 record, migrated: the published compaction record's inputs
        // name `alpha` and nothing else.
        seed_at(&store, 0, 100, 1, "alpha", VERSION_V7 as u32).await;
        let outcome = migrate_bucket_format(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket,
            FUTURE_VERSION,
        )
        .await
        .expect("migrate bucket");
        assert!(
            matches!(outcome, MigrateOutcome::Rewritten { .. }),
            "the below-target bucket must be rewritten, got {outcome:?}"
        );
        assert_eq!(compaction_record_count(&store, 0, 100).await, 1);

        // A second below-target L0 record lands in the same bucket afterward,
        // named by no compaction or rewrite record: still live, still
        // un-migrated.
        seed_at(&store, 0, 100, 2, "beta", VERSION_V7 as u32).await;
        let l0_present = list_bucket(&store, &bucket)
            .await
            .expect("list bucket")
            .commit_keys
            .len();
        assert_eq!(
            l0_present, 2,
            "the bucket now holds a superseded commit record and an uncovered one"
        );

        let audit = count_below_target(&store, &tenant_hash(), Signal::Metrics, 1, FUTURE_VERSION)
            .await
            .expect("re-audit");
        assert_eq!(
            (audit.l0, audit.l1, audit.rewrite_parts),
            (1, 1, 0),
            "the uncovered below-target commit record must count as a straggler \
             (l0 == 1) even though its bucket carries a compaction record: only \
             the record that compaction actually named is superseded. Counting 0 here \
             is a false floor raise over live un-migrated data"
        );
    }

    /// Build and PUT one compaction record at `(shard, hour)` naming the L0
    /// inputs seeded at `input_seqs` (the same `seq` [`seed_at`] takes, which
    /// fixes the input's writer identity). `hash_seed` fills `input_set_hash`
    /// independently of `inputs`, so a test controls both the record key and
    /// the selection rule's hash tie-break. Every part is stamped at
    /// `part_version`, so a test can keep `l1_below` out of the figure it is
    /// pinning. No part data objects are written: neither
    /// [`count_below_target`] nor the walk's eligibility check reads them.
    async fn put_compaction_record(
        store: &dyn ObjectStoreBackend,
        shard: u32,
        hour: u32,
        input_seqs: &[u64],
        hash_seed: u8,
        part_version: u32,
    ) -> String {
        put_compaction_fixture(
            store,
            CompactionFixture {
                shard,
                hour,
                input_seqs,
                hash_seed,
                part_versions: &[part_version],
                supersedes: "",
            },
        )
        .await
    }

    /// One compaction record for [`put_compaction_fixture`] to build.
    struct CompactionFixture<'a> {
        shard: u32,
        hour: u32,
        /// The L0 inputs this record names, by the same `seq` [`seed_at`]
        /// takes.
        input_seqs: &'a [u64],
        /// Fills a version 1 record's `input_set_hash`, which the record key
        /// embeds. A version 2 record's hash is the version 2 hash over its
        /// inputs and `supersedes`, so it ignores this.
        hash_seed: u8,
        /// One part per entry, stamped at that version.
        part_versions: &'a [u32],
        /// The compaction record key this one supersedes. Non-empty makes it a
        /// version 2 record.
        supersedes: &'a str,
    }

    /// Build and PUT one compaction record, version 1 or version 2 as
    /// `spec.supersedes` decides. No part data objects are written.
    async fn put_compaction_fixture(
        store: &dyn ObjectStoreBackend,
        spec: CompactionFixture<'_>,
    ) -> String {
        use prost::Message;
        use ravel_proto::commit::v1::{CompactionInputIdentity, CompactionPart};

        let CompactionFixture {
            shard,
            hour,
            input_seqs,
            hash_seed,
            part_versions,
            supersedes,
        } = spec;
        let th = tenant_hash();
        let created = i64::from(hour) * NS_PER_HOUR;
        let inputs: Vec<CompactionInputIdentity> = input_seqs
            .iter()
            .map(|seq| CompactionInputIdentity {
                writer_id: Uuid::from_u128(u128::from(*seq)).to_string(),
                writer_epoch: EPOCH,
                writer_seq: *seq,
            })
            .collect();
        let (format_version, input_set_hash) = if supersedes.is_empty() {
            (1, vec![hash_seed; 32])
        } else {
            (
                2,
                ravel_commit::erasure::compute_superseding_compaction_input_set_hash(
                    &inputs, supersedes,
                )
                .to_vec(),
            )
        };
        let record = CompactionRecord {
            format_version,
            tenant_hash: th.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket: hour,
            level: 1,
            inputs,
            input_set_hash: input_set_hash.clone(),
            parts: part_versions
                .iter()
                .zip(0u32..)
                .map(|(version, part_index)| CompactionPart {
                    part_index,
                    first_series_id: vec![0u8; 16],
                    last_series_id: vec![0xff; 16],
                    content_hash: vec![hash_seed.wrapping_add(part_index as u8); 32],
                    object_size: 4096,
                    sample_count: 1,
                    series_count: 1,
                    run_count: 1,
                    min_event_ts_ns: created,
                    max_event_ts_ns: created + 100,
                    segment_format_version: *version,
                    declared_column_stats: Vec::new(),
                })
                .collect(),
            created_unix_ns: created,
            superseded_record_key: supersedes.to_string(),
        };
        let hash16: String = input_set_hash[..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let key = keys::compaction_record_key(&th, Signal::Metrics, shard, hour, &hash16)
            .expect("compaction record key");
        store
            .put(
                &key,
                record.encode_to_vec().into(),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put compaction record");
        key
    }

    /// One selective-erasure rewrite record for [`put_rewrite_record`] to
    /// build.
    struct RewriteFixture<'a> {
        shard: u32,
        hour: u32,
        /// The L0 inputs this record supersedes, by the same `seq` [`seed_at`]
        /// takes. Empty for a rewrite that supersedes another record instead.
        input_seqs: &'a [u64],
        /// Fills `input_set_hash`, which the record key embeds.
        hash_seed: u8,
        part_version: u32,
        part_count: u32,
        /// The record key this one replaces, empty for a first-pass rewrite
        /// over raw L0, so a test can seed a predecessor that is superseded but
        /// still listed.
        supersedes: &'a str,
    }

    /// Build and PUT one selective-erasure rewrite record (ADR-0064), carrying
    /// `part_count` surviving parts all stamped at `part_version`. No part data
    /// objects are written: neither [`count_below_target`] nor the walk's
    /// eligibility check reads them.
    async fn put_rewrite_record(
        store: &dyn ObjectStoreBackend,
        spec: RewriteFixture<'_>,
    ) -> String {
        let RewriteFixture {
            shard,
            hour,
            input_seqs,
            hash_seed,
            part_version,
            part_count,
            supersedes,
        } = spec;
        use prost::Message;
        use ravel_proto::commit::v1::{CompactionInputIdentity, CompactionPart, RewriteDrop};

        let th = tenant_hash();
        let created = i64::from(hour) * NS_PER_HOUR;
        let input_set_hash = vec![hash_seed; 32];
        let record = RewriteRecord {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket: hour,
            inputs: input_seqs
                .iter()
                .map(|seq| CompactionInputIdentity {
                    writer_id: Uuid::from_u128(u128::from(*seq)).to_string(),
                    writer_epoch: EPOCH,
                    writer_seq: *seq,
                })
                .collect(),
            input_set_hash: input_set_hash.clone(),
            parts: (0..part_count)
                .map(|part_index| CompactionPart {
                    part_index,
                    first_series_id: vec![0u8; 16],
                    last_series_id: vec![0xff; 16],
                    content_hash: vec![hash_seed.wrapping_add(part_index as u8); 32],
                    object_size: 4096,
                    sample_count: 1,
                    series_count: 1,
                    run_count: 1,
                    min_event_ts_ns: created,
                    max_event_ts_ns: created + 100,
                    segment_format_version: part_version,
                    declared_column_stats: Vec::new(),
                })
                .collect(),
            drops: vec![RewriteDrop {
                request_id: Uuid::from_u128(0xdeadbeef).to_string(),
                dropped_count: 1,
            }],
            created_unix_ns: created,
            superseded_record_key: supersedes.to_string(),
        };
        let hash16: String = input_set_hash[..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let key = keys::rewrite_record_key(&th, Signal::Metrics, shard, hour, &hash16)
            .expect("rewrite record key");
        store
            .put(
                &key,
                record.encode_to_vec().into(),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put rewrite record");
        key
    }

    /// Acceptance (ADR-1331 decision 1, follow-up task 1): a bucket whose only
    /// live record is a below-target rewrite record blocks the floor, and the
    /// report names that exact bucket with `RewriteParts` and the exact count.
    ///
    /// The bucket holds one L0 commit record and a rewrite record naming it, so
    /// the L0 is superseded and `l0 == 0`; there is no compaction record, so
    /// `l1 == 0`. The two surviving rewrite parts are the only thing below the
    /// target, and nothing in this crate migrates them: the walk never calls the
    /// rewrite primitive here (the rewrite record leaves no L0 served raw), and
    /// the primitive would refuse the bucket if it did.
    ///
    /// Prove-the-test: the flipped line is `count_below_target`'s
    /// `RecordKind::Rewrite` arm. Restore the pre-change behaviour -- count
    /// `below` into `report.l1` and push no `BlockedBucket` -- and this fails
    /// with `Stragglers { l0: 0, l1: 2, rewrite_parts: 0, blocked: [] }`: the
    /// same refusal, naming nothing, against a guide that reads a zero
    /// `buckets_blocked` as "re-running is the remedy".
    #[tokio::test]
    async fn a_below_target_rewrite_record_blocks_the_floor_and_names_its_bucket() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        // The erased bucket: one L0 input, superseded by a rewrite record whose
        // two surviving parts were stamped at the erasure-time version, which is
        // below the target this run is migrating to.
        seed_at(&store, 0, 100, 1, "alpha", VERSION_V7 as u32).await;
        put_rewrite_record(
            &store,
            RewriteFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[1],
                hash_seed: 0x33,
                part_version: VERSION_V7 as u32,
                part_count: 2,
                supersedes: "",
            },
        )
        .await;

        let clock = FixedClock::new(sealed_now_ns_for(100));
        let report = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            FUTURE_VERSION,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate");

        assert!(report.walk_complete, "the walk drained within budget");
        assert_eq!(
            report.buckets_migrated, 0,
            "the erased bucket is not migratable, and nothing else is seeded"
        );
        let expected = vec![BlockedBucket {
            shard: 0,
            ingest_hour: 100,
            reason: BlockedReason::RewriteParts { below_target: 2 },
        }];
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 0,
                l1: 0,
                rewrite_parts: 2,
                blocked: expected.clone(),
            }),
            "the two below-target rewrite parts refuse the raise and are reported as their \
             own source, with the blocking bucket named: folding them into l1 says the same \
             refusal while naming nothing"
        );
        assert_eq!(report.blocked_buckets, expected);
        assert_eq!(report.buckets_blocked(), 1);

        // The floor is not raised, and a re-run reports exactly the same thing:
        // this is the "re-running is not the remedy" state the report claims.
        let floor = current_floor_from_store(&store, &tenant_hash(), Signal::Metrics, FAMILY)
            .await
            .expect("read floor");
        assert_eq!(floor, None, "a refused verify raises no floor");
        let again = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            FUTURE_VERSION,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("second migrate");
        assert_eq!(
            again.verification, report.verification,
            "re-running reports the identical blocked bucket: the block is permanent, which \
             is what the printed line tells the operator"
        );
    }

    /// `below_target` counts the parts of every rewrite record the bucket
    /// LISTS, not its live ones, and the guide, the CLI text and the changelog
    /// now say so.
    ///
    /// A superseding rewrite does not delete the record it supersedes; `sweep`
    /// does, on its own schedule. Until then both records are listed and both
    /// sets of parts are real objects the resolver can still be pointed at, so
    /// both are counted. The consequence an operator has to be told is the
    /// second half of this test: a bucket whose SUCCESSOR rewrite is already at
    /// the current output version stays blocked, on its predecessor's parts
    /// alone, until the sweep runs. "A later erasure request at the current
    /// version clears the block" was therefore wrong on its own.
    ///
    /// The single-bucket figure is a sum over two records (2 + 1, then 2 + 0),
    /// which is what pins the per-bucket summing rather than a one-record
    /// count.
    ///
    /// Prove-the-test: make `count_below_target` skip a record whose successor
    /// is present (or key the blocked entry on one record instead of summing
    /// the bucket) and the first case reports `below_target: 1` instead of 3.
    #[tokio::test]
    async fn a_superseded_predecessor_rewrite_keeps_counting_until_the_sweep() {
        // (successor part version, expected below-target parts for the bucket)
        for (successor_version, expected) in [(VERSION_V7 as u32, 3usize), (FUTURE_VERSION, 2)] {
            let store = MemoryStore::new();
            provision(&store, 1).await;
            seed_at(&store, 0, 100, 1, "alpha", VERSION_V7 as u32).await;
            // R1: the first erasure pass over the raw L0 input, two surviving
            // parts at the erasure-time version.
            let r1 = put_rewrite_record(
                &store,
                RewriteFixture {
                    shard: 0,
                    hour: 100,
                    input_seqs: &[1],
                    hash_seed: 0x33,
                    part_version: VERSION_V7 as u32,
                    part_count: 2,
                    supersedes: "",
                },
            )
            .await;
            // R2: a later erasure request supersedes R1 by key. R1 is still
            // listed: only a sweep removes it.
            put_rewrite_record(
                &store,
                RewriteFixture {
                    shard: 0,
                    hour: 100,
                    input_seqs: &[],
                    hash_seed: 0x55,
                    part_version: successor_version,
                    part_count: 1,
                    supersedes: &r1,
                },
            )
            .await;

            let clock = FixedClock::new(sealed_now_ns_for(100));
            let report = migrate_family(
                &store,
                &clock,
                &CompactorConfig::default(),
                tenant_hash(),
                Signal::Metrics,
                FAMILY,
                FUTURE_VERSION,
                1,
                MigrateBudget::unlimited(),
                "test",
            )
            .await
            .expect("migrate");

            let blocked = vec![BlockedBucket {
                shard: 0,
                ingest_hour: 100,
                reason: BlockedReason::RewriteParts {
                    below_target: expected,
                },
            }];
            assert_eq!(
                report.verification,
                Some(Verification::Stragglers {
                    l0: 0,
                    l1: 0,
                    rewrite_parts: expected,
                    blocked: blocked.clone(),
                }),
                "with the successor at version {successor_version} the bucket's figure is the \
                 sum over every LISTED rewrite record, so it is {expected}: R1's two parts \
                 count whether or not R2 supersedes it, because R1's parts are still there"
            );
            assert_eq!(report.blocked_buckets, blocked);
            assert_eq!(
                report.buckets_blocked(),
                1,
                "two records, one bucket: `buckets_blocked` counts buckets"
            );
        }
    }

    /// When BOTH passes name one bucket, the re-audit's `RewriteParts` entry is
    /// the one kept, and the merged list is sorted.
    ///
    /// Bucket `(0, 100)` carries overlapping compaction records that leave
    /// input 3 served raw and below the target (what the walk sees) and an
    /// erasure rewrite superseding the winning compaction record whose two
    /// surviving parts are below the target (what the re-audit sees). Only one
    /// entry may survive, since `buckets_blocked` counts buckets, and the entry
    /// that survives has to be the one carrying a count.
    ///
    /// Bucket `(0, 99)` is blocked too and sorts BEFORE it while being appended
    /// after, so the final order is not the order the two passes produced.
    ///
    /// Prove-the-test: restore the `continue`-on-collision merge (the walk's
    /// entry wins) and `(0, 100)` comes back as `LoserOnlyInputs` with no
    /// count, failing the exact-list assertion; drop the `sort_by_key` and the
    /// list comes back as `[(0, 100), (0, 99)]`.
    #[tokio::test]
    async fn a_bucket_both_passes_name_keeps_the_counted_reason_and_the_list_is_sorted() {
        let store = MemoryStore::new();
        provision(&store, 1).await;

        // (0, 99): erased, one surviving part below the target, nothing left
        // served raw, so only the re-audit names it.
        seed_at(&store, 0, 99, 6, "zeta", VERSION_V7 as u32).await;
        put_rewrite_record(
            &store,
            RewriteFixture {
                shard: 0,
                hour: 99,
                input_seqs: &[6],
                hash_seed: 0x66,
                part_version: VERSION_V7 as u32,
                part_count: 1,
                supersedes: "",
            },
        )
        .await;

        // (0, 100): the overlap (winner names {1, 2, 4}, loser names {2, 3}, so
        // input 3 is served raw) plus an erasure rewrite superseding the winner
        // with two below-target parts.
        for (seq, metric) in [(1u64, "alpha"), (2, "beta"), (3, "gamma"), (4, "delta")] {
            seed_at(&store, 0, 100, seq, metric, VERSION_V7 as u32).await;
        }
        let winner = put_compaction_record(&store, 0, 100, &[1, 2, 4], 0x11, FUTURE_VERSION).await;
        put_compaction_record(&store, 0, 100, &[2, 3], 0x22, FUTURE_VERSION).await;
        put_rewrite_record(
            &store,
            RewriteFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[],
                hash_seed: 0x77,
                part_version: VERSION_V7 as u32,
                part_count: 2,
                supersedes: &winner,
            },
        )
        .await;

        // (0, 101): plain and below target, so the walk migrates it and the
        // report is not uniformly blocked.
        seed_at(&store, 0, 101, 5, "epsilon", VERSION_V7 as u32).await;

        let clock = FixedClock::new(sealed_now_ns_for(101));
        let report = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            FUTURE_VERSION,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate");

        assert_eq!(
            report.buckets_migrated, 1,
            "only the plain bucket at hour 101 is migratable"
        );
        let expected = vec![
            BlockedBucket {
                shard: 0,
                ingest_hour: 99,
                reason: BlockedReason::RewriteParts { below_target: 1 },
            },
            BlockedBucket {
                shard: 0,
                ingest_hour: 100,
                reason: BlockedReason::RewriteParts { below_target: 2 },
            },
        ];
        assert_eq!(
            report.blocked_buckets, expected,
            "hour 100 is named by the walk (loser-only input 3) and by the re-audit (two \
             rewrite parts); the entry kept is the re-audit's, because dropping it drops \
             the only count either pass produces. Hour 99, appended second, sorts first"
        );
        assert_eq!(report.buckets_blocked(), 2);
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 1,
                l1: 1,
                rewrite_parts: 3,
                blocked: expected,
            }),
            "l0 is the loser-only input 3, l1 is hour 101's freshly written part (below the \
             fictional FUTURE_VERSION target), and rewrite_parts is 2 + 1 across the two \
             erased buckets"
        );
    }

    /// The other half of the split (ADR-1331 decision 1): a below-target
    /// COMPACTION part counts in `l1` and contributes nothing to
    /// `rewrite_parts`.
    ///
    /// It blocks the floor exactly as a rewrite part does, and re-running
    /// `migrate` does not change the figure: the part is already at the current
    /// writer's version, below only a target above it, so the force 2
    /// re-encode cannot carry it further and the walk does not try, while
    /// `compact_bucket` and `migrate_bucket_format` both refuse a bucket that
    /// already carries a compaction record. What this
    /// test pins is only that the two SOURCES are counted apart, and that a
    /// bucket whose AUTHORITATIVE record is below the target is not named in
    /// `blocked_buckets`: only a bucket held below by overlap losers' parts is
    /// ([`BlockedReason::LosingRecordParts`]).
    ///
    /// Prove-the-test: make `count_below_target` treat every record's parts as
    /// rewrite parts (delete the `match rec.kind`) and this fails with
    /// `rewrite_parts == 1` and one blocked bucket, the mirror of the
    /// acceptance test above.
    #[tokio::test]
    async fn a_below_target_compaction_part_counts_in_l1_and_blocks_nothing() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        seed_at(&store, 0, 100, 1, "alpha", VERSION_V7 as u32).await;
        // A compaction record over that input with one part below the target.
        put_compaction_record(&store, 0, 100, &[1], 0x44, VERSION_V7 as u32).await;

        let clock = FixedClock::new(sealed_now_ns_for(100));
        let report = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            FUTURE_VERSION,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate");

        assert!(report.walk_complete, "the walk drained within budget");
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 0,
                l1: 1,
                rewrite_parts: 0,
                blocked: Vec::new(),
            }),
            "the below-target compaction part is an l1 straggler and nothing else"
        );
        assert!(
            report.blocked_buckets.is_empty(),
            "the below-target part belongs to the bucket's authoritative record, so it blocks \
             the floor as an l1 count alone and names no bucket (issue #2093): {:?}",
            report.blocked_buckets
        );
        assert_eq!(report.buckets_blocked(), 0);

        // The figure is stable across runs, which is the part of "re-running is
        // not the remedy" that applies to an l1 straggler as much as to a
        // rewrite one: nothing migrated it in between.
        let again = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            FUTURE_VERSION,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("second migrate");
        assert_eq!(
            again.verification, report.verification,
            "re-running reports the identical l1 count: no path migrates a compaction part \
             already at the current writer's version, so the floor stays refused on the same \
             figure"
        );
    }

    /// Seed the overlap fixture: one bucket at `(0, 100)` holding four
    /// below-target L0 records and two OVERLAPPING compaction records, plus a
    /// plain uncompacted below-target bucket at `(0, 101)` so the asserted
    /// figure is a sum over two independent contributions rather than a
    /// single-record 0-or-1 flip.
    ///
    /// The winner names `winner_seqs`; the loser always names `{2, 3}`, so the
    /// two overlap on input 2 and form one component. Caller picks whether
    /// input 3 is also named by the winner, which is the whole variable under
    /// test. Part versions are stamped at the target so `l1_below` is 0 and the
    /// pinned figure is purely the L0 count.
    async fn seed_overlapping_records(store: &dyn ObjectStoreBackend, winner_seqs: &[u64]) {
        for (seq, metric) in [(1u64, "alpha"), (2, "beta"), (3, "gamma"), (4, "delta")] {
            seed_at(store, 0, 100, seq, metric, VERSION_V7 as u32).await;
        }
        // An unrelated below-target bucket with no compaction record at all:
        // always a straggler, in every scenario.
        seed_at(store, 0, 101, 5, "epsilon", VERSION_V7 as u32).await;

        // 0x11 sorts below 0x22, so the winner also wins the hash tie-break;
        // the input-set cardinality already decides it, and pinning the
        // fallback too keeps the fixture from depending on which term fired.
        put_compaction_record(store, 0, 100, winner_seqs, 0x11, FUTURE_VERSION).await;
        put_compaction_record(store, 0, 100, &[2, 3], 0x22, FUTURE_VERSION).await;
    }

    /// Regression (issue #1156): the re-audit must take an L0 input as
    /// superseded only where an AUTHORITATIVE compaction record names it.
    ///
    /// Two compaction records over one bucket overlap on input 2, so they form
    /// one overlap component and `select_authoritative_compaction_records`
    /// keeps exactly one. The loser's parts are served from nowhere, so input
    /// 3 -- which only the loser names -- is still served as a raw L0 segment
    /// by `Catalog::resolve`, is still below the target, and must still count.
    ///
    /// Both directions are asserted here rather than described: the same
    /// fixture is built twice, once with the winner naming `{1, 2, 4}` (input 3
    /// loser-only, so it counts) and once with the winner naming `{1, 2, 3, 4}`
    /// (input 3 named by the winner, so it does not). The figures are pinned
    /// exactly, and they differ, so neither a predicate that excludes
    /// everything nor one that excludes nothing passes.
    ///
    /// The flipped line is `count_below_target`'s authority filter: drop the
    /// `select_authoritative_compaction_records` pass and extend
    /// `superseded_commits` from every compaction record (the pre-fix
    /// `superseded_commits.extend(superseded_input_commit_keys(tenant_hash,
    /// signal, shard, &rec)?)` inside the `CompactionRecord` arm) and the
    /// loser-only case fails with `l0_below == 1`: input 3 is excluded on the
    /// strength of a record whose parts nothing serves, and `migrate_family`
    /// raises the format floor over an object the resolver still returns raw.
    /// `blocked_buckets` must not claim permanence when the refusal cause is
    /// gone. Both halves of the predicate, on one fixture with one variable
    /// changed.
    ///
    /// The transient half is the one the counter got wrong before: with every
    /// record deleted, `raw_served_commit_keys` short-circuits and returns
    /// EVERY commit as served raw, so an inputs-only test reads exactly like
    /// the overlap case. That bucket migrates on the next run, and reporting
    /// it as permanently blocked sends an operator looking for an overlap that
    /// is not there, against a guide that tells them re-running will not help.
    ///
    /// Prove-the-test: drop the empty-records early return in
    /// `refusal_is_permanent` and the second assertion reads true.
    #[tokio::test]
    async fn a_refusal_is_permanent_only_while_its_cause_survives() {
        let store = MemoryStore::new();
        seed_overlapping_records(&store, &[1, 2, 4]).await;
        let bucket = Bucket::new(tenant_hash(), Signal::Metrics, 0, 100);
        let config = CompactorConfig::default();

        assert!(
            refusal_is_permanent(&store, &bucket, &config, FUTURE_VERSION)
                .await
                .expect("permanence check"),
            "the overlap survives and leaves input 3 served raw and below target"
        );

        // Retention removes the records that caused the refusal.
        let listing = list_bucket(&store, &bucket).await.expect("list bucket");
        for key in &listing.compaction_record_keys {
            store.delete(key).await.expect("delete record");
        }

        assert!(
            !refusal_is_permanent(&store, &bucket, &config, FUTURE_VERSION)
                .await
                .expect("permanence check"),
            "with no record left to refuse on, the next run migrates this bucket: the \
             inputs are still below target and still served raw, so an inputs-only \
             check would wrongly call this permanent"
        );
    }

    /// A record listed and then deleted before it is read must not abort the
    /// walk. Retention can remove a compaction record between the listing and
    /// the read, and propagating `NotFound` would fail `migrate_family` before
    /// the cursor advances, losing a long migration's progress to an unrelated
    /// concurrent pass.
    ///
    /// The stale listing IS the race: it still names a key whose object is
    /// gone, which is exactly the state the walk holds when retention runs
    /// underneath it.
    ///
    /// Prove-the-test: restore `store.get(key, GetRange::Full).await?` in
    /// `raw_served_commit_keys` and this returns `Err(NotFound)` instead of a
    /// served set. The assertion on the result also pins the fail-safe
    /// direction: a vanished record supersedes nothing, so its inputs come
    /// back as served raw rather than being silently excluded.
    #[tokio::test]
    async fn a_record_deleted_after_the_listing_does_not_abort_the_walk() {
        let store = MemoryStore::new();
        seed_overlapping_records(&store, &[1, 2, 4]).await;
        let bucket = Bucket::new(tenant_hash(), Signal::Metrics, 0, 100);
        let listing = list_bucket(&store, &bucket).await.expect("list bucket");
        assert_eq!(
            listing.compaction_record_keys.len(),
            2,
            "the fixture must carry both overlapping records, or this proves nothing"
        );

        // Retention removes one of them while the walk still holds the listing.
        for key in &listing.compaction_record_keys {
            store.delete(key).await.expect("delete record");
        }

        let served = raw_served_commit_keys(&store, &bucket, &listing)
            .await
            .expect("a vanished record must not abort the walk");
        assert_eq!(
            served.keys.len(),
            4,
            "with both records gone nothing supersedes, so all four inputs are served \
             raw: the conservative direction, which refuses a floor raise rather than \
             raising it over an object that is still served"
        );
        assert_eq!(
            served.records_read, 0,
            "the listing still NAMES two records, so a caller trusting the listing would \
             conclude a refusal cause survives. Only the read count distinguishes a record \
             that is there from one that was there when we listed, which is why \
             refusal_is_permanent asks this rather than the listing it passed in"
        );
    }

    #[tokio::test]
    async fn a_loser_only_l0_input_still_counts_below_the_target() {
        // Winner names {1, 2, 4}; input 3 is named by the loser alone.
        let store = MemoryStore::new();
        seed_overlapping_records(&store, &[1, 2, 4]).await;

        // The selection rule itself, on this exact fixture: the smaller
        // overlapping record is the loser, so its parts are not served and its
        // exclusive input is not superseded.
        let bucket = Bucket::new(tenant_hash(), Signal::Metrics, 0, 100);
        let listing = list_bucket(&store, &bucket).await.expect("list bucket");
        let served = raw_served_commit_keys(&store, &bucket, &listing)
            .await
            .expect("raw-served set");
        let expected_raw = keys::commit_key(
            &tenant_hash(),
            Signal::Metrics,
            0,
            100,
            Uuid::from_u128(3),
            EPOCH,
            3,
        )
        .expect("commit key");
        assert_eq!(
            served.keys,
            vec![expected_raw],
            "exactly the loser-only input is left served raw: the winner's three inputs \
             are inside its parts, and the loser's parts are served from nowhere"
        );

        let audit = count_below_target(&store, &tenant_hash(), Signal::Metrics, 1, FUTURE_VERSION)
            .await
            .expect("re-audit");
        assert_eq!(
            (audit.l0, audit.l1, audit.rewrite_parts),
            (2, 0, 0),
            "exactly two below-target L0 records are live: the loser-only input 3, still \
             served raw, and the uncompacted bucket's input 5. Counting 1 here excludes \
             input 3 on a losing record's say-so and raises the floor over an object \
             Catalog::resolve still serves raw"
        );

        // Same fixture, one variable changed: the winner now names input 3 too,
        // so it IS superseded and must drop out of the count.
        let store = MemoryStore::new();
        seed_overlapping_records(&store, &[1, 2, 3, 4]).await;

        let listing = list_bucket(&store, &bucket).await.expect("list bucket");
        let served = raw_served_commit_keys(&store, &bucket, &listing)
            .await
            .expect("raw-served set");
        assert!(
            served.keys.is_empty(),
            "the winner names every input of the bucket, so nothing is served raw: {served:?}"
        );

        let audit = count_below_target(&store, &tenant_hash(), Signal::Metrics, 1, FUTURE_VERSION)
            .await
            .expect("re-audit");
        assert_eq!(
            (audit.l0, audit.l1, audit.rewrite_parts),
            (1, 0, 0),
            "only the uncompacted bucket's input 5 is left: an input the AUTHORITATIVE \
             record names is superseded and must not count"
        );
    }

    /// Regression (issue #1156), the walk half: a bucket whose compaction
    /// records leave a below-target L0 served raw must be VISITED, not skipped
    /// on the mere presence of a compaction record.
    ///
    /// The rewrite primitive still refuses it (one record set per bucket, and a
    /// new record over the loser-only subset would join the same overlap
    /// component and lose to the existing winner), so the correct end state is
    /// a reported blocked bucket and a refused floor raise, not a silent skip
    /// followed by a raise. The uncompacted bucket at `(0, 101)` is migrated in
    /// the same walk, which is what shows the visit is selective rather than a
    /// blanket refusal.
    ///
    /// The flipped line is the walk's eligibility gate: restore
    /// `listing.compaction_record_keys.is_empty() &&
    /// listing.rewrite_record_keys.is_empty()` to the `if` in `migrate_family`
    /// and `blocked_buckets` is empty -- the bucket is never examined past its
    /// listing.
    #[tokio::test]
    async fn the_walk_visits_a_bucket_whose_losing_record_leaves_raw_l0_served() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        seed_overlapping_records(&store, &[1, 2, 4]).await;

        let clock = FixedClock::new(sealed_now_ns_for(101));
        let report = migrate_family(
            &store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            FUTURE_VERSION,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate");

        assert_eq!(
            report.blocked_buckets,
            vec![BlockedBucket {
                shard: 0,
                ingest_hour: 100,
                reason: BlockedReason::LoserOnlyInputs,
            }],
            "the overlap bucket still serves a below-target L0 raw and the rewrite \
             primitive refuses it: the walk must name it with its reason, not skip it"
        );
        assert_eq!(report.buckets_blocked(), 1);
        assert_eq!(
            report.buckets_migrated, 1,
            "the uncompacted bucket at hour 101 is still migrated in the same walk"
        );
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 1,
                l1: 1,
                rewrite_parts: 0,
                blocked: vec![BlockedBucket {
                    shard: 0,
                    ingest_hour: 100,
                    reason: BlockedReason::LoserOnlyInputs,
                }],
            }),
            "the floor is not raised: the loser-only L0 input is still below the target \
             (l0 == 1), and hour 101's freshly written L1 part is below the fictional \
             FUTURE_VERSION target exactly as in the other tests here (l1 == 1). No rewrite \
             record exists here, so rewrite_parts is 0 and the one blocked bucket carries \
             the overlap reason"
        );
    }

    /// Run [`migrate_family`] once to [`FUTURE_VERSION`] over one shard with
    /// every bucket up to `sealed_hour` sealed.
    async fn migrate_to_future(
        store: &dyn ObjectStoreBackend,
        sealed_hour: u32,
    ) -> FamilyMigrateReport {
        let clock = FixedClock::new(sealed_now_ns_for(sealed_hour));
        migrate_family(
            store,
            &clock,
            &CompactorConfig::default(),
            tenant_hash(),
            Signal::Metrics,
            FAMILY,
            FUTURE_VERSION,
            1,
            MigrateBudget::unlimited(),
            "test",
        )
        .await
        .expect("migrate")
    }

    /// Seed four below-target L0 inputs at `(0, 100)` and two overlapping
    /// compaction records over them: the winner names all four with one part
    /// per entry of `winner_part_versions`, the loser names `{2, 3}` with three
    /// parts, two below [`FUTURE_VERSION`] and one at it. The winner names every
    /// input, so nothing is served raw and the walk has nothing to migrate.
    /// Returns the winner's key.
    async fn seed_losing_parts_bucket(
        store: &dyn ObjectStoreBackend,
        winner_part_versions: &[u32],
    ) -> String {
        for (seq, metric) in [(1u64, "alpha"), (2, "beta"), (3, "gamma"), (4, "delta")] {
            seed_at(store, 0, 100, seq, metric, VERSION_V7 as u32).await;
        }
        let winner = put_compaction_fixture(
            store,
            CompactionFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[1, 2, 3, 4],
                hash_seed: 0x11,
                part_versions: winner_part_versions,
                supersedes: "",
            },
        )
        .await;
        put_compaction_fixture(
            store,
            CompactionFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[2, 3],
                hash_seed: 0x22,
                part_versions: &[VERSION_V7 as u32, VERSION_V7 as u32, FUTURE_VERSION],
                supersedes: "",
            },
        )
        .await;
        winner
    }

    /// ADR-0066 force 2 amendment, item 9: a bucket whose authoritative record
    /// is at the target and whose overlap loser carries below-target parts is
    /// named `LosingRecordParts` with the loser's below-target part count, and
    /// those parts still count in `l1`, so the floor is still refused.
    ///
    /// The loser has three parts and two are below the target, so
    /// `below_target == 2` pins a count of below-target PARTS, not of losing
    /// records (1) or of the loser's parts (3). `l1 == 2` pins that the losing
    /// parts still count: the winner's part is at the target and adds nothing.
    ///
    /// Prove-the-test: delete the `blocked_by_hour.insert(*hour,
    /// BlockedReason::LosingRecordParts { below_target })` line in
    /// `count_below_target` and this fails with `blocked_buckets` empty
    /// (`left: []`): the floor refused over parts no line names, which is the
    /// pre-change report.
    #[tokio::test]
    async fn a_bucket_held_below_only_by_losing_parts_is_named_with_their_count() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        seed_losing_parts_bucket(&store, &[FUTURE_VERSION]).await;

        let report = migrate_to_future(&store, 100).await;

        let expected = vec![BlockedBucket {
            shard: 0,
            ingest_hour: 100,
            reason: BlockedReason::LosingRecordParts { below_target: 2 },
        }];
        assert_eq!(report.blocked_buckets, expected);
        assert_eq!(report.buckets_migrated, 0, "nothing is served raw");
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 0,
                l1: 2,
                rewrite_parts: 0,
                blocked: expected,
            }),
            "the loser's two below-target parts count in l1 and name the bucket"
        );
    }

    /// The same bucket with one of the winner's two parts below the target:
    /// its authoritative record has not converged, so the losers are not what
    /// holds it below and the bucket is not named for them. Every below-target
    /// part still counts in `l1`. The winner's other part is at the target, so
    /// the check is "any authoritative part below", not "every one".
    ///
    /// Prove-the-test: delete the `if parts.authoritative.iter().any(|v| *v <
    /// target_version) { continue; }` check in `count_below_target`, or change
    /// its `any` to `all`, and the first assertion fails with `left:
    /// [BlockedBucket { shard: 0, ingest_hour: 100, reason: LosingRecordParts {
    /// below_target: 2 } }]`.
    #[tokio::test]
    async fn a_bucket_whose_winner_is_below_target_is_not_named_for_its_losers() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        seed_losing_parts_bucket(&store, &[FUTURE_VERSION, VERSION_V7 as u32]).await;

        let report = migrate_to_future(&store, 100).await;

        assert_eq!(report.blocked_buckets, Vec::new());
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 0,
                l1: 3,
                rewrite_parts: 0,
                blocked: Vec::new(),
            }),
            "the winner's below-target part and the loser's two all count in l1"
        );
    }

    /// A record a present version 2 record supersedes is not an overlap loser,
    /// so its below-target parts do not name the bucket (nothing in this build
    /// reclaims it either). C1 is a version 1 record with a below-target part, C2 a version 2
    /// record naming C1 with its part at the target. C1's part still counts
    /// in `l1`.
    ///
    /// Prove-the-test: in `authoritative_compaction_records`, test
    /// `selection.is_excluded(key)` instead of `selection.losing().contains(key)`
    /// for the losing branch and this fails with `blocked: [BlockedBucket {
    /// shard: 0, ingest_hour: 100, reason: LosingRecordParts { below_target: 1
    /// } }]`.
    #[tokio::test]
    async fn a_record_a_version_2_record_supersedes_is_not_named_as_a_loser() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        for (seq, metric) in [(1u64, "alpha"), (2, "beta")] {
            seed_at(&store, 0, 100, seq, metric, VERSION_V7 as u32).await;
        }
        let c1 = put_compaction_record(&store, 0, 100, &[1, 2], 0x11, VERSION_V7 as u32).await;
        let c2 = put_compaction_fixture(
            &store,
            CompactionFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[1, 2],
                hash_seed: 0,
                part_versions: &[FUTURE_VERSION],
                supersedes: &c1,
            },
        )
        .await;

        // The fixture is what the test says it is: the selector reads C1 as
        // superseded, not as a loser.
        let mut records = Vec::new();
        for key in [&c1, &c2] {
            let got = store.get(key, GetRange::Full).await.expect("get record");
            let rec = record::decode_compaction(got.data.as_ref()).expect("decode record");
            records.push((key.clone(), rec));
        }
        let selection = select_authoritative_compaction_records(&records).expect("select");
        assert!(selection.superseded().contains(c1.as_str()));
        assert!(selection.losing().is_empty());

        let report = migrate_to_future(&store, 100).await;

        assert_eq!(report.blocked_buckets, Vec::new());
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 0,
                l1: 1,
                rewrite_parts: 0,
                blocked: Vec::new(),
            }),
            "C1's below-target part counts in l1 and names nothing"
        );
    }

    /// A bucket whose rewrite record parts are below the target keeps its
    /// `RewriteParts` entry and is not also named for its overlap loser's
    /// parts. The rewrite record supersedes the winner and carries one
    /// below-target part.
    ///
    /// Prove-the-test: delete the `if blocked_by_hour.contains_key(hour) {
    /// continue; }` check in `count_below_target` and this fails with the
    /// bucket named `LosingRecordParts { below_target: 2 }` in place of
    /// `RewriteParts { below_target: 1 }`.
    #[tokio::test]
    async fn a_bucket_whose_rewrite_parts_are_below_target_is_named_rewrite_parts_only() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        let winner = seed_losing_parts_bucket(&store, &[FUTURE_VERSION]).await;
        put_rewrite_record(
            &store,
            RewriteFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[],
                hash_seed: 0x33,
                part_version: VERSION_V7 as u32,
                part_count: 1,
                supersedes: &winner,
            },
        )
        .await;

        let report = migrate_to_future(&store, 100).await;

        let expected = vec![BlockedBucket {
            shard: 0,
            ingest_hour: 100,
            reason: BlockedReason::RewriteParts { below_target: 1 },
        }];
        assert_eq!(report.blocked_buckets, expected);
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 0,
                l1: 2,
                rewrite_parts: 1,
                blocked: expected,
            }),
            "the loser's parts still count in l1; the bucket is named for its rewrite part"
        );
    }

    /// A bucket that lists a rewrite record whose parts are all at the target
    /// is named `LosingRecordParts` for its overlap loser's below-target parts
    /// (issue #2169): the rewrite record's presence alone does not suppress
    /// the line. The rewrite record supersedes the winner and its two parts are
    /// at the target, so `rewrite_parts == 0` and the loser's two below-target
    /// parts are the whole `l1`.
    ///
    /// Prove-the-test: make the `if blocked_by_hour.contains_key(hour)` check
    /// in `count_below_target` skip every hour that lists a rewrite record (the
    /// code before issue #2169) and this fails with `blocked_buckets` empty
    /// (`left: []`).
    #[tokio::test]
    async fn a_bucket_whose_rewrite_parts_are_at_target_is_named_for_its_losers() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        let winner = seed_losing_parts_bucket(&store, &[FUTURE_VERSION]).await;
        put_rewrite_record(
            &store,
            RewriteFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[],
                hash_seed: 0x33,
                part_version: FUTURE_VERSION,
                part_count: 2,
                supersedes: &winner,
            },
        )
        .await;

        let report = migrate_to_future(&store, 100).await;

        let expected = vec![BlockedBucket {
            shard: 0,
            ingest_hour: 100,
            reason: BlockedReason::LosingRecordParts { below_target: 2 },
        }];
        assert_eq!(report.blocked_buckets, expected);
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 0,
                l1: 2,
                rewrite_parts: 0,
                blocked: expected,
            }),
            "the loser's two below-target parts count in l1 and name the bucket; the \
             rewrite record's parts are at the target and count nowhere"
        );
    }

    /// A record a present rewrite record supersedes is not named as an overlap
    /// loser, even when the selector reads it as one: `sweep` deletes it and
    /// its parts with the rewrite's chain group, so the `LosingRecordParts`
    /// claim that neither `migrate` nor `sweep` reclaims them would be false.
    /// The race: C1 lands, a rewrite R supersedes C1, then a racing C2 over a
    /// superset of C1's inputs lands and wins the overlap. C2's part and R's
    /// two parts are at the target; C1's two below-target parts still count
    /// in `l1`, because C1 is listed until the sweep.
    ///
    /// Prove-the-test: drop the `.filter(|(key, _)|
    /// !authority.rewrite_superseded.contains(key.as_str()))` line from the
    /// `part_versions` closure in `read_shard_family` and this fails with
    /// `left: [BlockedBucket { shard: 0, ingest_hour: 100, reason:
    /// LosingRecordParts { below_target: 2 } }]`.
    #[tokio::test]
    async fn a_loser_a_rewrite_record_supersedes_is_not_named_for_its_parts() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        for (seq, metric) in [(1u64, "alpha"), (2, "beta"), (3, "gamma"), (4, "delta")] {
            seed_at(&store, 0, 100, seq, metric, VERSION_V7 as u32).await;
        }
        let c1 = put_compaction_fixture(
            &store,
            CompactionFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[2, 3],
                hash_seed: 0x22,
                part_versions: &[VERSION_V7 as u32, VERSION_V7 as u32, FUTURE_VERSION],
                supersedes: "",
            },
        )
        .await;
        put_rewrite_record(
            &store,
            RewriteFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[],
                hash_seed: 0x33,
                part_version: FUTURE_VERSION,
                part_count: 2,
                supersedes: &c1,
            },
        )
        .await;
        let c2 = put_compaction_fixture(
            &store,
            CompactionFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[1, 2, 3, 4],
                hash_seed: 0x11,
                part_versions: &[FUTURE_VERSION],
                supersedes: "",
            },
        )
        .await;

        // The fixture is what the test says it is: the selector reads C1 as
        // C2's overlap loser.
        let mut records = Vec::new();
        for key in [&c1, &c2] {
            let got = store.get(key, GetRange::Full).await.expect("get record");
            let rec = record::decode_compaction(got.data.as_ref()).expect("decode record");
            records.push((key.clone(), rec));
        }
        let selection = select_authoritative_compaction_records(&records).expect("select");
        assert!(selection.losing().contains(c1.as_str()));
        assert!(!selection.is_excluded(c2.as_str()));

        let report = migrate_to_future(&store, 100).await;

        assert_eq!(report.blocked_buckets, Vec::new());
        assert_eq!(report.buckets_migrated, 0, "nothing is served raw");
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 0,
                l1: 2,
                rewrite_parts: 0,
                blocked: Vec::new(),
            }),
            "C1's two below-target parts count in l1 and name nothing"
        );
    }

    /// The overlap WINNER is below the target and a live rewrite record
    /// supersedes it; a separate overlap loser is below the target and no
    /// rewrite supersedes it. The bucket is named `LosingRecordParts` for the
    /// loser's parts, because the rewrite-superseded winner is dropped from the
    /// authoritative side (its parts belong to the rewrite's chain, which
    /// `sweep` reclaims), so its below-target part does not read as an
    /// authoritative record that has not converged and does not suppress the
    /// line. C2 is the winner over `{1, 2, 3, 4}` with one below-target part,
    /// superseded by rewrite R whose two parts are at the target; C1 is the
    /// overlap loser over `{2, 3}` with two below-target parts, superseded by
    /// nothing. C1's two below-target parts name the bucket; C2's one and C1's
    /// two all count in `l1`, R's count nowhere.
    ///
    /// Prove-the-test: remove the `.filter(|(key, _)|
    /// !authority.rewrite_superseded.contains(key.as_str()))` from the
    /// authoritative side of the `part_versions` closure in `read_shard_family`
    /// (keep it on the losing side) and this fails with `blocked_buckets` empty
    /// (`left: []`): C2's below-target part is read back as an unconverged
    /// authoritative record, the `parts.authoritative.iter().any(...)` check in
    /// `count_below_target` continues over the hour, and the loser is never
    /// named.
    #[tokio::test]
    async fn a_rewrite_superseded_winner_below_target_does_not_suppress_loser_naming() {
        let store = MemoryStore::new();
        provision(&store, 1).await;
        for (seq, metric) in [(1u64, "alpha"), (2, "beta"), (3, "gamma"), (4, "delta")] {
            seed_at(&store, 0, 100, seq, metric, VERSION_V7 as u32).await;
        }
        let winner = put_compaction_fixture(
            &store,
            CompactionFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[1, 2, 3, 4],
                hash_seed: 0x11,
                part_versions: &[VERSION_V7 as u32],
                supersedes: "",
            },
        )
        .await;
        let loser = put_compaction_fixture(
            &store,
            CompactionFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[2, 3],
                hash_seed: 0x22,
                part_versions: &[VERSION_V7 as u32, VERSION_V7 as u32, FUTURE_VERSION],
                supersedes: "",
            },
        )
        .await;
        put_rewrite_record(
            &store,
            RewriteFixture {
                shard: 0,
                hour: 100,
                input_seqs: &[],
                hash_seed: 0x33,
                part_version: FUTURE_VERSION,
                part_count: 2,
                supersedes: &winner,
            },
        )
        .await;

        // The fixture is what the test says it is: the selector reads the
        // rewrite-superseded record as the overlap winner and the other as the
        // loser.
        let mut records = Vec::new();
        for key in [&winner, &loser] {
            let got = store.get(key, GetRange::Full).await.expect("get record");
            let rec = record::decode_compaction(got.data.as_ref()).expect("decode record");
            records.push((key.clone(), rec));
        }
        let selection = select_authoritative_compaction_records(&records).expect("select");
        assert!(!selection.is_excluded(winner.as_str()));
        assert!(selection.losing().contains(loser.as_str()));

        let report = migrate_to_future(&store, 100).await;

        let expected = vec![BlockedBucket {
            shard: 0,
            ingest_hour: 100,
            reason: BlockedReason::LosingRecordParts { below_target: 2 },
        }];
        assert_eq!(report.blocked_buckets, expected);
        assert_eq!(report.buckets_migrated, 0, "nothing is served raw");
        assert_eq!(
            report.verification,
            Some(Verification::Stragglers {
                l0: 0,
                l1: 3,
                rewrite_parts: 0,
                blocked: expected,
            }),
            "the winner's one and the loser's two below-target parts all count \
             in l1; the loser names the bucket and the rewrite parts count nowhere"
        );
    }
}
