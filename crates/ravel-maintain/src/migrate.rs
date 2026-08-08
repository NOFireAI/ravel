//! Format-floor migration driver (epic EM, EM-T5; issues #770, #463).
//!
//! This is the resumable driver that walks one `(tenant, signal, format
//! family)`, rewrites every live record still below a target format version up
//! to it via the EM-T4 rewrite primitive ([`crate::rewrite::migrate_bucket_format`]),
//! and -- only once a fresh re-audit confirms nothing below the target survives
//! -- raises the recorded format floor
//! ([`ravel_catalog::raise_format_floor`], EM-T3).
//!
//! It reimplements neither the codec-level rewrite (that is EM-T4's
//! [`migrate_bucket_format`], called per bucket) nor the floor history (that is
//! EM-T3's append-only CAS machinery, called once at the end). It contributes
//! three things on top of them:
//!
//! 1. **A resumable cursor.** Progress is a `(shard, ingest_hour)` position
//!    persisted in object storage under the advisory-CAS cursor pattern
//!    [`crate::scan`] established (ADR-0018 §Decision 7): losing or corrupting
//!    it costs a rescan, never correctness. The cursor lets a huge migration
//!    run across many bounded invocations, each resuming exactly where the last
//!    stopped, never reprocessing an already-migrated bucket (the pre-migration
//!    per-bucket compaction-record check makes reprocessing a no-op even if the
//!    cursor is lost) and never skipping one (the walk order is total and the
//!    cursor only ever moves forward). The cursor exists only to carry an
//!    unfinished walk across invocations, so a walk that drains clears it: it
//!    is a position in one walk, not a permanent high-water mark, and a
//!    surviving one would make every bucket of every lower shard unreachable
//!    to the next invocation.
//!
//! 2. **A budget.** One invocation migrates at most [`MigrateBudget::max_records`]
//!    L0 records before persisting the cursor and returning control, so a very
//!    large migration never holds a lock or a long-lived process. The unit is
//!    records rather than wall time because the EM-T4 primitive is
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
//!    over a stale audit (EM-T10's named acceptance pattern).
//!
//! The floor is a claim about *every* live object of the family, so the
//! re-audit deliberately counts every live commit and compaction record
//! regardless of seal state: a fresh, still-unsealed below-target record the
//! walk could not yet migrate still blocks the raise, which is exactly the
//! guarantee the floor must carry.

use ravel_catalog::current_floor_from_store;
use ravel_commit::{keys, record};
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, UploadChecksum, Version,
    list_all,
};
use ravel_proto::commit::v1::CompactionRecord;
use ravel_types::{Signal, TenantHash};

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read::{list_bucket, load_inputs};
use crate::rewrite::{MigrateOutcome, migrate_bucket_format};

/// One-byte version tag on the advisory migrate-cursor payload. As with
/// [`crate::scan`]'s compaction cursor, this is not a frozen format: the tag
/// only lets a future encoding change be recognized and treated as "no usable
/// cursor" (rescan from the start), never silently misread.
const MIGRATE_CURSOR_TAG: u8 = 1;

/// Length of a well-formed cursor payload: tag + shard (u32 LE) + hour (u32 LE).
const MIGRATE_CURSOR_LEN: usize = 9;

/// The per-invocation work budget for [`migrate_family`], counted in L0 records
/// migrated. A budget bounds how much one invocation does before persisting its
/// cursor and returning control, so a large migration runs across many
/// invocations without a lock or a long-lived process.
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
    /// so the floor was left untouched. `l0` counts below-target commit records,
    /// `l1` counts below-target compaction parts.
    Stragglers { l0: usize, l1: usize },
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
    /// Buckets whose live L0 set was rewritten to the target this invocation.
    pub buckets_migrated: usize,
    /// L0 records migrated this invocation (the budget spend).
    pub records_migrated: u64,
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
    /// to raise the floor because stragglers remain. Callers use this to exit
    /// nonzero (the CLI) or log an alert (the maintain loop).
    pub fn stragglers_found(&self) -> bool {
        matches!(&self.verification, Some(v) if v.refused())
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
pub async fn count_below_target(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    scan_shards: u32,
    target_version: u32,
) -> Result<(usize, usize)> {
    use prost::Message;

    let mut l0_below = 0usize;
    let mut l1_below = 0usize;
    for shard in 0..scan_shards {
        let prefix = keys::commit_shard_prefix(tenant_hash, signal, shard)?;
        let metas = list_all(store, &prefix).await?;
        for meta in metas {
            match keys::partition_bucket_entry(&meta.key) {
                Ok(keys::BucketEntry::CommitRecord(_)) => {
                    let got = store.get(&meta.key, GetRange::Full).await?;
                    let rec = record::decode(&got.data)?;
                    if rec.segment_format_version < target_version {
                        l0_below += 1;
                    }
                }
                Ok(keys::BucketEntry::CompactionRecord(_)) => {
                    let got = store.get(&meta.key, GetRange::Full).await?;
                    let rec = CompactionRecord::decode(got.data.as_ref()).map_err(|err| {
                        MaintainError::Invariant(format!(
                            "compaction record {} is corrupt during migration re-audit: {err}",
                            meta.key
                        ))
                    })?;
                    for part in &rec.parts {
                        if part.segment_format_version < target_version {
                            l1_below += 1;
                        }
                    }
                }
                Ok(keys::BucketEntry::Tombstone(_)) => {}
                Err(e) => return Err(MaintainError::Key(e)),
            }
        }
    }
    Ok((l0_below, l1_below))
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
///    below the cursor, and for each sealed, un-tombstoned, not-yet-compacted
///    bucket carrying a below-target L0 record, rewrites its whole live L0 set
///    to the target via [`migrate_bucket_format`] (the EM-T4 primitive),
///    advancing the cursor past every examined bucket;
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
/// This is the entry point both `ravel-cli maintain migrate` and the server
/// maintain loop call; it takes only a store, an injected clock, and plain
/// parameters, matching the per-`(tenant, signal)` shape of the crate's other
/// maintenance drivers ([`crate::scan::scan_and_maintain`]).
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
            // Only a sealed, un-tombstoned, not-yet-compacted bucket carrying a
            // below-target L0 record is rewritten here; a compacted bucket's L1
            // parts are EM-T6's rewrite-on-touch scope, and the re-audit below
            // still refuses the floor raise if any such L1 straggler survives.
            let listing = list_bucket(store, &bucket).await?;
            if bucket.is_sealed(now, config)
                && listing.tombstone_key.is_none()
                && listing.compaction_record_keys.is_empty()
                && !listing.commit_keys.is_empty()
            {
                let inputs = load_inputs(store, &bucket, &listing.commit_keys).await?;
                let l0_below = inputs
                    .iter()
                    .filter(|i| i.record.segment_format_version < target_version)
                    .count() as u64;
                if l0_below > 0 {
                    match migrate_bucket_format(store, clock, config, &bucket, target_version)
                        .await?
                    {
                        MigrateOutcome::Rewritten { .. } => {
                            report.buckets_migrated += 1;
                            report.records_migrated += l0_below;
                            spent += l0_below;
                        }
                        // A concurrent compaction/tombstone/rewrite landed
                        // between our listing and the call: harmless, the other
                        // actor's result stands and the re-audit still gates the
                        // floor.
                        MigrateOutcome::NotSealed
                        | MigrateOutcome::Tombstoned
                        | MigrateOutcome::AlreadyCompacted
                        | MigrateOutcome::UpToDate => {}
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
    // asserted over a stale audit (EM-T10).
    let (l0_below, l1_below) =
        count_below_target(store, &tenant_hash, signal, scan_shards, target_version).await?;
    if l0_below + l1_below > 0 {
        report.verification = Some(Verification::Stragglers {
            l0: l0_below,
            l1: l1_below,
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
    //! Exercises the driver end to end on a `MemoryStore` over real v6 RSEG
    //! objects: the resumable cursor and budget (a migration interrupted after
    //! partial progress resumes exactly where it stopped, migrating every bucket
    //! exactly once), the floor-raise race close (a fresh straggler between the
    //! walk and the re-audit refuses the raise), and the basic end-to-end raise
    //! (a drained walk with no stragglers raises the floor and a subsequent
    //! audit finds nothing below it).

    use ravel_commit::keys;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_segment::{
        IngestBounds, SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues, VERSION_V6,
    };
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantHash, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::{CompactorConfig, FixedClock};

    const TENANT: &str = "acme";
    const FAMILY: &str = "rseg";
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;
    /// A version above the current writer's output, so every real v6 object
    /// counts as "below target" and the rewrite path runs. Stands in for the
    /// N-1 version an actual reader window would supply.
    const FUTURE_VERSION: u32 = VERSION_V6 as u32 + 1;

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
    /// commit record as given. The object bytes are always real v6; only the
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
        // Shard 0 hours 100, 101; shard 1 hour 100. All real v6 objects.
        seed_at(&store, 0, 100, 1, "alpha", VERSION_V6 as u32).await;
        seed_at(&store, 0, 101, 2, "beta", VERSION_V6 as u32).await;
        seed_at(&store, 1, 100, 3, "gamma", VERSION_V6 as u32).await;

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

    /// Race safety (EM-T10's named acceptance pattern): the floor raise refuses
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
        let target = VERSION_V6 as u32;

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
            Some(Verification::Stragglers { l0: 1, l1: 0 }),
            "exactly the one below-target straggler is reported"
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
        let target = VERSION_V6 as u32;
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
        let (l0_below, l1_below) =
            count_below_target(&store, &tenant_hash(), Signal::Metrics, 2, target)
                .await
                .expect("re-audit");
        assert_eq!((l0_below, l1_below), (0, 0));
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
        seed_at(&store, 0, 100, 1, "alpha", VERSION_V6 as u32).await;
        seed_at(&store, 1, 100, 2, "beta", VERSION_V6 as u32).await;

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
        seed_at(&store, 0, 200, 3, "gamma", VERSION_V6 as u32).await;

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
        let target = VERSION_V6 as u32;
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
}
