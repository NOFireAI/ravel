//! Sealed-bucket scan with the advisory CAS cursor
//! (docs/compaction-retention-plan.md §3.2, ADR-0018). Walks the hours of one
//! `(tenant, signal, shard)` upward from the cursor, compacting every sealed,
//! eligible bucket, and advances the cursor past the buckets it finished. The
//! cursor is advisory mutable state (the ADR-0003 HEAD-pointer precedent):
//! losing or corrupting it costs a rescan, never correctness.

use std::collections::{HashMap, HashSet};

use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_types::{Signal, TenantHash};

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::compact::{CompactionOutcome, compact_bucket};
use crate::config::{CompactorConfig, NS_PER_HOUR, RetentionConfig};
use crate::error::{MaintainError, Result};
use crate::retention::{RetentionOutcome, maintain_bucket};
use crate::sweep::LeaseCheck;

/// One-byte version tag on the advisory cursor payload. The cursor is not a
/// frozen format; the tag only lets a future encoding change be detected and
/// treated as "no usable cursor" (rescan), never misread.
const CURSOR_TAG: u8 = 1;

/// Outcome of one scan pass over a `(tenant, signal, shard)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanReport {
    /// Buckets newly compacted this pass.
    pub compacted: usize,
    /// Sealed buckets found already done (already-compacted, tombstoned, or
    /// below the input threshold).
    pub already_done: usize,
    /// Buckets skipped because they are not yet sealed (scan stops there).
    pub not_sealed: usize,
    /// The hour the cursor was advanced to, if it moved.
    pub cursor_advanced_to: Option<u32>,
}

/// Scan and compact every eligible sealed bucket for one `(tenant, signal,
/// shard)`, then advance the advisory cursor. Idempotent: re-running after a
/// crash reprocesses at most the buckets past the last persisted cursor, and
/// each bucket's own idempotency (plan §3.4) makes reprocessing harmless.
pub async fn scan_and_compact(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    tenant_hash: TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<ScanReport> {
    let cursor_key = keys::maint_cursor_key(&tenant_hash, signal, shard)?;
    let cursor = read_cursor(store, &cursor_key).await?;
    let start_after = cursor.as_ref().map(|(hour, _)| *hour);

    let hours = list_shard_hours(store, &tenant_hash, signal, shard).await?;

    let mut report = ScanReport {
        compacted: 0,
        already_done: 0,
        not_sealed: 0,
        cursor_advanced_to: None,
    };
    let mut highest_done: Option<u32> = None;

    for hour in hours {
        if start_after.is_some_and(|after| hour <= after) {
            continue;
        }
        let bucket = Bucket::new(tenant_hash, signal, shard, hour);
        match compact_bucket(store, clock, config, &bucket).await? {
            CompactionOutcome::NotSealed => {
                // Hours are ascending, so every later bucket is also unsealed.
                report.not_sealed += 1;
                break;
            }
            CompactionOutcome::Compacted { .. } => {
                report.compacted += 1;
                highest_done = Some(hour);
            }
            CompactionOutcome::AlreadyCompacted
            | CompactionOutcome::Tombstoned
            | CompactionOutcome::BelowMinInputs { .. } => {
                report.already_done += 1;
                highest_done = Some(hour);
            }
        }
    }

    if let Some(hour) = highest_done {
        // Advisory: a lost CAS race just means another maintainer already
        // moved the cursor, so treat AlreadyExists/PreconditionFailed as fine.
        write_cursor(store, &cursor_key, hour, cursor.map(|(_, v)| v)).await?;
        report.cursor_advanced_to = Some(hour);
    }

    Ok(report)
}

/// Outcome of one full-scan maintenance pass over a `(tenant, signal, shard)`
/// ([`scan_and_maintain`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaintainReport {
    /// Sealed buckets whose retention pass wrote or already held a tombstone
    /// (expired), or that were physically swept this pass.
    pub retired: usize,
    /// Buckets compacted this pass (retention left them live and they were
    /// eligible).
    pub compacted: usize,
    /// Buckets already compacted / below the input threshold (retention left
    /// them live, compaction found nothing to do).
    pub already_done: usize,
    /// Buckets skipped because not yet sealed.
    pub not_sealed: usize,
    /// Buckets skipped this pass because the [`MaintainMemo`] already knows them
    /// terminal (issue #280), so no per-bucket LIST/GET was issued for them.
    /// Always zero on a cold pass and for the non-memoized
    /// [`scan_and_maintain`] entry point.
    pub skipped_terminal: usize,
}

/// List every ingest-hour bucket present under one `(tenant, signal, shard)`,
/// ascending. Shared by [`scan_and_compact`] and [`scan_and_maintain`]; a
/// non-hour common prefix under the shard is layout drift and errors.
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
        // common == "<shard_prefix><hour>/"; extract the hour segment.
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

/// Default full re-verify interval for [`MaintainMemo`] (1 hour). A bucket the
/// memo has marked terminal is re-listed and re-evaluated at least this often,
/// bounding how long a memo entry may be stale -- a bucket that became
/// retention-expired, or an entry corrupted in memory -- before the maintain
/// loop acts on it. One hour is far below any retention window (days) and any
/// protection horizon, so the deferred action is never a correctness problem,
/// only a small bound on promptness (issue #280).
pub const DEFAULT_MEMO_REVERIFY_INTERVAL_NS: i64 = NS_PER_HOUR;

/// Memo key: `(tenant, signal, shard, ingest-hour)`, the full identity of one
/// bucket. Tenant and signal are included so a single per-worker memo can span
/// every `(signal, shard)` the worker maintains without cross-bucket aliasing.
type BucketKey = (TenantHash, Signal, u32, u32);

/// Why a bucket is terminal for the maintain loop: no retention or compaction
/// action is due now, and none can become due until either a later retention
/// expiry (caught by the memo's periodic re-verify) or never. Recorded for
/// observability and tests; the skip decision itself needs only an entry's
/// presence and freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalState {
    /// A compaction record is present and retention left the bucket live
    /// ("compacted-and-sweep-complete"): compaction is done, and only a later
    /// retention expiry can change the bucket.
    Compacted,
    /// Sealed, below `min_compaction_inputs`, retention left it live: a sealed
    /// bucket's input set is frozen, so it can never reach the threshold and
    /// will never compact; only a later retention expiry can change it.
    BelowThreshold,
    /// Tombstoned and physically swept empty ("tombstone-swept-empty"): the
    /// bucket holds nothing further to do. (It normally also vanishes from the
    /// shard listing, so its entry is pruned on the next pass.)
    SweptEmpty,
}

/// One memo entry: the terminal classification and the injected time the bucket
/// was last verified against object storage.
#[derive(Debug, Clone, Copy)]
struct MemoEntry {
    state: TerminalState,
    verified_at_ns: i64,
}

/// Per-worker in-memory memo of terminal bucket states (issue #280).
///
/// The full-scan-every-tick maintain loop is correct but re-lists and re-reads
/// every retained bucket on every tick, at roughly two LISTs plus a few GETs
/// per sealed bucket, growing linearly with the retention window. Most of those
/// buckets are terminal -- compacted-and-not-yet-expired, below-threshold, or
/// already swept empty -- and re-evaluating them re-issues identical store
/// reads for an identical no-op. This memo records the terminal ones per
/// [`BucketKey`] so [`scan_and_maintain_with_memo`] skips re-listing them, until
/// a periodic full re-verify (`reverify_interval_ns`) forces a fresh
/// evaluation.
///
/// The memo is **ephemeral and never correctness-bearing**. It lives only in
/// one worker's memory and is reconstructible from object storage at any time:
/// on worker restart it is cold (empty), giving exactly one full rescan
/// identical to the pre-memo behavior. If an entry is wrong, absent, or
/// corrupted, the worst case is a bucket re-evaluated late (bounded by the
/// re-verify interval) or redundant work -- never a missed retention or
/// compaction action, and never an object deleted early. No durability,
/// visibility, or query path reads it.
#[derive(Debug, Clone)]
pub struct MaintainMemo {
    entries: HashMap<BucketKey, MemoEntry>,
    reverify_interval_ns: i64,
}

impl MaintainMemo {
    /// A cold memo that re-verifies each terminal bucket at least every
    /// `reverify_interval_ns`. A non-positive interval disables skipping
    /// entirely (every terminal entry is always treated as stale), reproducing
    /// the pre-memo full-scan-every-tick behavior.
    pub fn new(reverify_interval_ns: i64) -> Self {
        MaintainMemo {
            entries: HashMap::new(),
            reverify_interval_ns,
        }
    }

    /// A cold memo with the default re-verify interval
    /// ([`DEFAULT_MEMO_REVERIFY_INTERVAL_NS`]).
    pub fn with_default_interval() -> Self {
        MaintainMemo::new(DEFAULT_MEMO_REVERIFY_INTERVAL_NS)
    }

    /// The re-verify interval in nanoseconds.
    pub fn reverify_interval_ns(&self) -> i64 {
        self.reverify_interval_ns
    }

    /// Number of memoized terminal buckets (introspection and tests).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the memo holds no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The memoized terminal state of one bucket, if any (introspection and
    /// tests). Returns `None` when the bucket is not memoized, regardless of
    /// freshness.
    pub fn terminal_state(
        &self,
        tenant_hash: TenantHash,
        signal: Signal,
        shard: u32,
        hour: u32,
    ) -> Option<TerminalState> {
        self.entries
            .get(&(tenant_hash, signal, shard, hour))
            .map(|entry| entry.state)
    }

    /// Whether `key` is memoized terminal and still fresh at `now_ns` (last
    /// verified within the re-verify interval), so its bucket may be skipped
    /// this tick. A non-positive interval is never fresh.
    fn is_fresh_terminal(&self, key: &BucketKey, now_ns: i64) -> bool {
        match self.entries.get(key) {
            Some(entry) => {
                self.reverify_interval_ns > 0
                    && now_ns.saturating_sub(entry.verified_at_ns) < self.reverify_interval_ns
            }
            None => false,
        }
    }

    /// Record `key` as terminal in `state`, verified at `now_ns`.
    fn mark_terminal(&mut self, key: BucketKey, state: TerminalState, now_ns: i64) {
        self.entries.insert(
            key,
            MemoEntry {
                state,
                verified_at_ns: now_ns,
            },
        );
    }

    /// Forget `key` (it is no longer terminal).
    fn forget(&mut self, key: &BucketKey) {
        self.entries.remove(key);
    }

    /// Split off the memo entries for one `(tenant, signal, shard)` unit into a
    /// standalone memo carrying the same re-verify interval, removing them from
    /// `self`. For the bounded-concurrent per-unit walk (ADR-0065 decision 2):
    /// each owned unit maintains its own slice of the memo independently on its
    /// own future, and [`Self::merge_unit`] folds the slice back once the unit's
    /// concurrent tick completes. Distinct units never share a [`BucketKey`], so
    /// split-then-merge is lossless.
    pub fn split_unit(&mut self, tenant: TenantHash, signal: Signal, shard: u32) -> MaintainMemo {
        let mut moved = HashMap::new();
        self.entries.retain(|(t, s, sh, hour), entry| {
            if *t == tenant && *s == signal && *sh == shard {
                moved.insert((*t, *s, *sh, *hour), *entry);
                false
            } else {
                true
            }
        });
        MaintainMemo {
            entries: moved,
            reverify_interval_ns: self.reverify_interval_ns,
        }
    }

    /// Merge a unit memo produced by [`Self::split_unit`] back into `self` after
    /// its concurrent tick. The unit owns a disjoint [`BucketKey`] space, so
    /// this never overwrites another unit's entry.
    pub fn merge_unit(&mut self, unit: MaintainMemo) {
        self.entries.extend(unit.entries);
    }

    /// Drop entries for `(tenant, signal, shard)` whose hour is absent from
    /// `present`, bounding memory to buckets that still exist. Entries for other
    /// shards, signals, or tenants are untouched.
    fn retain_present(
        &mut self,
        tenant: TenantHash,
        signal: Signal,
        shard: u32,
        present: &HashSet<u32>,
    ) {
        self.entries.retain(|(t, s, sh, hour), _| {
            *t != tenant || *s != signal || *sh != shard || present.contains(hour)
        });
    }
}

/// Classify one bucket's maintain outcome as terminal (memoizable) or not.
/// Terminal means this tick did no durable work and the next tick would repeat
/// the identical no-op store reads until a later retention expiry, or forever.
fn classify_terminal(
    retention: &RetentionOutcome,
    compaction: &Option<CompactionOutcome>,
) -> Option<TerminalState> {
    match retention {
        // Physically swept empty: nothing remains in the bucket.
        RetentionOutcome::Swept => Some(TerminalState::SweptEmpty),
        // A tombstone is present but the horizon-gated sweep is still pending,
        // or a sweep left residue: real work is due on a later tick.
        RetentionOutcome::Tombstoned | RetentionOutcome::SweptPartial => None,
        // Retention left the bucket live; the compaction outcome decides.
        RetentionOutcome::NoPolicy | RetentionOutcome::NotSealed | RetentionOutcome::NotExpired => {
            match compaction {
                Some(CompactionOutcome::AlreadyCompacted) => Some(TerminalState::Compacted),
                Some(CompactionOutcome::BelowMinInputs { .. }) => {
                    Some(TerminalState::BelowThreshold)
                }
                // Just compacted this tick: re-verify next tick to reach the stable
                // AlreadyCompacted state before memoizing it.
                Some(CompactionOutcome::Compacted { .. }) => None,
                // Not sealed yet (the newest buckets): keep evaluating every tick
                // until the seal margin passes.
                Some(CompactionOutcome::NotSealed) | None => None,
                // A tombstone appeared between the retention and compaction reads:
                // not a steady state, let the next tick re-evaluate.
                Some(CompactionOutcome::Tombstoned) => None,
            }
        }
    }
}

/// Run retention-before-compaction over *every* sealed bucket of one
/// `(tenant, signal, shard)`, via [`maintain_bucket`]. Unlike
/// [`scan_and_compact`], this does NOT use the advisory compaction cursor: the
/// cursor advances monotonically past done buckets and never revisits them,
/// but retention must re-evaluate every sealed bucket on every pass (a bucket
/// compacted long ago becomes retention-expired only later, and a tombstoned
/// bucket needs a later pass to run its horizon-gated physical sweep once the
/// protection horizon has elapsed). A cursor-skipping driver would silently
/// never retire aging data. So this walks all hours each pass, matching the
/// cursorless full-scan model [`crate::sweep::sweep_shard`] uses, and pairs
/// with a `sweep_shard` call for the same shard to run all three deletion
/// paths per tick (plan §8). Idempotent: `maintain_bucket` and every rule it
/// drives converge on re-run.
#[allow(clippy::too_many_arguments)]
pub async fn scan_and_maintain(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    retention: &RetentionConfig,
    lease: &dyn LeaseCheck,
    tenant_hash: TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<MaintainReport> {
    // A cold, discarded memo skips nothing (it is empty for the whole call), so
    // this is byte-for-byte the pre-memo full-scan-every-tick behavior. A driver
    // that wants the steady-state skip persists one MaintainMemo per worker
    // across ticks and calls scan_and_maintain_with_memo instead.
    let mut memo = MaintainMemo::new(0);
    scan_and_maintain_with_memo(
        &mut memo,
        store,
        clock,
        config,
        retention,
        lease,
        tenant_hash,
        signal,
        shard,
    )
    .await
}

/// [`scan_and_maintain`] with a caller-owned [`MaintainMemo`] persisted across
/// ticks (issue #280). Buckets the memo already knows terminal
/// (compacted-and-not-expired, below-threshold, or swept empty) are skipped
/// without any per-bucket LIST or GET, until the memo's periodic re-verify
/// interval forces a fresh evaluation. On the first tick after a worker start
/// the memo is empty, so this does exactly one full rescan identical to
/// [`scan_and_maintain`].
///
/// The memo is advisory and never correctness-bearing (see [`MaintainMemo`]):
/// the eventually-consistent full re-verify still re-evaluates every retained
/// bucket, so no retention or compaction action is ever missed, only deferred
/// by at most the re-verify interval.
#[allow(clippy::too_many_arguments)]
pub async fn scan_and_maintain_with_memo(
    memo: &mut MaintainMemo,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    retention: &RetentionConfig,
    lease: &dyn LeaseCheck,
    tenant_hash: TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<MaintainReport> {
    let now = clock.now_ns();
    let hours = list_shard_hours(store, &tenant_hash, signal, shard).await?;
    let present: HashSet<u32> = hours.iter().copied().collect();

    let mut report = MaintainReport::default();
    for hour in hours {
        let key: BucketKey = (tenant_hash, signal, shard, hour);

        // Steady state: the memo already proved this bucket terminal and the
        // entry is still fresh, so skip it without listing or reading anything.
        if memo.is_fresh_terminal(&key, now) {
            report.skipped_terminal += 1;
            continue;
        }

        let bucket = Bucket::new(tenant_hash, signal, shard, hour);
        let (retention_outcome, compaction) =
            maintain_bucket(store, clock, config, retention, lease, &bucket).await?;
        match retention_outcome {
            // The bucket is (being) retired; compaction was skipped by design.
            RetentionOutcome::Tombstoned
            | RetentionOutcome::Swept
            | RetentionOutcome::SweptPartial => {
                report.retired += 1;
            }
            // Retention left the bucket live; the compaction outcome classifies
            // it (compaction always ran in these arms; see maintain_bucket).
            RetentionOutcome::NoPolicy
            | RetentionOutcome::NotSealed
            | RetentionOutcome::NotExpired => match compaction {
                Some(CompactionOutcome::NotSealed) => report.not_sealed += 1,
                Some(CompactionOutcome::Compacted { .. }) => report.compacted += 1,
                Some(
                    CompactionOutcome::AlreadyCompacted
                    | CompactionOutcome::Tombstoned
                    | CompactionOutcome::BelowMinInputs { .. },
                ) => report.already_done += 1,
                None => {}
            },
        }

        // Update the memo from this fresh, authoritative evaluation: remember a
        // newly terminal bucket, and forget one that transitioned away from a
        // terminal state (e.g. a compacted bucket that just became expired).
        match classify_terminal(&retention_outcome, &compaction) {
            Some(state) => memo.mark_terminal(key, state, now),
            None => memo.forget(&key),
        }
    }

    // Bound memory to buckets that still exist: a swept-empty bucket disappears
    // from the shard listing entirely, and its stale memo entry can be dropped.
    memo.retain_present(tenant_hash, signal, shard, &present);
    Ok(report)
}

/// Read the advisory cursor. Returns the recorded hour and the object version
/// (for the next CAS). A decode failure is treated as no cursor (advisory
/// rescan), never an error; store faults propagate.
async fn read_cursor(store: &dyn ObjectStoreBackend, key: &str) -> Result<Option<(u32, Version)>> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            let data = got.data;
            if data.len() == 5 && data[0] == CURSOR_TAG {
                let hour = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                Ok(Some((hour, got.version)))
            } else {
                // Unrecognized cursor payload: ignore it (rescan from zero),
                // but keep the version so we can CAS-overwrite it cleanly.
                Ok(Some((0, got.version)))
            }
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// Write the advisory cursor. First write is `CreateIfAbsent`; subsequent
/// writes CAS against the version we read. A lost race
/// (`AlreadyExists`/`PreconditionFailed`) is not an error: the cursor is
/// advisory and another maintainer's update is equally valid.
async fn write_cursor(
    store: &dyn ObjectStoreBackend,
    key: &str,
    hour: u32,
    prev_version: Option<Version>,
) -> Result<()> {
    let mut payload = Vec::with_capacity(5);
    payload.push(CURSOR_TAG);
    payload.extend_from_slice(&hour.to_le_bytes());
    let mode = match prev_version {
        Some(v) => PutMode::CasVersion(v),
        None => PutMode::CreateIfAbsent,
    };
    let opts = PutOptions {
        mode,
        checksum: Some(ravel_object_store::UploadChecksum::Crc32c(crc32c::crc32c(
            &payload,
        ))),
    };
    match store.put(key, payload.into(), opts).await {
        Ok(_) | Err(StoreError::AlreadyExists) | Err(StoreError::PreconditionFailed) => Ok(()),
        Err(e) => Err(MaintainError::Store(e)),
    }
}
