//! Sealed-bucket scan with the advisory CAS cursor
//! (ADR-0018). Walks the hours of one
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
use crate::retention::{
    RetentionOutcome, SnapshotBlock, SnapshotReachability, maintain_bucket_with_reach,
};
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
/// each bucket's own idempotency makes reprocessing harmless.
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
    /// Expired buckets whose physical sweep was blocked because the live
    /// catalog HEAD snapshot still names an object inside the bucket
    /// (ADR-0020 delete-blocker, [`RetentionOutcome::BlockedBySnapshot`] with
    /// [`crate::retention::SnapshotBlock::Named`]). Nothing was deleted and the
    /// tombstone was left in place; a later sweep finishes once the fold's
    /// retention-frontier reconcile drops the bucket from the snapshot. A
    /// nonzero steady-state value means the fold is lagging the sweep.
    pub blocked_by_snapshot: usize,
    /// Expired buckets whose physical sweep was blocked fail-closed because
    /// HEAD, or a snapshot part covering the bucket's hour, could not be read
    /// ([`RetentionOutcome::BlockedBySnapshot`] with
    /// [`crate::retention::SnapshotBlock::Unreadable`]). Non-reachability
    /// cannot be proven from data that cannot be read, so the delete is
    /// refused. A persistent nonzero value is an operator signal that a HEAD or
    /// part object is corrupt or missing, not the ordinary lagging-fold case.
    pub blocked_by_unreadable_head: usize,
    /// Buckets skipped this pass because the [`MaintainMemo`] already knows them
    /// terminal, so no per-bucket LIST/GET was issued for them.
    /// Always zero on a cold pass and for the non-memoized
    /// [`scan_and_maintain`] entry point.
    pub skipped_terminal: usize,
    /// Hours this pass classified as [`Zone::Head`] or [`Zone::Tail`], i.e.
    /// evaluated every tick regardless of the memo. The maintain driver reuses
    /// this set as the hour scope for the same tick's [`crate::sweep::sweep_shard_zoned`]
    /// call (ADR-0065 decision 3), so the sweep never issues a second LIST of
    /// the shard's hours to compute it.
    pub head_tail_hours: Vec<u32>,
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

/// A unit's hour zone (ADR-0065 decision 3). Recomputed fresh every tick
/// from `now_ns` and the tenant's config by [`classify_zone`] -- never
/// stored -- because both the head frontier and, when a retention policy
/// exists, the tail window move with the clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Zone {
    /// Newer than the seal margin plus one hour of slack: sealing and first
    /// compaction still happen here. Evaluated every tick, bypassing
    /// [`MaintainMemo`] entirely, exactly as before this zone split existed.
    Head,
    /// From the bucket's computed retention expiry through the protection
    /// horizon past it: tombstoning and the horizon-gated physical sweep
    /// both happen in this window. Evaluated every tick, bypassing the
    /// memo, same as `Head`. Unreachable when the tenant has no retention
    /// policy (nothing ever expires, so there is no tail).
    Tail,
    /// Below the head frontier and before its computed retention expiry (or
    /// the tenant has no retention policy at all): nothing can change a
    /// terminal bucket here except that future expiry, an operator action
    /// (tombstone, hold), or a future EJ erasure order
    /// ([`MaintainMemo::invalidate`] is the seam for the latter two).
    /// Memo-gated: skipped while a terminal entry is fresh, re-verified once
    /// `CompactorConfig::interior_reverify_ns` has elapsed since the last
    /// verification, or immediately once the zone recomputes to `Tail` at
    /// the bucket's actual expiry.
    Interior,
}

/// Classify `hour` into a [`Zone`] for `now_ns` (ADR-0065 decision 3).
/// `retention_window_ns` is `None` when the tenant has no retention policy,
/// in which case no hour is ever `Tail`.
///
/// The tail window anchors on `hour`'s own nominal bucket-end timestamp
/// (`(hour + 1) * NS_PER_HOUR`) plus the window, not on the bucket's actual
/// maximum event timestamp: the real expiry check
/// ([`crate::retention::is_expired`]) reads decoded commit and compaction
/// records and is only exact at evaluation time inside `maintain_bucket`,
/// but this function is a scheduling heuristic, not a correctness gate --
/// it only decides when to look, never whether to delete. Because a sealed
/// bucket's events fall inside its own hour
/// (`max_event_ts <= bucket_end_ns`, modulo the clock-skew allowance already
/// folded into the seal margin), this heuristic's computed expiry instant is
/// always at or after the bucket's true expiry, so the zone transition into
/// `Tail` -- and thus tick-cadence evaluation -- can lag the true expiry by
/// at most about one hour. That lag is bounded well inside
/// `protection_horizon_ns` (which must clear `max_query_duration + grace`),
/// so it costs a small, documented scheduling delay, never a promptness or
/// safety violation.
pub fn classify_zone(
    hour: u32,
    now_ns: i64,
    config: &CompactorConfig,
    retention_window_ns: Option<i64>,
) -> Zone {
    let bucket_end_ns = i64::from(hour)
        .saturating_add(1)
        .saturating_mul(NS_PER_HOUR);
    let head_frontier_ns =
        now_ns.saturating_sub(config.seal_margin_ns().saturating_add(NS_PER_HOUR));
    if bucket_end_ns > head_frontier_ns {
        return Zone::Head;
    }
    if let Some(window_ns) = retention_window_ns {
        let expiry_ns = bucket_end_ns.saturating_add(window_ns);
        let tail_hi = expiry_ns.saturating_add(config.protection_horizon_ns);
        if now_ns >= expiry_ns && now_ns <= tail_hi {
            return Zone::Tail;
        }
    }
    Zone::Interior
}

/// Default full re-verify interval for [`MaintainMemo`] (1 hour). A bucket the
/// memo has marked terminal is re-listed and re-evaluated at least this often,
/// bounding how long a memo entry may be stale -- a bucket that became
/// retention-expired, or an entry corrupted in memory -- before the maintain
/// loop acts on it. One hour is far below any retention window (days) and any
/// protection horizon, so the deferred action is never a correctness problem,
/// only a small bound on promptness.
pub const DEFAULT_MEMO_REVERIFY_INTERVAL_NS: i64 = NS_PER_HOUR;

/// Staleness bound past which a durable memo snapshot (ADR-0065 decision 3,
/// `sys/maintain/memo/<process_id>`) is not trusted for warm start or handoff
/// seeding: a snapshot whose `snapshot_unix_ns` is further than this from the
/// reader's clock (in either direction) is ignored, degrading that unit to the
/// cold rescan we do unconditionally today.
///
/// This is deliberately an **independent** bound, not the worker heartbeat's
/// `3 * H` liveness window, and it is set to the memo re-verify interval
/// ([`DEFAULT_MEMO_REVERIFY_INTERVAL_NS`], 1 hour). The justification is exact:
/// a snapshot is written at tick time and every entry it carries was verified
/// at or before that write, so `snapshot_unix_ns >= max(entry.verified_at_ns)`.
/// Past one re-verify interval, `now - snapshot_unix_ns > reverify_interval`
/// implies `now - entry.verified_at_ns > reverify_interval` for *every* entry,
/// so no seeded entry could pass [`MaintainMemo::is_fresh_terminal`] and none
/// could suppress a single read -- loading such a snapshot is pure waste. Tying
/// the bound to the re-verify interval makes it precisely "trust a snapshot
/// only while at least its newest possible entry could still be fresh."
///
/// Coupling to worker liveness (`3 * H` = 180s) would be wrong here in the
/// other direction: a live worker whose memo is stable does not rewrite its
/// snapshot every tick (writes are debounced), so its own last snapshot can be
/// many minutes old while the worker is perfectly healthy; a 180s bound would
/// wrongly reject that worker's own snapshot on a warm restart. The re-verify
/// interval is the only bound that matches what the snapshot is actually for.
/// A far-future-dated snapshot (clock skew) is excluded symmetrically, exactly
/// as [`crate::worker_set`] excludes a far-future heartbeat.
pub const DEFAULT_MEMO_SNAPSHOT_STALENESS_NS: i64 = DEFAULT_MEMO_REVERIFY_INTERVAL_NS;

/// Version tag on the durable memo snapshot payload (ADR-0065 decision 3). Like
/// the advisory cursor's [`CURSOR_TAG`], the snapshot is not a frozen format:
/// the tag only lets a future encoding be detected and treated as "no usable
/// snapshot" (cold rescan), never misread.
const MEMO_SNAPSHOT_TAG: u8 = 1;

/// Memo key: `(tenant, signal, shard, ingest-hour)`, the full identity of one
/// bucket. Tenant and signal are included so a single per-worker memo can span
/// every `(signal, shard)` the worker maintains without cross-bucket aliasing.
type BucketKey = (TenantHash, Signal, u32, u32);

/// Snapshot grouping key for one `(tenant, signal-code, shard)` unit, with the
/// signal held as its one-byte snapshot discriminant so the map orders
/// deterministically (required for the debounce body to be a stable function of
/// the memo).
type SnapshotUnitKey = (TenantHash, u8, u32);

/// One unit's terminal buckets as `(hour, state, verified_at_ns)` before they
/// are run-length encoded into a frontier plus exceptions.
type SnapshotUnitEntries = Vec<(u32, TerminalState, i64)>;

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

impl TerminalState {
    /// Stable one-byte discriminant for the durable memo snapshot payload
    /// (ADR-0065 decision 3). Fixed values: a persisted snapshot must decode
    /// the same across versions, so these never change once shipped.
    fn to_code(self) -> u8 {
        match self {
            TerminalState::Compacted => 0,
            TerminalState::BelowThreshold => 1,
            TerminalState::SweptEmpty => 2,
        }
    }

    /// Inverse of [`Self::to_code`]. An unrecognized code is a future-version or
    /// corrupt snapshot; the decoder treats it as undecodable (cold rescan),
    /// never a panic.
    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(TerminalState::Compacted),
            1 => Some(TerminalState::BelowThreshold),
            2 => Some(TerminalState::SweptEmpty),
            _ => None,
        }
    }
}

/// One memo entry: the terminal classification and the injected time the bucket
/// was last verified against object storage.
#[derive(Debug, Clone, Copy)]
struct MemoEntry {
    state: TerminalState,
    verified_at_ns: i64,
}

/// Per-worker in-memory memo of terminal bucket states.
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
    /// When [`crate::sweep::sweep_shard`] (the full-keyspace safety-net pass,
    /// not [`crate::sweep::sweep_shard_zoned`]) last ran for a `(tenant,
    /// signal, shard)` unit, keyed the same way [`Self::split_unit`] and
    /// [`Self::merge_unit`] already partition entries, so it rides the same
    /// per-unit split/merge without its own concurrency plumbing. Absent means
    /// never (a cold worker's first tick), which [`Self::full_sweep_due`]
    /// treats as due, matching this memo's own cold-start behavior.
    last_full_sweep_ns: HashMap<(TenantHash, Signal, u32), i64>,
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
            last_full_sweep_ns: HashMap::new(),
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

    /// Mark `hours` of `(tenant, signal, shard)` as due for re-evaluation on
    /// the next tick, regardless of interior re-verify cadence.
    ///
    /// This is the public invalidate seam named in ADR-0065: callers outside
    /// this crate (the erasure rewrite work orders) use
    /// it to force a specific interior hour back through `maintain_bucket`
    /// without waiting for the slow safety-net sweep or the interior
    /// re-verify interval to elapse. It is a thin wrapper over [`Self::forget`]:
    /// a forgotten entry has no memoized verdict, so `is_fresh_terminal`
    /// reports it as not fresh and the next tick re-evaluates it. Because
    /// there is nothing to encode, this trivially survives a memo snapshot
    /// save/load round-trip (there's no stale entry to reseed).
    ///
    /// Known gap, not addressed here: if a sibling worker's own snapshot
    /// (taken before the invalidation, but merged in after it) re-seeds this
    /// key as terminal via `seed_from_snapshot`'s freshest-verdict-wins rule,
    /// and that snapshot's `verified_at_ns` is later than this invalidation
    /// event, the entry can come back fresh before its next natural tick.
    /// Closing this needs a durable invalidation marker (a tombstone with its
    /// own timestamp) in the snapshot format, which is out of scope for this
    /// task.
    pub fn invalidate(&mut self, tenant: TenantHash, signal: Signal, shard: u32, hours: &[u32]) {
        for hour in hours {
            self.forget(&(tenant, signal, shard, *hour));
        }
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
        let mut moved_sweep = HashMap::new();
        if let Some(ns) = self.last_full_sweep_ns.remove(&(tenant, signal, shard)) {
            moved_sweep.insert((tenant, signal, shard), ns);
        }
        MaintainMemo {
            entries: moved,
            reverify_interval_ns: self.reverify_interval_ns,
            last_full_sweep_ns: moved_sweep,
        }
    }

    /// Merge a unit memo produced by [`Self::split_unit`] back into `self` after
    /// its concurrent tick. The unit owns a disjoint [`BucketKey`] space, so
    /// this never overwrites another unit's entry.
    pub fn merge_unit(&mut self, unit: MaintainMemo) {
        self.entries.extend(unit.entries);
        self.last_full_sweep_ns.extend(unit.last_full_sweep_ns);
    }

    /// Whether a full-keyspace sweep pass ([`crate::sweep::sweep_shard`]) is
    /// due for `(tenant, signal, shard)` at `now_ns`, i.e. it has never run or
    /// last ran at least `cadence_ns` ago. A non-positive `cadence_ns` is
    /// always due, mirroring [`Self::is_fresh_terminal`]'s non-positive-
    /// interval convention.
    pub fn full_sweep_due(
        &self,
        tenant: TenantHash,
        signal: Signal,
        shard: u32,
        now_ns: i64,
        cadence_ns: i64,
    ) -> bool {
        if cadence_ns <= 0 {
            return true;
        }
        match self.last_full_sweep_ns.get(&(tenant, signal, shard)) {
            Some(last_ns) => now_ns.saturating_sub(*last_ns) >= cadence_ns,
            None => true,
        }
    }

    /// Record that a full-keyspace sweep pass ran for `(tenant, signal,
    /// shard)` at `now_ns`, resetting [`Self::full_sweep_due`]'s clock.
    pub fn record_full_sweep(
        &mut self,
        tenant: TenantHash,
        signal: Signal,
        shard: u32,
        now_ns: i64,
    ) {
        self.last_full_sweep_ns
            .insert((tenant, signal, shard), now_ns);
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

    /// Seed one terminal entry from a durable snapshot (ADR-0065 decision 3),
    /// keeping the freshest verdict when a bucket is seeded from more than one
    /// snapshot: an existing entry verified at or after `verified_at_ns` wins,
    /// so merging siblings' snapshots converges on the most recently verified
    /// state per bucket regardless of read order.
    fn seed_entry(&mut self, key: BucketKey, state: TerminalState, verified_at_ns: i64) {
        match self.entries.get(&key) {
            Some(existing) if existing.verified_at_ns >= verified_at_ns => {}
            _ => {
                self.entries.insert(
                    key,
                    MemoEntry {
                        state,
                        verified_at_ns,
                    },
                );
            }
        }
    }

    /// The RLE-encoded snapshot **body** (ADR-0065 decision 3): every terminal
    /// bucket the memo holds, grouped per `(tenant, signal, shard)` unit, each
    /// unit summarized as a dense **frontier** run plus a sparse **exception**
    /// list, without the [`MEMO_SNAPSHOT_TAG`]/`snapshot_unix_ns` header. The
    /// body is a pure function of the memoized entries and is emitted in a
    /// canonical order (units sorted by `(tenant, signal-code, shard)`, hours
    /// ascending), so a driver can compare two bodies byte-for-byte to **debounce**
    /// the durable write (skip a tick that changed nothing) without the ever-
    /// changing timestamp forcing a write every tick.
    ///
    /// Encoding, per unit: the terminal hours are partitioned into maximal
    /// contiguous same-state runs; the single longest run (ties broken toward
    /// the highest hour) is the frontier -- the compacted interior of the
    /// retention window, one run covering hundreds of hours -- stored as
    /// `(frontier_start_hour, terminal_frontier_hour, state, verified_ns)`. Every
    /// other run is an exception, stored as `(start_hour, run_len, state,
    /// verified_ns)`. Each run's `verified_ns` is the *minimum* verified time
    /// across its hours, so a warm-started entry is never treated as fresher
    /// than its oldest member. See [`Self::encode_snapshot`] for the full framed
    /// object and [`Self::seed_from_snapshot`] for the inverse.
    pub fn snapshot_body(&self) -> Vec<u8> {
        // Group terminal entries per unit, deterministically ordered so the
        // body is a stable function of the memo (required for debounce).
        let mut by_unit: std::collections::BTreeMap<SnapshotUnitKey, SnapshotUnitEntries> =
            std::collections::BTreeMap::new();
        for (&(t, s, sh, hour), entry) in &self.entries {
            by_unit
                .entry((t, signal_to_code(s), sh))
                .or_default()
                .push((hour, entry.state, entry.verified_at_ns));
        }

        let mut buf = Vec::new();
        buf.extend_from_slice(&(by_unit.len() as u32).to_le_bytes());
        for ((tenant, signal_code, shard), mut hours) in by_unit {
            hours.sort_unstable_by_key(|(hour, _, _)| *hour);
            let runs = terminal_runs(&hours);
            // Frontier = the longest run; ties toward the highest end hour. The
            // retention-window interior is one run, so this is the compression.
            let frontier_idx = runs
                .iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| {
                    a.len()
                        .cmp(&b.len())
                        .then_with(|| a.end_hour.cmp(&b.end_hour))
                })
                .map(|(i, _)| i);

            buf.extend_from_slice(&tenant.0);
            buf.push(signal_code);
            buf.extend_from_slice(&shard.to_le_bytes());

            match frontier_idx {
                Some(idx) => {
                    let f = &runs[idx];
                    buf.push(1); // frontier present
                    buf.extend_from_slice(&f.start_hour.to_le_bytes());
                    buf.extend_from_slice(&f.end_hour.to_le_bytes());
                    buf.push(f.state.to_code());
                    buf.extend_from_slice(&f.verified_ns.to_le_bytes());

                    let exceptions: Vec<&TerminalRun> = runs
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != idx)
                        .map(|(_, r)| r)
                        .collect();
                    buf.extend_from_slice(&(exceptions.len() as u32).to_le_bytes());
                    for run in exceptions {
                        buf.extend_from_slice(&run.start_hour.to_le_bytes());
                        buf.extend_from_slice(&run.len().to_le_bytes());
                        buf.push(run.state.to_code());
                        buf.extend_from_slice(&run.verified_ns.to_le_bytes());
                    }
                }
                None => {
                    // A unit with an entry always has at least one run, so this
                    // arm is only reached for an entry-less unit, which the
                    // grouping above never produces; encode it as empty anyway.
                    buf.push(0); // no frontier
                    buf.extend_from_slice(&0u32.to_le_bytes()); // zero exceptions
                }
            }
        }
        buf
    }

    /// Frame a snapshot body ([`Self::snapshot_body`]) into the durable object
    /// bytes: `[MEMO_SNAPSHOT_TAG][snapshot_unix_ns i64 LE][body]`. Taking a
    /// pre-computed body lets a driver reuse the body it already built for the
    /// debounce comparison rather than re-encoding it.
    pub fn snapshot_bytes_from_body(body: &[u8], snapshot_unix_ns: i64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 8 + body.len());
        buf.push(MEMO_SNAPSHOT_TAG);
        buf.extend_from_slice(&snapshot_unix_ns.to_le_bytes());
        buf.extend_from_slice(body);
        buf
    }

    /// The full durable snapshot object (ADR-0065 decision 3), stamped with the
    /// writer's clock `snapshot_unix_ns` for the reader-side staleness check
    /// ([`DEFAULT_MEMO_SNAPSHOT_STALENESS_NS`]). Convenience over
    /// [`Self::snapshot_bytes_from_body`] when the caller does not separately
    /// need the body for debouncing.
    pub fn encode_snapshot(&self, snapshot_unix_ns: i64) -> Vec<u8> {
        Self::snapshot_bytes_from_body(&self.snapshot_body(), snapshot_unix_ns)
    }

    /// Seed this memo from one durable snapshot's bytes for the units `owns`
    /// accepts (ADR-0065 decision 3's warm start and handoff). Fail-open by
    /// contract: an undecodable or truncated payload returns an
    /// [`MemoSnapshotError`] the caller logs and treats as a cold start for
    /// those units, never a panic and never a blocked loop.
    ///
    /// `staleness_ns` gates the whole snapshot on its `snapshot_unix_ns`: a
    /// snapshot further than `staleness_ns` from `now_ns` in either direction is
    /// ignored ([`SeedStats::stale`] set, nothing seeded), matching the
    /// far-future exclusion [`crate::worker_set`] applies to heartbeats. A
    /// non-positive `staleness_ns` disables the whole-snapshot gate (every
    /// snapshot is loaded; per-entry freshness still governs skipping). Seeded
    /// entries only ever *suppress reads* through the same freshness rules as a
    /// warm in-memory entry ([`Self::is_fresh_terminal`]); the memo stays as
    /// advisory as ever, so a wrong seed can at worst defer a bucket's
    /// re-evaluation by one re-verify interval.
    pub fn seed_from_snapshot(
        &mut self,
        bytes: &[u8],
        now_ns: i64,
        staleness_ns: i64,
        owns: impl Fn(TenantHash, Signal, u32) -> bool,
    ) -> std::result::Result<SeedStats, MemoSnapshotError> {
        let decoded = decode_snapshot(bytes)?;

        if staleness_ns > 0 {
            let too_old = now_ns.saturating_sub(decoded.snapshot_unix_ns) > staleness_ns;
            let too_future = decoded.snapshot_unix_ns.saturating_sub(now_ns) > staleness_ns;
            if too_old || too_future {
                return Ok(SeedStats {
                    stale: true,
                    seeded_units: 0,
                    seeded_buckets: 0,
                });
            }
        }

        let mut stats = SeedStats {
            stale: false,
            seeded_units: 0,
            seeded_buckets: 0,
        };
        for unit in decoded.units {
            if !owns(unit.tenant, unit.signal, unit.shard) {
                continue;
            }
            stats.seeded_units += 1;
            for (hour, state, verified_ns) in unit.entries {
                // Clamp each entry's verified_ns to the snapshot's own
                // snapshot_unix_ns. By construction a snapshot is written at
                // tick time and every entry was verified at or before that
                // write, so `snapshot_unix_ns >= max(entry.verified_at_ns)`
                // always holds for an honest snapshot (see the staleness bound
                // doc above). A corrupted or clock-skewed entry with a future
                // verified_ns would otherwise read as eternally fresh through
                // `is_fresh_terminal` and, because the memo is re-encoded and
                // republished each cycle, self-propagate fleet-wide. The clamp
                // makes it structurally impossible for a decoded entry to be
                // fresher than the snapshot it came from, which the header-level
                // too_future guard has already bounded against `now_ns`.
                let verified_ns = verified_ns.min(decoded.snapshot_unix_ns);
                self.seed_entry(
                    (unit.tenant, unit.signal, unit.shard, hour),
                    state,
                    verified_ns,
                );
                stats.seeded_buckets += 1;
            }
        }
        Ok(stats)
    }
}

/// What one [`MaintainMemo::seed_from_snapshot`] call did, for logging and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SeedStats {
    /// The snapshot was excluded by the staleness gate; nothing was seeded.
    pub stale: bool,
    /// Owned units the snapshot carried entries for and seeded from.
    pub seeded_units: usize,
    /// Terminal bucket entries seeded (may include buckets already warm, which
    /// keep whichever verdict is fresher).
    pub seeded_buckets: usize,
}

/// A durable memo snapshot ([`MaintainMemo::encode_snapshot`]) that failed to
/// decode. Every variant means the same thing to the caller -- treat the
/// snapshot as absent and cold-start the affected units -- but they are
/// distinguished so a log line can say why a snapshot was skipped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MemoSnapshotError {
    /// The payload ended before a field the format requires was fully read.
    #[error("memo snapshot truncated: wanted {wanted} more bytes at offset {offset}")]
    Truncated { offset: usize, wanted: usize },
    /// The leading version tag is not one this build writes.
    #[error("memo snapshot has unknown version tag {0} (a future or corrupt encoding)")]
    UnknownTag(u8),
    /// A terminal-state discriminant outside [`TerminalState`]'s range.
    #[error("memo snapshot carries unknown terminal-state code {0}")]
    UnknownState(u8),
    /// A signal discriminant outside [`Signal`]'s range.
    #[error("memo snapshot carries unknown signal code {0}")]
    UnknownSignal(u8),
    /// An exception run with zero length, which the encoder never emits.
    #[error("memo snapshot carries a zero-length run (a corrupt encoding)")]
    ZeroLengthRun,
}

/// One maximal contiguous same-state run of terminal hours within a unit.
#[derive(Debug, Clone, Copy)]
struct TerminalRun {
    start_hour: u32,
    end_hour: u32,
    state: TerminalState,
    /// Minimum `verified_at_ns` across the run (conservative for freshness).
    verified_ns: i64,
}

impl TerminalRun {
    fn len(&self) -> u32 {
        self.end_hour - self.start_hour + 1
    }
}

/// Partition sorted `(hour, state, verified_ns)` entries into maximal runs of
/// consecutive hours sharing one [`TerminalState`]. A run breaks on a hour gap
/// or a state change. Input must be sorted ascending by hour with unique hours
/// (the memo keys one entry per bucket, so hours are unique by construction).
fn terminal_runs(sorted: &[(u32, TerminalState, i64)]) -> Vec<TerminalRun> {
    let mut runs: Vec<TerminalRun> = Vec::new();
    for &(hour, state, verified_ns) in sorted {
        match runs.last_mut() {
            Some(run)
                if run.end_hour + 1 == hour
                    && std::mem::discriminant(&run.state) == std::mem::discriminant(&state) =>
            {
                run.end_hour = hour;
                run.verified_ns = run.verified_ns.min(verified_ns);
            }
            _ => runs.push(TerminalRun {
                start_hour: hour,
                end_hour: hour,
                state,
                verified_ns,
            }),
        }
    }
    runs
}

/// One decoded unit of a snapshot: its identity plus every terminal bucket the
/// runs expanded to.
struct DecodedUnit {
    tenant: TenantHash,
    signal: Signal,
    shard: u32,
    entries: Vec<(u32, TerminalState, i64)>,
}

/// A fully decoded snapshot.
struct DecodedSnapshot {
    snapshot_unix_ns: i64,
    units: Vec<DecodedUnit>,
}

/// Stable one-byte discriminant for a [`Signal`] in the memo snapshot. Fixed
/// values (a persisted snapshot must decode identically across versions); every
/// signal is mapped, not just the maintained three, so a snapshot is total.
fn signal_to_code(signal: Signal) -> u8 {
    match signal {
        Signal::Metrics => 0,
        Signal::Logs => 1,
        Signal::Spans => 2,
        Signal::Profiles => 3,
        Signal::Alerts => 4,
        Signal::Audit => 5,
    }
}

/// Inverse of [`signal_to_code`]. An unknown code is a future-version or corrupt
/// snapshot, surfaced as [`MemoSnapshotError::UnknownSignal`].
fn signal_from_code(code: u8) -> Option<Signal> {
    match code {
        0 => Some(Signal::Metrics),
        1 => Some(Signal::Logs),
        2 => Some(Signal::Spans),
        3 => Some(Signal::Profiles),
        4 => Some(Signal::Alerts),
        5 => Some(Signal::Audit),
        _ => None,
    }
}

/// A bounds-checked cursor over the snapshot bytes; every read that would run
/// off the end is a typed [`MemoSnapshotError::Truncated`], never a panic.
struct SnapshotReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotReader<'a> {
    fn take(&mut self, n: usize) -> std::result::Result<&'a [u8], MemoSnapshotError> {
        let end = self
            .offset
            .checked_add(n)
            .ok_or(MemoSnapshotError::Truncated {
                offset: self.offset,
                wanted: n,
            })?;
        if end > self.bytes.len() {
            return Err(MemoSnapshotError::Truncated {
                offset: self.offset,
                wanted: n,
            });
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn u8(&mut self) -> std::result::Result<u8, MemoSnapshotError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> std::result::Result<u32, MemoSnapshotError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i64(&mut self) -> std::result::Result<i64, MemoSnapshotError> {
        let b = self.take(8)?;
        Ok(i64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

/// Decode a full durable snapshot object, expanding every frontier/exception
/// run back into per-bucket terminal entries. Every malformed input yields a
/// typed [`MemoSnapshotError`] (fail-open at the caller), never a panic.
fn decode_snapshot(bytes: &[u8]) -> std::result::Result<DecodedSnapshot, MemoSnapshotError> {
    let mut reader = SnapshotReader { bytes, offset: 0 };
    let tag = reader.u8()?;
    if tag != MEMO_SNAPSHOT_TAG {
        return Err(MemoSnapshotError::UnknownTag(tag));
    }
    let snapshot_unix_ns = reader.i64()?;
    let unit_count = reader.u32()?;

    let mut units = Vec::with_capacity(unit_count as usize);
    for _ in 0..unit_count {
        let tenant_bytes = reader.take(16)?;
        let mut tenant = [0u8; 16];
        tenant.copy_from_slice(tenant_bytes);
        let tenant = TenantHash(tenant);
        let signal_code = reader.u8()?;
        let signal =
            signal_from_code(signal_code).ok_or(MemoSnapshotError::UnknownSignal(signal_code))?;
        let shard = reader.u32()?;

        let mut entries: Vec<(u32, TerminalState, i64)> = Vec::new();

        let frontier_present = reader.u8()?;
        if frontier_present == 1 {
            let start = reader.u32()?;
            let end = reader.u32()?;
            let state_code = reader.u8()?;
            let state = TerminalState::from_code(state_code)
                .ok_or(MemoSnapshotError::UnknownState(state_code))?;
            let verified = reader.i64()?;
            expand_run(
                &mut entries,
                start,
                end.saturating_sub(start) + 1,
                state,
                verified,
            );
        }

        let exception_count = reader.u32()?;
        for _ in 0..exception_count {
            let start = reader.u32()?;
            let run_len = reader.u32()?;
            if run_len == 0 {
                return Err(MemoSnapshotError::ZeroLengthRun);
            }
            let state_code = reader.u8()?;
            let state = TerminalState::from_code(state_code)
                .ok_or(MemoSnapshotError::UnknownState(state_code))?;
            let verified = reader.i64()?;
            expand_run(&mut entries, start, run_len, state, verified);
        }

        units.push(DecodedUnit {
            tenant,
            signal,
            shard,
            entries,
        });
    }

    Ok(DecodedSnapshot {
        snapshot_unix_ns,
        units,
    })
}

/// Expand one RLE run of `run_len` consecutive hours from `start` into per-hour
/// terminal entries. Saturating arithmetic keeps a corrupt oversized run from
/// overflowing the hour index rather than panicking.
fn expand_run(
    out: &mut Vec<(u32, TerminalState, i64)>,
    start: u32,
    run_len: u32,
    state: TerminalState,
    verified_ns: i64,
) {
    for i in 0..run_len {
        let Some(hour) = start.checked_add(i) else {
            break;
        };
        out.push((hour, state, verified_ns));
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
        // a sweep left residue, or the sweep is blocked because the snapshot
        // still reaches the bucket (ADR-0020): real work is due on a later tick
        // (once the fold drops the bucket, or the residue/HEAD read clears), so
        // never memoize it as terminal.
        RetentionOutcome::Tombstoned
        | RetentionOutcome::SweptPartial
        | RetentionOutcome::BlockedBySnapshot(_) => None,
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
/// paths per tick. Idempotent: `maintain_bucket` and every rule it
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
/// ticks. Buckets the memo already knows terminal
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
    let retention_window_ns = retention.window_for(&tenant_hash);

    // One HEAD-reachability cache for the whole pass: the delete-blocker gate
    // (ADR-0020) reads the catalog HEAD at most once and each covering snapshot
    // part at most once across every bucket of this (tenant, signal, shard),
    // never once per bucket (ADR-0076 request cost).
    let mut reach = SnapshotReachability::new();

    let mut report = MaintainReport::default();
    for hour in hours {
        let key: BucketKey = (tenant_hash, signal, shard, hour);

        // Head and tail hours evaluate every tick, exactly as before this
        // zone split existed (ADR-0065 decision 3): only an interior hour
        // ever consults the memo. Steady state: the memo already proved an
        // interior bucket terminal and the entry is still fresh (within
        // `CompactorConfig::interior_reverify_ns`), so skip it without
        // listing or reading anything.
        let zone = classify_zone(hour, now, config, retention_window_ns);
        if zone != Zone::Interior {
            report.head_tail_hours.push(hour);
        } else if memo.is_fresh_terminal(&key, now) {
            report.skipped_terminal += 1;
            continue;
        }

        let bucket = Bucket::new(tenant_hash, signal, shard, hour);
        let (retention_outcome, compaction) =
            maintain_bucket_with_reach(&mut reach, store, clock, config, retention, lease, &bucket)
                .await?;
        match retention_outcome {
            // The bucket is (being) retired; compaction was skipped by design.
            RetentionOutcome::Tombstoned
            | RetentionOutcome::Swept
            | RetentionOutcome::SweptPartial => {
                report.retired += 1;
            }
            // The expired bucket's physical sweep was blocked by HEAD
            // reachability (ADR-0020): count by reason and treat as retired
            // work-in-progress (compaction was skipped, same as a tombstone).
            RetentionOutcome::BlockedBySnapshot(reason) => {
                report.retired += 1;
                match reason {
                    SnapshotBlock::Named => report.blocked_by_snapshot += 1,
                    SnapshotBlock::Unreadable => report.blocked_by_unreadable_head += 1,
                }
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod memo_snapshot_tests {
    use super::*;
    use ravel_types::TenantId;

    fn tenant() -> TenantHash {
        TenantId::new("acme").hash()
    }

    /// A cold snapshot round-trips to nothing: an empty memo encodes a
    /// well-formed object that seeds nothing and decodes clean.
    #[test]
    fn empty_memo_round_trips_to_empty() {
        let memo = MaintainMemo::with_default_interval();
        let bytes = memo.encode_snapshot(1_000);
        let mut seeded = MaintainMemo::with_default_interval();
        let stats = seeded
            .seed_from_snapshot(
                &bytes,
                1_000,
                DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
                |_, _, _| true,
            )
            .expect("decode empty");
        assert!(!stats.stale);
        assert_eq!(stats.seeded_buckets, 0);
        assert!(seeded.is_empty());
    }

    /// The frontier/exception split reconstructs the exact terminal set: a long
    /// same-state interior run (the frontier) plus divergent stragglers (the
    /// exceptions), across two units, seed back byte-for-byte identical.
    #[test]
    fn frontier_and_exceptions_round_trip_exactly() {
        let t = tenant();
        let mut memo = MaintainMemo::with_default_interval();
        // Unit A: a Compacted interior run 10..=20 (the frontier), an isolated
        // BelowThreshold at hour 5, and a SweptEmpty at hour 25 (exceptions).
        for hour in 10..=20 {
            memo.mark_terminal(
                (t, Signal::Metrics, 0, hour),
                TerminalState::Compacted,
                5_000,
            );
        }
        memo.mark_terminal(
            (t, Signal::Metrics, 0, 5),
            TerminalState::BelowThreshold,
            5_000,
        );
        memo.mark_terminal(
            (t, Signal::Metrics, 0, 25),
            TerminalState::SweptEmpty,
            5_000,
        );
        // Unit B (different shard, different signal): a small Compacted run.
        for hour in 100..=102 {
            memo.mark_terminal((t, Signal::Logs, 3, hour), TerminalState::Compacted, 6_000);
        }
        let want_len = memo.len();

        let bytes = memo.encode_snapshot(9_000);
        let mut seeded = MaintainMemo::with_default_interval();
        let stats = seeded
            .seed_from_snapshot(
                &bytes,
                9_000,
                DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
                |_, _, _| true,
            )
            .expect("decode");
        assert!(!stats.stale);
        assert_eq!(stats.seeded_units, 2);
        assert_eq!(seeded.len(), want_len);

        // Every original entry reconstructs with its exact state.
        for hour in 10..=20 {
            assert_eq!(
                seeded.terminal_state(t, Signal::Metrics, 0, hour),
                Some(TerminalState::Compacted)
            );
        }
        assert_eq!(
            seeded.terminal_state(t, Signal::Metrics, 0, 5),
            Some(TerminalState::BelowThreshold)
        );
        assert_eq!(
            seeded.terminal_state(t, Signal::Metrics, 0, 25),
            Some(TerminalState::SweptEmpty)
        );
        for hour in 100..=102 {
            assert_eq!(
                seeded.terminal_state(t, Signal::Logs, 3, hour),
                Some(TerminalState::Compacted)
            );
        }
    }

    /// A contiguous interior collapses to one frontier run and no exceptions,
    /// so the body stays small (the compression the ADR relies on): the
    /// exception-count field for the unit is zero regardless of run length.
    #[test]
    fn contiguous_interior_encodes_as_one_frontier_run() {
        let t = tenant();
        let mut memo = MaintainMemo::with_default_interval();
        for hour in 0..500 {
            memo.mark_terminal(
                (t, Signal::Metrics, 0, hour),
                TerminalState::Compacted,
                1_000,
            );
        }
        // Body is one unit header + one frontier record + a zero exception
        // count -- far smaller than 500 per-bucket entries would be.
        let body = memo.snapshot_body();
        // 4 (unit count) + [16 tenant +1 sig +4 shard +1 present +4 start +4 end
        // +1 state +8 verified +4 exc_count(=0)] = 4 + 43 = 47 bytes.
        assert_eq!(body.len(), 47, "500 hours collapse to one frontier run");
    }

    /// The whole-snapshot staleness gate excludes an old snapshot: nothing is
    /// seeded and `stale` is set, exactly one re-verify interval past the write.
    #[test]
    fn stale_snapshot_is_ignored() {
        let t = tenant();
        let mut memo = MaintainMemo::with_default_interval();
        memo.mark_terminal((t, Signal::Metrics, 0, 1), TerminalState::Compacted, 0);
        let bytes = memo.encode_snapshot(0);

        // Read one nanosecond past the staleness bound: excluded.
        let now = DEFAULT_MEMO_SNAPSHOT_STALENESS_NS + 1;
        let mut seeded = MaintainMemo::with_default_interval();
        let stats = seeded
            .seed_from_snapshot(
                &bytes,
                now,
                DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
                |_, _, _| true,
            )
            .expect("decode");
        assert!(stats.stale, "a snapshot past the bound is ignored");
        assert_eq!(stats.seeded_buckets, 0);
        assert!(seeded.is_empty());

        // Exactly at the bound: still trusted (inclusive), seeded.
        let mut seeded2 = MaintainMemo::with_default_interval();
        let stats2 = seeded2
            .seed_from_snapshot(
                &bytes,
                DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
                DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
                |_, _, _| true,
            )
            .expect("decode");
        assert!(!stats2.stale);
        assert_eq!(stats2.seeded_buckets, 1);
    }

    /// A far-future-dated snapshot is excluded symmetrically, like a
    /// far-future heartbeat: a skewed clock cannot seed stale facts as fresh.
    #[test]
    fn future_dated_snapshot_is_ignored() {
        let t = tenant();
        let mut memo = MaintainMemo::with_default_interval();
        memo.mark_terminal(
            (t, Signal::Metrics, 0, 1),
            TerminalState::Compacted,
            10 * NS_PER_HOUR,
        );
        let bytes = memo.encode_snapshot(10 * NS_PER_HOUR);
        let mut seeded = MaintainMemo::with_default_interval();
        let stats = seeded
            .seed_from_snapshot(&bytes, 0, DEFAULT_MEMO_SNAPSHOT_STALENESS_NS, |_, _, _| {
                true
            })
            .expect("decode");
        assert!(stats.stale, "a future-dated snapshot is excluded");
    }

    /// Only owned units are seeded: a snapshot spanning several units seeds
    /// exactly the ones the ownership predicate accepts.
    #[test]
    fn only_owned_units_are_seeded() {
        let t = tenant();
        let mut memo = MaintainMemo::with_default_interval();
        memo.mark_terminal((t, Signal::Metrics, 0, 1), TerminalState::Compacted, 0);
        memo.mark_terminal((t, Signal::Metrics, 1, 1), TerminalState::Compacted, 0);
        let bytes = memo.encode_snapshot(0);

        let mut seeded = MaintainMemo::with_default_interval();
        let stats = seeded
            .seed_from_snapshot(
                &bytes,
                0,
                DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
                |_, _, shard| shard == 0,
            )
            .expect("decode");
        assert_eq!(stats.seeded_units, 1);
        assert_eq!(
            seeded.terminal_state(t, Signal::Metrics, 0, 1),
            Some(TerminalState::Compacted)
        );
        assert_eq!(
            seeded.terminal_state(t, Signal::Metrics, 1, 1),
            None,
            "unowned unit not seeded"
        );
    }

    /// Merging two snapshots keeps the freshest verdict per bucket regardless of
    /// which is applied first, so sibling handoff converges deterministically.
    #[test]
    fn merging_snapshots_keeps_the_freshest_entry() {
        let t = tenant();
        let mut older = MaintainMemo::with_default_interval();
        older.mark_terminal(
            (t, Signal::Metrics, 0, 1),
            TerminalState::BelowThreshold,
            1_000,
        );
        let older_bytes = older.encode_snapshot(1_000);

        let mut newer = MaintainMemo::with_default_interval();
        newer.mark_terminal((t, Signal::Metrics, 0, 1), TerminalState::Compacted, 5_000);
        let newer_bytes = newer.encode_snapshot(5_000);

        // Apply older then newer: newer wins.
        let mut a = MaintainMemo::new(0);
        a.seed_from_snapshot(&older_bytes, 5_000, 0, |_, _, _| true)
            .expect("older");
        a.seed_from_snapshot(&newer_bytes, 5_000, 0, |_, _, _| true)
            .expect("newer");
        assert_eq!(
            a.terminal_state(t, Signal::Metrics, 0, 1),
            Some(TerminalState::Compacted)
        );

        // Apply newer then older: newer still wins (order independent).
        let mut b = MaintainMemo::new(0);
        b.seed_from_snapshot(&newer_bytes, 5_000, 0, |_, _, _| true)
            .expect("newer");
        b.seed_from_snapshot(&older_bytes, 5_000, 0, |_, _, _| true)
            .expect("older");
        assert_eq!(
            b.terminal_state(t, Signal::Metrics, 0, 1),
            Some(TerminalState::Compacted)
        );
    }

    /// A truncated or wrong-tag payload is a typed error, never a panic, so the
    /// driver can log it and cold-start (fail-open).
    #[test]
    fn corrupt_payloads_are_typed_errors_not_panics() {
        let mut memo = MaintainMemo::with_default_interval();
        assert!(matches!(
            memo.seed_from_snapshot(&[], 0, 0, |_, _, _| true),
            Err(MemoSnapshotError::Truncated { .. })
        ));
        assert!(matches!(
            memo.seed_from_snapshot(&[9, 0, 0, 0, 0, 0, 0, 0, 0], 0, 0, |_, _, _| true),
            Err(MemoSnapshotError::UnknownTag(9))
        ));
        // Valid tag + timestamp but a unit count promising a unit that is not
        // there: truncated, not a panic.
        let t = tenant();
        let mut good = MaintainMemo::with_default_interval();
        good.mark_terminal((t, Signal::Metrics, 0, 1), TerminalState::Compacted, 0);
        let mut bytes = good.encode_snapshot(0);
        bytes.truncate(bytes.len() - 3);
        assert!(matches!(
            memo.seed_from_snapshot(&bytes, 0, 0, |_, _, _| true),
            Err(MemoSnapshotError::Truncated { .. })
        ));
    }

    /// The snapshot body is a stable function of the memo (debounce contract):
    /// two memos with the same terminal set and verify times produce identical
    /// bodies, and the body ignores the timestamp header.
    #[test]
    fn snapshot_body_is_stable_for_debounce() {
        let t = tenant();
        let mut a = MaintainMemo::with_default_interval();
        let mut b = MaintainMemo::with_default_interval();
        for hour in [3u32, 1, 2, 7, 5] {
            a.mark_terminal(
                (t, Signal::Metrics, 0, hour),
                TerminalState::Compacted,
                1_000,
            );
            b.mark_terminal(
                (t, Signal::Metrics, 0, hour),
                TerminalState::Compacted,
                1_000,
            );
        }
        assert_eq!(
            a.snapshot_body(),
            b.snapshot_body(),
            "same content, same body"
        );
        // The framed object differs only in the timestamp header, so the body
        // used for debounce is unchanged across two writes at different times.
        let body_from_full_1 = &a.encode_snapshot(1)[9..];
        let body_from_full_2 = &a.encode_snapshot(999)[9..];
        assert_eq!(body_from_full_1, body_from_full_2);
    }

    /// A decoded entry whose `verified_ns` is past the
    /// snapshot's own `snapshot_unix_ns` -- a corrupted or clock-skewed run --
    /// is clamped to `snapshot_unix_ns` at the seed site, never trusted at its
    /// raw future value. Without the clamp such an entry reads as eternally
    /// fresh and, because the memo is re-encoded and republished each cycle,
    /// self-propagates fleet-wide, permanently suppressing retention/compaction
    /// for its bucket. The header stays fresh so only the per-entry value is out
    /// of range, isolating the guard this test proves.
    #[test]
    fn future_dated_entry_is_clamped_to_snapshot_time() {
        let t = tenant();
        let snapshot_unix_ns = 5 * NS_PER_HOUR;
        // Roughly a century past the snapshot's own write time.
        let future_ns = snapshot_unix_ns + 100 * 365 * 24 * NS_PER_HOUR;

        let mut poisoned = MaintainMemo::with_default_interval();
        poisoned.mark_terminal(
            (t, Signal::Metrics, 0, 1),
            TerminalState::Compacted,
            future_ns,
        );
        // Re-frame the poisoned body under an honest, fresh header, so the
        // whole-snapshot staleness gate passes and only the entry is skewed.
        let bytes =
            MaintainMemo::snapshot_bytes_from_body(&poisoned.snapshot_body(), snapshot_unix_ns);

        let mut seeded = MaintainMemo::with_default_interval();
        let stats = seeded
            .seed_from_snapshot(
                &bytes,
                snapshot_unix_ns,
                DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
                |_, _, _| true,
            )
            .expect("decode");
        assert_eq!(stats.seeded_buckets, 1);

        let key = (t, Signal::Metrics, 0, 1);
        let entry = seeded.entries.get(&key).expect("seeded entry");
        assert_eq!(
            entry.verified_at_ns, snapshot_unix_ns,
            "a future-dated entry is clamped to the snapshot's own time"
        );
        // The clamp restores normal expiry: fresh at snapshot time, but no
        // longer fresh one re-verify interval later (not eternally fresh).
        assert!(seeded.is_fresh_terminal(&key, snapshot_unix_ns));
        assert!(
            !seeded.is_fresh_terminal(
                &key,
                snapshot_unix_ns + DEFAULT_MEMO_REVERIFY_INTERVAL_NS + 1
            ),
            "clamped entry expires on schedule instead of suppressing reads forever"
        );
    }

    fn state_of(code: u8) -> TerminalState {
        match code % 3 {
            0 => TerminalState::Compacted,
            1 => TerminalState::BelowThreshold,
            _ => TerminalState::SweptEmpty,
        }
    }

    proptest::proptest! {
        /// RLE memo codec round-trip. For an arbitrary
        /// per-hour terminal/non-terminal state sequence (independent choices at
        /// each hour, so consecutive hours may alternate state), `decode(encode)`
        /// reproduces the *effective* per-hour state and verified_ns. Effective
        /// accounts for the codec's run collapse: `terminal_runs` folds a maximal
        /// contiguous same-state span to that span's minimum verified_ns
        /// (conservative for freshness), and every hour in the span decodes to
        /// that minimum. The snapshot's write time is set at or above every
        /// entry, so fix 1's clamp is a no-op here and the round-trip is exact
        /// against that effective expectation.
        #[test]
        fn rle_snapshot_round_trips_per_hour(
            hours in proptest::collection::vec(
                proptest::option::of((0u8..3u8, 1i64..1_000_000i64)),
                0usize..48usize,
            )
        ) {
            let t = tenant();
            let mut memo = MaintainMemo::with_default_interval();
            let mut present: Vec<(u32, TerminalState, i64)> = Vec::new();
            for (hour, cell) in hours.iter().enumerate() {
                if let Some((code, verified_ns)) = cell {
                    let hour = hour as u32;
                    let state = state_of(*code);
                    memo.mark_terminal((t, Signal::Metrics, 0, hour), state, *verified_ns);
                    present.push((hour, state, *verified_ns));
                }
            }

            // Independent oracle for the effective per-hour values: collapse each
            // maximal contiguous same-state span to its minimum verified_ns.
            present.sort_by_key(|e| e.0);
            let mut expected: Vec<(u32, TerminalState, i64)> = Vec::new();
            let mut i = 0;
            while i < present.len() {
                let (_, state, _) = present[i];
                let mut j = i;
                let mut run_min = present[i].2;
                while j + 1 < present.len()
                    && present[j].0 + 1 == present[j + 1].0
                    && present[j + 1].1 == state
                {
                    j += 1;
                    run_min = run_min.min(present[j].2);
                }
                for entry in present.iter().take(j + 1).skip(i) {
                    expected.push((entry.0, state, run_min));
                }
                i = j + 1;
            }

            // Honest write time: at or after every entry, so the clamp is a no-op.
            let snapshot_unix_ns = present.iter().map(|e| e.2).max().unwrap_or(0);
            let bytes = memo.encode_snapshot(snapshot_unix_ns);

            let mut seeded = MaintainMemo::with_default_interval();
            seeded
                .seed_from_snapshot(&bytes, snapshot_unix_ns, 0, |_, _, _| true)
                .expect("decode");

            proptest::prop_assert_eq!(seeded.len(), expected.len());
            for (hour, state, verified_ns) in expected {
                let key = (t, Signal::Metrics, 0, hour);
                proptest::prop_assert_eq!(
                    seeded.terminal_state(t, Signal::Metrics, 0, hour),
                    Some(state)
                );
                let entry = seeded.entries.get(&key).expect("entry");
                proptest::prop_assert_eq!(entry.verified_at_ns, verified_ns);
            }
        }
    }
}

/// Proves `erasure_rewrite_bucket` actually invalidates
/// a memoized terminal state, for all three wired signals (metrics, logs,
/// spans), including metrics. Lives here
/// rather than in `erasure_rewrite.rs`'s own test module because
/// `MaintainMemo::mark_terminal` is private to this module and only visible
/// to its descendants (the same access pattern `memo_snapshot_tests` above
/// already relies on).
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod invalidate_tests {
    use bytes::Bytes;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_logseg::writer::ObjectIdentity as LogObjectIdentity;
    use ravel_logseg::{LogRecord, RlogConfig, RlogWriter};
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
    use ravel_rspan::writer::ObjectIdentity as SpanObjectIdentity;
    use ravel_rspan::{RspanConfig, RspanWriter, SpanRecord, StatusCode};
    use ravel_segment::{
        IngestBounds, SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues, VERSION_V7,
    };
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::build::OUTPUT_FORMAT_VERSION;
    use crate::clock::FixedClock;
    use crate::erasure_rewrite::{
        ErasureRewriteOutcome, PendingErasureRequest, erasure_rewrite_bucket,
    };
    use crate::publish::PublishOutcome;
    use crate::sweep::NoLeases;

    const TENANT: &str = "acme";
    const SHARD: u32 = 3;
    const HOUR: u32 = 500_000;
    const EPOCH: u64 = 1;

    fn tenant_hash() -> TenantHash {
        TenantId::new(TENANT).hash()
    }

    fn sealed_now_ns() -> i64 {
        (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
    }

    fn erasure_request(signal: Signal, key: &str, value: &str) -> ErasureRequest {
        ErasureRequest {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: ravel_commit::signal::to_proto(signal) as i32,
            request_id: Uuid::from_u128(1).to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: key.to_string(),
                value: value.to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        }
    }

    async fn seed_metrics(store: &dyn ObjectStoreBackend) {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(1);
        let created = i64::from(HOUR) * NS_PER_HOUR;
        let identity = SegmentIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.to_string(),
            writer_epoch: EPOCH,
            writer_seq: 1,
        };
        let labels = LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "checkout_latency".to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&TenantId::new(TENANT), "checkout_latency", &labels)
            .expect("series id");
        let series = SeriesInputV3 {
            series_id,
            labels,
            values: SeriesValues::Scalar(vec![Sample {
                ts_ns: created + 1,
                value: 1.0,
            }]),
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
        };
        let written = SegmentWriter::write_histograms_with_exemplars(
            vec![series],
            identity,
            bounds,
            Vec::new(),
        )
        .expect("write L0");
        let content_hash = written.summary.blake3;
        let data_key = keys::data_key(
            &th,
            Signal::Metrics,
            SHARD,
            writer_id,
            EPOCH,
            1,
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
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: u32::from(VERSION_V7),
            created_unix_ns: created,
            ingest_hour_bucket: HOUR,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
    }

    async fn seed_logs(store: &dyn ObjectStoreBackend) {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(1);
        let created = i64::from(HOUR) * NS_PER_HOUR;
        let identity = LogObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: 1,
        };
        let res = vec![(
            "service.name".to_string(),
            ravel_logseg::AttrValue::Str("checkout".to_string()),
        )];
        let stream_id = ravel_types::logstream::log_stream_id(&res, "scope", "1", &[]);
        let stream_attrs = ravel_logseg::stream_attrs_bytes(&res, "scope", "1", &[]);
        let record = LogRecord {
            stream_id,
            stream_attrs,
            ts_ns: created + 1,
            observed_ts_ns: created + 1,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "checkout failed".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![(
                "service".to_string(),
                ravel_logseg::AttrValue::Str("checkout".to_string()),
            )],
        };
        let mut w = RlogWriter::new(RlogConfig::default(), identity);
        w.push(record).expect("push log record");
        let bytes = Bytes::from(w.finish().expect("finish L0"));
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data_key = keys::data_key(&th, Signal::Logs, SHARD, writer_id, EPOCH, 1, &content_hash)
            .expect("data key");
        store
            .put(&data_key, bytes.clone(), PutOptions::default())
            .await
            .expect("put data object");
        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Logs,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: created + 1,
            max_event_ts_ns: created + 1,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: OUTPUT_FORMAT_VERSION,
            created_unix_ns: created,
            ingest_hour_bucket: HOUR,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
    }

    async fn seed_spans(store: &dyn ObjectStoreBackend) {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(1);
        let created = i64::from(HOUR) * NS_PER_HOUR;
        let identity = SpanObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: 1,
        };
        let record = SpanRecord {
            trace_id: [1; 16],
            span_id: [1; 8],
            parent_span_id: None,
            name: "op".into(),
            start_ts_ns: created + 1,
            end_ts_ns: created + 2,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: vec![("service".to_string(), "checkout".to_string())],
        };
        let mut w = RspanWriter::new(RspanConfig::default(), identity);
        w.push(record);
        let bytes = Bytes::from(w.finish().expect("finish L0"));
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data_key = keys::data_key(
            &th,
            Signal::Spans,
            SHARD,
            writer_id,
            EPOCH,
            1,
            &content_hash,
        )
        .expect("data key");
        store
            .put(&data_key, bytes.clone(), PutOptions::default())
            .await
            .expect("put data object");
        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Spans,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: created + 1,
            max_event_ts_ns: created + 2,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: OUTPUT_FORMAT_VERSION,
            created_unix_ns: created,
            ingest_hour_bucket: HOUR,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
    }

    /// A successful METRICS rewrite forgets the bucket's memoized terminal
    /// state, so the next scan tick re-evaluates it instead of trusting a
    /// stale "already compacted" verdict.
    /// Flip-line proof: in `invalidate_after_publish`
    /// (`erasure_rewrite.rs`), changing the call's `bucket.signal` argument
    /// to a hardcoded `Signal::Logs` (or deleting the call from the
    /// `Signal::Metrics` dispatch arm) leaves this bucket's memo entry
    /// intact, failing the `assert_eq!(memo.terminal_state(...), None)`
    /// below.
    #[tokio::test]
    async fn metrics_rewrite_invalidates_memoized_terminal_state() {
        let store = MemoryStore::new();
        seed_metrics(&store).await;
        let t = tenant_hash();
        let mut memo = MaintainMemo::with_default_interval();
        memo.mark_terminal(
            (t, Signal::Metrics, SHARD, HOUR),
            TerminalState::Compacted,
            0,
        );
        assert_eq!(
            memo.terminal_state(t, Signal::Metrics, SHARD, HOUR),
            Some(TerminalState::Compacted)
        );

        let request = erasure_request(Signal::Metrics, "__name__", "checkout_latency");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];
        let bucket = Bucket::new(t, Signal::Metrics, SHARD, HOUR);
        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let outcome = erasure_rewrite_bucket(
            &store, &clock, &config, &NoLeases, &bucket, &pending, &mut memo,
        )
        .await
        .expect("rewrite");
        assert!(matches!(
            outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ));

        assert_eq!(
            memo.terminal_state(t, Signal::Metrics, SHARD, HOUR),
            None,
            "a published metrics rewrite must invalidate the memoized terminal state"
        );
    }

    /// The LOGS sibling of the metrics invalidate test above: a published
    /// LOGS rewrite forgets its bucket's memoized terminal state too, proving
    /// the wiring is not scoped to the metrics dispatch arm alone. Flip-line
    /// proof: same as above -- restricting `invalidate_after_publish`'s call
    /// to fire only for `Signal::Metrics` leaves this bucket's entry intact.
    #[tokio::test]
    async fn logs_rewrite_invalidates_memoized_terminal_state() {
        let store = MemoryStore::new();
        seed_logs(&store).await;
        let t = tenant_hash();
        let mut memo = MaintainMemo::with_default_interval();
        memo.mark_terminal((t, Signal::Logs, SHARD, HOUR), TerminalState::Compacted, 0);

        let request = erasure_request(Signal::Logs, "service", "checkout");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];
        let bucket = Bucket::new(t, Signal::Logs, SHARD, HOUR);
        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let outcome = erasure_rewrite_bucket(
            &store, &clock, &config, &NoLeases, &bucket, &pending, &mut memo,
        )
        .await
        .expect("rewrite");
        assert!(matches!(
            outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ));

        assert_eq!(
            memo.terminal_state(t, Signal::Logs, SHARD, HOUR),
            None,
            "a published logs rewrite must invalidate the memoized terminal state"
        );
    }

    /// The SPANS sibling of the two invalidate tests above: a published
    /// SPANS rewrite forgets its bucket's memoized terminal state too.
    /// Flip-line proof: same as above -- restricting
    /// `invalidate_after_publish`'s call to fire only for
    /// `Signal::Metrics`/`Signal::Logs` leaves this bucket's entry intact.
    #[tokio::test]
    async fn spans_rewrite_invalidates_memoized_terminal_state() {
        let store = MemoryStore::new();
        seed_spans(&store).await;
        let t = tenant_hash();
        let mut memo = MaintainMemo::with_default_interval();
        memo.mark_terminal((t, Signal::Spans, SHARD, HOUR), TerminalState::Compacted, 0);

        let request = erasure_request(Signal::Spans, "service", "checkout");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];
        let bucket = Bucket::new(t, Signal::Spans, SHARD, HOUR);
        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let outcome = erasure_rewrite_bucket(
            &store, &clock, &config, &NoLeases, &bucket, &pending, &mut memo,
        )
        .await
        .expect("rewrite");
        assert!(matches!(
            outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ));

        assert_eq!(
            memo.terminal_state(t, Signal::Spans, SHARD, HOUR),
            None,
            "a published spans rewrite must invalidate the memoized terminal state"
        );
    }
}
