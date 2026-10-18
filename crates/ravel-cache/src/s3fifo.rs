//! S3-FIFO eviction (ADR-0046 decision 6, amended 2026-08-02): a small FIFO
//! probation queue, a main FIFO for entries that proved themselves, and a
//! ghost queue of keys evicted from probation before they got a second
//! chance.
//!
//! Shared by both tiers. The amended decision 6 scopes scan resistance to
//! disk *more* than RAM, not less: a disk miss costs an S3 fetch, the most
//! expensive thing a query does, and the disk tier is the large one that
//! actually holds the working set. [`crate::disk::DiskCache`] and
//! [`crate::Cache`] both use this same policy rather than each carrying its
//! own; the disk tier instantiates it with `V = ()` since its payload lives
//! on disk, not in this structure, and passes the entry size in explicitly
//! at [`S3Fifo::insert`] rather than deriving it from the stored value.
//!
//! Plain LRU is wrong for this cache: the compactor and the folder scan
//! cold, content-addressed data in the same process as queries in every
//! mode except a dedicated maintain deployment. A single compaction pass
//! is a long run of distinct keys touched exactly once each, and under
//! LRU that walk is itself the most-recently-used list, so it evicts the
//! query working set on its way through. S3-FIFO's small probation queue
//! absorbs exactly that pattern: a key admitted and never touched again
//! is evicted straight out of probation without ever reaching main, so
//! the scan cannot touch anything already promoted into main.
//!
//! Promotion rule: a freshly admitted entry always starts in the small
//! queue with `freq = 0`. Every [`S3Fifo::get`] hit bumps `freq` (capped).
//! When an entry reaches the front of the small queue for eviction, a
//! nonzero `freq` (a second access happened after admission) promotes it
//! to the back of the main queue instead of discarding it; a zero `freq`
//! evicts it and remembers its key in the ghost queue. The main queue is
//! a CLOCK: an entry at its front with nonzero `freq` gets `freq`
//! decremented and one more lap instead of being evicted immediately.
//!
//! A key already in the ghost queue (evicted from probation, then
//! requested again) is inserted directly into the main queue, since a
//! second request within the ghost window is exactly the "worth keeping"
//! signal that would otherwise take a full second access in probation to
//! detect.
//!
//! # Access intent (ADR-3072 decision 3b)
//!
//! Everything above is the [`AccessIntent::Interactive`] policy, which
//! [`S3Fifo::get`] and [`S3Fifo::insert`] apply. [`S3Fifo::get_with_intent`]
//! and [`S3Fifo::insert_with_intent`] take the intent:
//!
//! - [`AccessIntent::Maintenance`]: a hit leaves `freq` alone and an
//!   admission goes to the small queue even when the key is in the ghost
//!   queue, so a maintenance read never moves an entry into main. Every
//!   admission that stores a key, whatever its intent, drops it from the
//!   ghost queue.
//! - [`AccessIntent::BulkScan`]: a hit leaves `freq` alone and an admission
//!   goes to a bulk region outside the small and main queues, never to main,
//!   ghost or not. A loop larger than the cache cannot be served by recency
//!   (ADR-2677's decision 3 deferral), so the region keeps a deterministic
//!   subset of the keys instead: each key has a fixed rank, a hash of every
//!   key field, and a full region admits a key only by evicting a resident
//!   of higher rank. The resident set converges to the lowest-ranked keys of
//!   the loop and, once there, the same keys are served on every later pass.
//!   Once the bulk tenants' pass bytes exceed the region's capacity, a key
//!   whose rank is at or above that capacity's share of them plus a quarter
//!   of slack is declined without looking at the region.
//!
//! The region holds up to `bulk_share` of `max_bytes` against interactive
//! admissions, and more while the small and main queues leave bytes unused.
//! A bulk admission frees room by evicting higher-ranked region entries and,
//! only while the region is below its share, small-queue entries; it never
//! evicts from main. An interactive admission reclaims region bytes above
//! the share before it evicts anything interactive. An interactive hit on a
//! region entry moves it to the small queue with the hit counted.
//!
//! A tenant's pass bytes are estimated per window from a bottom-k sketch of
//! the distinct ranks its bulk reads touched. A window closes once its
//! traffic reaches twice the tenant's pass bytes (at least twice and at most
//! sixteen times `max_bytes`), and the tenant's region entries last read or
//! admitted before the window that just closed are evicted, so a loop the
//! tenant stopped reading gives its bytes back. A tenant with no bulk read
//! for sixteen times `max_bytes` of bulk traffic is retired with its entries.
//! The sketches and the region's rank map are charged inside `max_bytes` and
//! capped at one percent of it ([`S3Fifo::bookkeeping_bytes`]).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use crate::AccessIntent;
use crate::key::CacheKey;
use crate::limits::CacheLimits;
use crate::metrics::CacheMetrics;

const MAX_FREQ: u8 = 3;

/// Bookkeeping charged per resident bulk-region entry: an estimate of its
/// slot in the rank-ordered region map, a `u64` rank and a 64-byte
/// [`CacheKey`] in a B-tree node at minimum fill. The entry's bulk flag and
/// window stamp sit in [`Entry`]'s padding and add nothing to its size.
pub(crate) const REGION_ENTRY_BOOKKEEPING_BYTES: u64 = 192;

/// Bookkeeping charged per tracked bulk tenant: its sketch of
/// [`SKETCH_LEN`] `(rank, size)` pairs, its counters, and its map slot.
pub(crate) const TENANT_BOOKKEEPING_BYTES: u64 = 2048;

/// Bulk tenants tracked at once; a new one retires the least recently read.
const MAX_BULK_TENANTS: usize = 64;

/// Distinct ranks kept per tenant sketch.
const SKETCH_LEN: usize = 64;

/// Bookkeeping is held to `max_bytes / BOOKKEEPING_BUDGET_DIVISOR`.
const BOOKKEEPING_BUDGET_DIVISOR: u64 = 100;

/// A tenant's pass window closes after at least this many times `max_bytes`
/// of its traffic, and after at most [`WINDOW_CEILING_MULTIPLE`] times.
const WINDOW_FLOOR_MULTIPLE: u64 = 2;
const WINDOW_CEILING_MULTIPLE: u64 = 16;

struct Entry<V> {
    value: V,
    size: u64,
    freq: u8,
    /// Held in the bulk region rather than the small or main queue.
    bulk: bool,
    /// For a bulk entry, its tenant's pass window at the entry's last bulk
    /// read or its admission.
    window: u32,
}

impl<V> Entry<V> {
    fn interactive(value: V, size: u64) -> Self {
        Entry {
            value,
            size,
            freq: 0,
            bulk: false,
            window: 0,
        }
    }
}

/// Outcome of processing one candidate at the front of a queue during
/// eviction: nothing left to look at, something happened but nothing left
/// this structure's accounting (a promotion, or a stale queue entry whose
/// value was already gone), or a key was actually evicted and its resident
/// bytes freed.
enum Step {
    Empty,
    Skipped,
    Evicted(CacheKey),
}

/// The smallest [`SKETCH_LEN`] distinct ranks one tenant's bulk reads
/// touched in the current window, each with its entry size: a bottom-k
/// sketch that estimates the window's distinct bytes without remembering
/// every key.
struct DistinctBytes {
    smallest: Vec<(u64, u64)>,
}

impl DistinctBytes {
    fn new() -> Self {
        DistinctBytes {
            smallest: Vec::with_capacity(SKETCH_LEN + 1),
        }
    }

    fn observe(&mut self, rank: u64, size: u64) {
        if self.smallest.len() == SKETCH_LEN
            && self.smallest.last().is_some_and(|&(last, _)| rank >= last)
        {
            return;
        }
        if let Err(pos) = self.smallest.binary_search_by_key(&rank, |&(r, _)| r) {
            self.smallest.insert(pos, (rank, size));
            self.smallest.truncate(SKETCH_LEN);
        }
    }

    /// Exact below [`SKETCH_LEN`] distinct keys; above it, the k-minimum
    /// values estimate of the distinct key count, `(k - 1) / kth_rank` over
    /// the unit rank space, times the sample's mean entry size.
    fn estimate(&self) -> u64 {
        let sum: u128 = self
            .smallest
            .iter()
            .map(|&(_, size)| u128::from(size))
            .sum();
        let kth = match self.smallest.get(SKETCH_LEN - 1) {
            Some(&(rank, _)) => rank,
            None => return u64::try_from(sum).unwrap_or(u64::MAX),
        };
        let k = SKETCH_LEN as u128;
        let distinct = ((k - 1) << 64) / (u128::from(kth) + 1);
        u64::try_from(distinct.saturating_mul(sum) / k).unwrap_or(u64::MAX)
    }

    fn clear(&mut self) {
        self.smallest.clear();
    }
}

/// One bulk tenant's pass accounting.
struct TenantPass {
    sketch: DistinctBytes,
    /// The distinct-bytes estimate of the window before this one.
    previous_bytes: u64,
    window: u32,
    window_traffic: u64,
    /// The region's bulk read clock at this tenant's last bulk read.
    last_read: u64,
}

impl TenantPass {
    fn new(clock: u64) -> Self {
        TenantPass {
            sketch: DistinctBytes::new(),
            previous_bytes: 0,
            window: 0,
            window_traffic: 0,
            last_read: clock,
        }
    }

    fn pass_bytes(&self) -> u64 {
        self.previous_bytes.max(self.sketch.estimate())
    }
}

#[derive(Default)]
struct BulkRegion {
    by_rank: BTreeMap<u64, CacheKey>,
    bytes: u64,
    tenants: HashMap<[u8; 16], TenantPass>,
    /// Bytes of every bulk read so far, the time base for idle tenants.
    clock: u64,
}

/// A key's fixed position in the bulk region's admission order: every key
/// field mixed through splitmix64, so ranks spread evenly over `u64` and a
/// key has the same rank on every pass and in every process.
fn rank_of(key: &CacheKey) -> u64 {
    let mut rank = 0u64;
    let mut mix = |word: u64| rank = splitmix64(rank ^ word);
    for word in key
        .tenant_hash
        .as_chunks::<8>()
        .0
        .iter()
        .chain(key.content_hash.as_chunks::<8>().0)
    {
        mix(u64::from_le_bytes(*word));
    }
    mix(key.offset);
    mix(key.len);
    rank
}

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

pub(crate) struct S3Fifo<V> {
    entries: HashMap<CacheKey, Entry<V>>,
    small: VecDeque<CacheKey>,
    main: VecDeque<CacheKey>,
    ghost: VecDeque<CacheKey>,
    ghost_set: HashSet<CacheKey>,
    small_bytes: u64,
    main_bytes: u64,
    small_quota_bytes: u64,
    ghost_capacity: usize,
    region: BulkRegion,
    region_share_bytes: u64,
    bookkeeping_budget: u64,
    limits: CacheLimits,
}

impl<V: Clone> S3Fifo<V> {
    pub(crate) fn new(limits: CacheLimits) -> Self {
        // 10% probation / 90% main, the split the S3-FIFO paper uses for
        // skewed access patterns; at least 1 byte so a tiny configured
        // cache still has a probation queue to evict from.
        let small_quota_bytes = (limits.max_bytes / 10).max(1);
        // The ghost window must approximate the RESIDENT capacity, not
        // `max_entries`. Whichever of the two bounds binds first decides how
        // many entries actually fit, and the byte bound is normally the
        // binding one, so sizing the ghost from `max_entries` alone can make
        // it many times larger than the cache.
        //
        // That is not a tuning detail. A ghost window wider than the cache
        // means every key from a completed scan is still remembered when the
        // next scan starts, so each one is admitted straight to `main`,
        // bypassing probation, and the second pass evicts the working set the
        // first pass survived. Scan resistance would then hold for exactly one
        // pass, while ADR-0046 decision 6 exists because the compactor and the
        // folder scan cold data continuously.
        //
        // `max_entry_bytes` is the smallest entry size we can be sure of, so
        // `max_bytes / max_entry_bytes` is a lower bound on resident capacity;
        // take the tighter of that and `max_entries`.
        let capacity_from_bytes = limits
            .max_bytes
            .checked_div(limits.max_entry_bytes.max(1))
            .unwrap_or(0) as usize;
        let ghost_capacity = capacity_from_bytes.min(limits.max_entries).max(1);
        let region_share_bytes = (limits.max_bytes as f64 * limits.effective_bulk_share()) as u64;
        S3Fifo {
            entries: HashMap::new(),
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_set: HashSet::new(),
            small_bytes: 0,
            main_bytes: 0,
            small_quota_bytes,
            ghost_capacity,
            region: BulkRegion::default(),
            region_share_bytes,
            bookkeeping_budget: limits.max_bytes / BOOKKEEPING_BUDGET_DIVISOR,
            limits,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Resident payload bytes across the small and main queues and the bulk
    /// region. [`Self::bookkeeping_bytes`] is charged against `max_bytes`
    /// on top of this.
    pub(crate) fn total_bytes(&self) -> u64 {
        self.small_bytes + self.main_bytes + self.region.bytes
    }

    /// Bytes charged inside `max_bytes` for the bulk region's own
    /// bookkeeping: [`REGION_ENTRY_BOOKKEEPING_BYTES`] per region entry and
    /// [`TENANT_BOOKKEEPING_BYTES`] per tracked bulk tenant. Zero until a
    /// [`AccessIntent::BulkScan`] admission, and never above one percent of
    /// `max_bytes`.
    pub(crate) fn bookkeeping_bytes(&self) -> u64 {
        self.region.by_rank.len() as u64 * REGION_ENTRY_BOOKKEEPING_BYTES
            + self.region.tenants.len() as u64 * TENANT_BOOKKEEPING_BYTES
    }

    fn charged_bytes(&self) -> u64 {
        self.total_bytes() + self.bookkeeping_bytes()
    }

    pub(crate) fn contains(&self, key: &CacheKey) -> bool {
        self.entries.contains_key(key)
    }

    /// [`Self::get_with_intent`] for an [`AccessIntent::Interactive`] read.
    pub(crate) fn get(&mut self, key: &CacheKey) -> Option<V> {
        self.hit_interactive(key)
    }

    /// Look up `key` for a read of the given intent. Returns the value and
    /// the keys, if any, a [`AccessIntent::BulkScan`] read's window rotation
    /// evicted (always empty for the other intents); the caller owns what
    /// those keys referenced, as with [`Self::insert`].
    pub(crate) fn get_with_intent(
        &mut self,
        key: &CacheKey,
        intent: AccessIntent,
        metrics: &CacheMetrics,
    ) -> (Option<V>, Vec<CacheKey>) {
        match intent {
            AccessIntent::Interactive => (self.hit_interactive(key), Vec::new()),
            AccessIntent::BulkScan => self.hit_bulk(key, metrics),
            AccessIntent::Maintenance => (
                self.entries.get(key).map(|entry| entry.value.clone()),
                Vec::new(),
            ),
        }
    }

    fn hit_interactive(&mut self, key: &CacheKey) -> Option<V> {
        let entry = self.entries.get_mut(key)?;
        entry.freq = entry.freq.saturating_add(1).min(MAX_FREQ);
        let value = entry.value.clone();
        if entry.bulk {
            // An interactive read of a region entry: it joins the small
            // queue with this hit counted, as if admitted interactively and
            // read once since.
            entry.bulk = false;
            let size = entry.size;
            self.region.by_rank.remove(&rank_of(key));
            self.region.bytes -= size;
            self.small.push_back(*key);
            self.small_bytes += size;
        }
        Some(value)
    }

    fn hit_bulk(&mut self, key: &CacheKey, metrics: &CacheMetrics) -> (Option<V>, Vec<CacheKey>) {
        let Some(entry) = self.entries.get(key) else {
            return (None, Vec::new());
        };
        let value = entry.value.clone();
        let size = entry.size;
        let mut evicted = Vec::new();
        if self.region.tenants.contains_key(&key.tenant_hash) {
            self.observe_bulk(key, rank_of(key), size, metrics, &mut evicted);
        }
        (Some(value), evicted)
    }

    /// [`Self::insert_with_intent`] for an [`AccessIntent::Interactive`]
    /// admission.
    pub(crate) fn insert(
        &mut self,
        key: CacheKey,
        value: V,
        size: u64,
        metrics: &CacheMetrics,
    ) -> (bool, Vec<CacheKey>) {
        self.insert_with_intent(key, value, size, AccessIntent::Interactive, metrics)
    }

    /// Admit `value` (of `size` bytes) under `key` for a read of the given
    /// intent. Returns whether it was admitted and the keys, if any, evicted
    /// to make or keep room -- the caller owns whatever those keys' values
    /// referenced (an in-memory `Bytes` just drops; the disk tier deletes
    /// the corresponding file). Not admitting is not an error, just a decline
    /// to cache: `size` exceeds `max_entry_bytes`, or a
    /// [`AccessIntent::BulkScan`] key falls outside the region's subset.
    pub(crate) fn insert_with_intent(
        &mut self,
        key: CacheKey,
        value: V,
        size: u64,
        intent: AccessIntent,
        metrics: &CacheMetrics,
    ) -> (bool, Vec<CacheKey>) {
        self.insert_with_intent_lazy(key, size, intent, metrics, || value)
    }

    /// [`Self::insert_with_intent`] with the value built by `make`, which
    /// runs only once the entry is stored: never for a decline, and never
    /// for a key already resident. The RAM tier makes its owned copy of the
    /// payload here so a declined admission copies nothing.
    pub(crate) fn insert_with_intent_lazy(
        &mut self,
        key: CacheKey,
        size: u64,
        intent: AccessIntent,
        metrics: &CacheMetrics,
        make: impl FnOnce() -> V,
    ) -> (bool, Vec<CacheKey>) {
        if size > self.limits.max_entry_bytes {
            metrics.record_rejected_size();
            return (false, Vec::new());
        }
        if self.entries.contains_key(&key) {
            // Content-addressed: an existing entry for this key is
            // already these exact bytes. Leave its queue position alone.
            return (true, Vec::new());
        }
        if intent == AccessIntent::BulkScan {
            return self.insert_bulk(key, size, metrics, make);
        }

        let promote_to_main = self.forget_ghost(&key) && intent == AccessIntent::Interactive;

        self.entries.insert(key, Entry::interactive(make(), size));
        if promote_to_main {
            self.main.push_back(key);
            self.main_bytes += size;
        } else {
            self.small.push_back(key);
            self.small_bytes += size;
        }
        metrics.record_admission(size);

        let evicted = self.evict_to_bounds(metrics);
        (true, evicted)
    }

    /// Drops `key` from the ghost queue, returning whether it was there. Run
    /// by every admission that stores the key, so a resident key is never
    /// also a ghost; only an interactive admission promotes on the result.
    fn forget_ghost(&mut self, key: &CacheKey) -> bool {
        if !self.ghost_set.remove(key) {
            return false;
        }
        if let Some(pos) = self.ghost.iter().position(|k| k == key) {
            self.ghost.remove(pos);
        }
        true
    }

    fn insert_bulk(
        &mut self,
        key: CacheKey,
        size: u64,
        metrics: &CacheMetrics,
        make: impl FnOnce() -> V,
    ) -> (bool, Vec<CacheKey>) {
        let mut evicted = Vec::new();
        let rank = rank_of(&key);
        if !self.region.tenants.contains_key(&key.tenant_hash)
            && !self.track_tenant(key.tenant_hash, metrics, &mut evicted)
        {
            return (false, evicted);
        }
        self.observe_bulk(&key, rank, size, metrics, &mut evicted);
        // A rank already held by another key is a 64-bit collision; the
        // newcomer is simply not cached.
        if self.region.by_rank.contains_key(&rank) || u128::from(rank) >= self.bulk_rank_bound() {
            return (false, evicted);
        }
        while !self.bulk_fits(size) {
            if let Some((&top_rank, &top_key)) = self.region.by_rank.last_key_value()
                && top_rank > rank
            {
                self.evict_bulk(top_rank, top_key, metrics);
                evicted.push(top_key);
                continue;
            }
            if self.region.bytes + size <= self.region_share_bytes && !self.small.is_empty() {
                if let Step::Evicted(key) = self.evict_from_small(metrics) {
                    evicted.push(key);
                }
                continue;
            }
            return (false, evicted);
        }
        let window = self
            .region
            .tenants
            .get(&key.tenant_hash)
            .map_or(0, |pass| pass.window);
        self.forget_ghost(&key);
        self.entries.insert(
            key,
            Entry {
                value: make(),
                size,
                freq: 0,
                bulk: true,
                window,
            },
        );
        self.region.by_rank.insert(rank, key);
        self.region.bytes += size;
        metrics.record_admission(size);
        (true, evicted)
    }

    /// Whether one more region entry of `size` bytes fits every bound with
    /// its bookkeeping charged.
    fn bulk_fits(&self, size: u64) -> bool {
        let bookkeeping = self.bookkeeping_bytes() + REGION_ENTRY_BOOKKEEPING_BYTES;
        bookkeeping <= self.bookkeeping_budget
            && self
                .total_bytes()
                .saturating_add(size)
                .saturating_add(bookkeeping)
                <= self.limits.max_bytes
            && self.entries.len() < self.limits.max_entries
    }

    /// The bytes the region can hold now: whatever the small and main queues
    /// leave unused, or its share where only the small queue stands in the
    /// way of it.
    fn region_capacity(&self) -> u64 {
        let bookkeeping = self.bookkeeping_bytes();
        let unused = self
            .limits
            .max_bytes
            .saturating_sub(self.small_bytes + self.main_bytes + bookkeeping);
        let share = self.region_share_bytes.min(
            self.limits
                .max_bytes
                .saturating_sub(self.main_bytes + bookkeeping),
        );
        unused.max(share)
    }

    /// Exclusive upper bound on the rank a bulk admission may have, over the
    /// rank space `[0, 2^64)`: all of it while the tracked tenants' pass bytes
    /// fit the region, else the region's share of those bytes with a quarter
    /// of slack. The slack keeps an overestimated pass from leaving the
    /// region short; rank eviction trims what an underestimate lets in.
    fn bulk_rank_bound(&self) -> u128 {
        let full = 1u128 << 64;
        let capacity = self.region_capacity();
        let pass_bytes = self
            .region
            .tenants
            .values()
            .fold(0u64, |sum, pass| sum.saturating_add(pass.pass_bytes()));
        if pass_bytes <= capacity {
            return full;
        }
        let fraction = capacity as f64 * 1.25 / pass_bytes as f64;
        ((fraction * full as f64) as u128).min(full)
    }

    /// Counts one bulk read of `key` against its tracked tenant: the window's
    /// traffic and sketch, a window stamp on `key`'s region entry if it has
    /// one, and, once the window's traffic reaches twice the tenant's pass
    /// bytes, a rotation that evicts the tenant's region entries no read
    /// touched during the window it closes, then retires idle tenants.
    fn observe_bulk(
        &mut self,
        key: &CacheKey,
        rank: u64,
        size: u64,
        metrics: &CacheMetrics,
        evicted: &mut Vec<CacheKey>,
    ) {
        self.region.clock = self.region.clock.saturating_add(size);
        let clock = self.region.clock;
        let floor = self.limits.max_bytes.saturating_mul(WINDOW_FLOOR_MULTIPLE);
        let ceiling = self
            .limits
            .max_bytes
            .saturating_mul(WINDOW_CEILING_MULTIPLE);
        let Some(pass) = self.region.tenants.get_mut(&key.tenant_hash) else {
            return;
        };
        pass.last_read = clock;
        pass.window_traffic = pass.window_traffic.saturating_add(size);
        pass.sketch.observe(rank, size);
        let window = pass.window;
        let rotate =
            pass.window_traffic >= pass.pass_bytes().saturating_mul(2).clamp(floor, ceiling);
        if rotate {
            pass.previous_bytes = pass.sketch.estimate();
            pass.sketch.clear();
            pass.window = pass.window.wrapping_add(1);
            pass.window_traffic = 0;
        }
        if let Some(entry) = self.entries.get_mut(key)
            && entry.bulk
        {
            entry.window = window;
        }
        if rotate {
            self.evict_unread(key.tenant_hash, metrics, evicted);
            self.retire_idle(metrics, evicted);
        }
    }

    /// Starts pass accounting for `tenant`, retiring the least recently read
    /// tenant, evicting the highest-ranked region entry, or, while the region
    /// is below its share, evicting from the small queue until its
    /// bookkeeping fits. False when it cannot fit.
    fn track_tenant(
        &mut self,
        tenant: [u8; 16],
        metrics: &CacheMetrics,
        evicted: &mut Vec<CacheKey>,
    ) -> bool {
        loop {
            let full = self.region.tenants.len() >= MAX_BULK_TENANTS
                || self.bookkeeping_bytes() + TENANT_BOOKKEEPING_BYTES > self.bookkeeping_budget;
            if !full {
                if self.charged_bytes() + TENANT_BOOKKEEPING_BYTES <= self.limits.max_bytes {
                    self.region
                        .tenants
                        .insert(tenant, TenantPass::new(self.region.clock));
                    return true;
                }
                let step = if !self.region.by_rank.is_empty() {
                    self.evict_bulk_top(metrics)
                } else if TENANT_BOOKKEEPING_BYTES <= self.region_share_bytes
                    && !self.small.is_empty()
                {
                    self.evict_from_small(metrics)
                } else {
                    Step::Empty
                };
                match step {
                    Step::Evicted(key) => evicted.push(key),
                    Step::Skipped => {}
                    Step::Empty => return false,
                }
                continue;
            }
            let Some(oldest) = self
                .region
                .tenants
                .iter()
                .min_by_key(|(_, pass)| pass.last_read)
                .map(|(tenant, _)| *tenant)
            else {
                return false;
            };
            self.retire_tenant(oldest, metrics, evicted);
        }
    }

    /// Evicts `tenant`'s region entries last read before the window that
    /// just closed.
    fn evict_unread(
        &mut self,
        tenant: [u8; 16],
        metrics: &CacheMetrics,
        evicted: &mut Vec<CacheKey>,
    ) {
        let Some(current) = self.region.tenants.get(&tenant).map(|pass| pass.window) else {
            return;
        };
        let unread: Vec<(u64, CacheKey)> = self
            .region
            .by_rank
            .iter()
            .filter(|(_, key)| {
                key.tenant_hash == tenant
                    && self.entries.get(key).is_some_and(|entry| {
                        entry.window.wrapping_add(1) != current && entry.window != current
                    })
            })
            .map(|(rank, key)| (*rank, *key))
            .collect();
        for (rank, key) in unread {
            self.evict_bulk(rank, key, metrics);
            evicted.push(key);
        }
    }

    /// Retires every tenant with no bulk read in the last
    /// [`WINDOW_CEILING_MULTIPLE`] times `max_bytes` of bulk traffic.
    fn retire_idle(&mut self, metrics: &CacheMetrics, evicted: &mut Vec<CacheKey>) {
        let clock = self.region.clock;
        let idle_after = self
            .limits
            .max_bytes
            .saturating_mul(WINDOW_CEILING_MULTIPLE);
        let idle: Vec<[u8; 16]> = self
            .region
            .tenants
            .iter()
            .filter(|(_, pass)| clock.saturating_sub(pass.last_read) > idle_after)
            .map(|(tenant, _)| *tenant)
            .collect();
        for tenant in idle {
            self.retire_tenant(tenant, metrics, evicted);
        }
    }

    /// Drops `tenant`'s pass accounting and evicts its region entries.
    fn retire_tenant(
        &mut self,
        tenant: [u8; 16],
        metrics: &CacheMetrics,
        evicted: &mut Vec<CacheKey>,
    ) {
        self.region.tenants.remove(&tenant);
        let owned: Vec<(u64, CacheKey)> = self
            .region
            .by_rank
            .iter()
            .filter(|(_, key)| key.tenant_hash == tenant)
            .map(|(rank, key)| (*rank, *key))
            .collect();
        for (rank, key) in owned {
            self.evict_bulk(rank, key, metrics);
            evicted.push(key);
        }
    }

    fn evict_bulk(&mut self, rank: u64, key: CacheKey, metrics: &CacheMetrics) {
        self.region.by_rank.remove(&rank);
        if let Some(entry) = self.entries.remove(&key) {
            self.region.bytes -= entry.size;
            metrics.record_eviction();
        }
    }

    /// Evicts the highest-ranked region entry, which keeps what remains the
    /// lowest-ranked prefix and so the same subset on the next pass.
    fn evict_bulk_top(&mut self, metrics: &CacheMetrics) -> Step {
        let Some((&rank, &key)) = self.region.by_rank.last_key_value() else {
            return Step::Empty;
        };
        self.evict_bulk(rank, key, metrics);
        Step::Evicted(key)
    }

    /// Seeds an entry directly into the small (probation) queue with no
    /// admission check, no eviction pass, and no metrics -- for populating
    /// accounting from files a startup scan already found on disk, which
    /// exist regardless of whether they would pass today's admission rules
    /// (e.g. a limit that shrank since they were written). A later
    /// [`S3Fifo::insert`] enforces bounds and will evict them like any
    /// other entry if the tier is over budget.
    pub(crate) fn seed(&mut self, key: CacheKey, value: V, size: u64) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key, Entry::interactive(value, size));
        self.small.push_back(key);
        self.small_bytes += size;
    }

    /// Drops `key` from this structure's accounting regardless of which
    /// queue or region holds it, without evicting anything else or touching
    /// metrics: for a caller that has independently decided an entry is gone
    /// (e.g. the backing file failed to open or verify), not for ordinary
    /// capacity-driven eviction.
    pub(crate) fn remove(&mut self, key: &CacheKey) -> bool {
        let Some(entry) = self.entries.remove(key) else {
            return false;
        };
        if entry.bulk {
            self.region.by_rank.remove(&rank_of(key));
            self.region.bytes -= entry.size;
        } else if let Some(pos) = self.small.iter().position(|k| k == key) {
            self.small.remove(pos);
            self.small_bytes -= entry.size;
        } else if let Some(pos) = self.main.iter().position(|k| k == key) {
            self.main.remove(pos);
            self.main_bytes -= entry.size;
        }
        true
    }

    /// Removes every resident entry whose stored value satisfies `expired`,
    /// returning their keys so the caller can count them or free whatever the
    /// values referenced. Queue membership, byte totals, and entry count are
    /// all kept consistent via [`S3Fifo::remove`]; ghost entries are left
    /// untouched (they hold no resident bytes). Used by the RAM tier's periodic
    /// age sweep, which stamps each value with its write time; the disk tier
    /// ages entries by their on-disk header instead and does not use this.
    pub(crate) fn drain_where<F>(&mut self, mut expired: F) -> Vec<CacheKey>
    where
        F: FnMut(&V) -> bool,
    {
        let keys: Vec<CacheKey> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| expired(&entry.value).then_some(*key))
            .collect();
        for key in &keys {
            self.remove(key);
        }
        keys
    }

    /// Eviction for an interactive or maintenance admission: region bytes
    /// above the region's share go first, highest rank first, then the
    /// S3-FIFO order over the small and main queues, then the rest of the
    /// region.
    fn evict_to_bounds(&mut self, metrics: &CacheMetrics) -> Vec<CacheKey> {
        let mut evicted_keys = Vec::new();
        while self.charged_bytes() > self.limits.max_bytes
            || self.entries.len() > self.limits.max_entries
        {
            let step = if self.region.bytes > self.region_share_bytes {
                self.evict_bulk_top(metrics)
            } else if self.small_bytes > self.small_quota_bytes && !self.small.is_empty() {
                self.evict_from_small(metrics)
            } else if !self.main.is_empty() {
                self.evict_from_main(metrics)
            } else if !self.small.is_empty() {
                self.evict_from_small(metrics)
            } else {
                self.evict_bulk_top(metrics)
            };
            match step {
                Step::Empty => break,
                Step::Skipped => {}
                Step::Evicted(key) => evicted_keys.push(key),
            }
        }
        evicted_keys
    }

    /// Pop the front of the small queue. Promotes it to main if it was
    /// touched again after admission (`freq > 0`); otherwise evicts it and
    /// remembers the key in the ghost queue.
    fn evict_from_small(&mut self, metrics: &CacheMetrics) -> Step {
        let Some(key) = self.small.pop_front() else {
            return Step::Empty;
        };
        let Some(entry) = self.entries.remove(&key) else {
            return Step::Skipped;
        };
        let size = entry.size;
        self.small_bytes -= size;
        if entry.freq > 0 {
            self.entries
                .insert(key, Entry::interactive(entry.value, size));
            self.main.push_back(key);
            self.main_bytes += size;
            Step::Skipped
        } else {
            metrics.record_eviction();
            self.ghost.push_back(key);
            self.ghost_set.insert(key);
            while self.ghost.len() > self.ghost_capacity {
                if let Some(oldest) = self.ghost.pop_front() {
                    self.ghost_set.remove(&oldest);
                }
            }
            Step::Evicted(key)
        }
    }

    /// CLOCK sweep over the main queue: an entry with `freq > 0` gets one
    /// more lap with `freq` decremented; the first with `freq == 0` is
    /// evicted permanently (no ghost entry; ghost exists to give
    /// probation entries a fair second chance, not to remember main
    /// evictions).
    fn evict_from_main(&mut self, metrics: &CacheMetrics) -> Step {
        loop {
            let Some(key) = self.main.pop_front() else {
                return Step::Empty;
            };
            let Some(mut entry) = self.entries.remove(&key) else {
                continue;
            };
            if entry.freq > 0 {
                entry.freq -= 1;
                self.entries.insert(key, entry);
                self.main.push_back(key);
                continue;
            }
            self.main_bytes -= entry.size;
            metrics.record_eviction();
            return Step::Evicted(key);
        }
    }

    #[cfg(test)]
    fn main_bytes(&self) -> u64 {
        self.main_bytes
    }

    #[cfg(test)]
    pub(crate) fn region_bytes(&self) -> u64 {
        self.region.bytes
    }

    /// `key`'s rank and the current exclusive rank bound on bulk admissions.
    #[cfg(test)]
    pub(crate) fn rank_and_bound(&self, key: &CacheKey) -> (u64, u128) {
        (rank_of(key), self.bulk_rank_bound())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn key(tenant: u8, n: u64, len: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([tenant; 16], content_hash, 0, len)
    }

    /// One read the way a funnel issues it: a lookup, and on a miss an
    /// admission of the fetched bytes. True on a hit.
    fn touch(
        fifo: &mut S3Fifo<()>,
        key: CacheKey,
        intent: AccessIntent,
        metrics: &CacheMetrics,
    ) -> bool {
        if fifo.get_with_intent(&key, intent, metrics).0.is_some() {
            return true;
        }
        fifo.insert_with_intent(key, (), key.len, intent, metrics);
        false
    }

    /// Every bound the cache promises, checked after each step of a test.
    fn assert_within_bounds<V: Clone>(fifo: &S3Fifo<V>) {
        let limits = fifo.limits;
        assert!(
            fifo.total_bytes() + fifo.bookkeeping_bytes() <= limits.max_bytes,
            "payload {} plus bookkeeping {} exceeds max_bytes {}",
            fifo.total_bytes(),
            fifo.bookkeeping_bytes(),
            limits.max_bytes
        );
        assert!(
            fifo.bookkeeping_bytes() <= limits.max_bytes / 100,
            "bookkeeping {} exceeds 1% of max_bytes {}",
            fifo.bookkeeping_bytes(),
            limits.max_bytes
        );
        assert!(fifo.len() <= limits.max_entries);
    }

    /// ADR-3072 decision 3, the loop row: N keys over a cache of C < N entries,
    /// all bulk reads, serve at least 0.8 x C/N of their touches on passes 2
    /// and 3, and the same keys on both.
    ///
    /// FLIP: reading every intent as `Interactive` (the policy before intents
    /// existed: no `insert_bulk` branch in `insert_with_intent`, no intent
    /// term in `promote_to_main`, and `hit_interactive` for the `BulkScan`
    /// arm of `get_with_intent`) serves 0 touches on pass 2, since a ghost
    /// window of one key promotes nothing. Promoting every bulk miss to main
    /// (`promote_to_main` true for `BulkScan`, no `insert_bulk` branch) serves
    /// 0 as well. FIFO eviction inside the region also serves 0; see
    /// `a_reordered_bulk_scan_serves_the_same_subset`.
    #[test]
    fn repeated_bulk_scan_larger_than_the_cache_serves_a_stable_subset() {
        const N: u64 = 2_000;
        const C: u64 = 1_000;
        const ENTRY: u64 = 64 * 1024;
        // The server's 64 MiB entry cap, against which today's ghost sizing
        // remembers max_bytes / max_entry_bytes keys: here one.
        let limits = CacheLimits::new(C * ENTRY, 1_000_000, 64 * 1024 * 1024);
        let metrics = CacheMetrics::default();
        let mut fifo = S3Fifo::<()>::new(limits);

        let mut served: Vec<BTreeSet<u64>> = Vec::new();
        for _pass in 0..3 {
            let mut hits = BTreeSet::new();
            for n in 0..N {
                if touch(
                    &mut fifo,
                    key(1, n, ENTRY),
                    AccessIntent::BulkScan,
                    &metrics,
                ) {
                    hits.insert(n);
                }
                assert_within_bounds(&fifo);
            }
            served.push(hits);
        }

        let floor = (0.8 * C as f64 / N as f64 * N as f64) as usize;
        for pass in [1, 2] {
            assert!(
                served[pass].len() >= floor,
                "pass {} served {} of {N} touches, under the floor of {floor}",
                pass + 1,
                served[pass].len()
            );
        }
        assert_eq!(
            served[1], served[2],
            "passes 2 and 3 must serve the same keys"
        );
        assert_eq!(fifo.main_bytes(), 0, "no bulk read reaches main");
    }

    /// The loop row again with each pass reading the keys in a different
    /// order, as parallel partitions do: the served subset is fixed by rank,
    /// not by read order.
    ///
    /// FLIP: FIFO eviction inside the region (keying `by_rank` by the bulk
    /// clock at admission and evicting its first entry, whatever the
    /// newcomer's rank) serves 564 touches on pass 2 and a different set on
    /// pass 3, since more keys pass the rank bound than the region holds. Any
    /// victim order that is a fixed function of the key (lowest rank first,
    /// map order) stays stable here too; only recency breaks it.
    #[test]
    fn a_reordered_bulk_scan_serves_the_same_subset() {
        const N: u64 = 2_000;
        const C: u64 = 1_000;
        const ENTRY: u64 = 64 * 1024;
        let limits = CacheLimits::new(C * ENTRY, 1_000_000, 64 * 1024 * 1024);
        let metrics = CacheMetrics::default();
        let mut fifo = S3Fifo::<()>::new(limits);

        let mut served: Vec<BTreeSet<u64>> = Vec::new();
        // Multipliers coprime to N, so each pass is a permutation of 0..N.
        for (stride, offset) in [(1u64, 0u64), (7, 311), (13, 1_003)] {
            let mut hits = BTreeSet::new();
            for i in 0..N {
                let n = (i * stride + offset) % N;
                if touch(
                    &mut fifo,
                    key(1, n, ENTRY),
                    AccessIntent::BulkScan,
                    &metrics,
                ) {
                    hits.insert(n);
                }
            }
            served.push(hits);
        }
        let floor = (0.8 * C as f64) as usize;
        assert!(
            served[1].len() >= floor,
            "pass 2 served {}",
            served[1].len()
        );
        assert_eq!(
            served[1], served[2],
            "passes 2 and 3 must serve the same keys"
        );
    }

    /// The server's cache geometry (ADR-3072 decision 3 acceptance table): an
    /// 11.5 GB cache with the 1,000,000-entry and 64 MiB entry caps
    /// `services/ravel-server/src/store.rs` sets, a 33 MB hot set of 16 KiB
    /// interactive ranges, and a loop of 25 MB bulk objects 15 GB long.
    const SERVER_MAX_BYTES: u64 = 11_500_000_000;
    const HOT_RANGE: u64 = 16 * 1024;
    const HOT_RANGES: u64 = 33_000_000 / HOT_RANGE;
    const BULK_OBJECT: u64 = 25_000_000;
    const BULK_LOOP: u64 = 600;
    /// Each hot range is read once per this many bulk reads, interleaved.
    const BULK_READS_PER_HOT_READ: u64 = 1_000;

    fn server_fifo() -> S3Fifo<()> {
        S3Fifo::new(CacheLimits::new(
            SERVER_MAX_BYTES,
            1_000_000,
            64 * 1024 * 1024,
        ))
    }

    /// Hot-set hits over every read of a range after its first, and the bulk
    /// loop's hits, for `bulk_reads` bulk reads with the hot set interleaved
    /// from bulk read `hot_from` on.
    struct HotSetRun {
        hot_hits: u64,
        hot_rereads: u64,
        bulk_hits: u64,
        bulk_reads_after_first_pass: u64,
    }

    fn run_hot_set_against_bulk_loop(hot_from: u64, bulk_reads: u64) -> (S3Fifo<()>, HotSetRun) {
        let metrics = CacheMetrics::default();
        let mut fifo = server_fifo();
        let mut run = HotSetRun {
            hot_hits: 0,
            hot_rereads: 0,
            bulk_hits: 0,
            bulk_reads_after_first_pass: 0,
        };
        let mut hot_reads = 0u64;
        for bulk_read in 0..bulk_reads {
            let hit = touch(
                &mut fifo,
                key(2, bulk_read % BULK_LOOP, BULK_OBJECT),
                AccessIntent::BulkScan,
                &metrics,
            );
            if bulk_read >= BULK_LOOP {
                run.bulk_reads_after_first_pass += 1;
                run.bulk_hits += u64::from(hit);
            }
            if bulk_read < hot_from {
                continue;
            }
            // Spread the hot set's reads so each range is read once every
            // BULK_READS_PER_HOT_READ bulk reads.
            let due = (bulk_read - hot_from + 1) * HOT_RANGES / BULK_READS_PER_HOT_READ;
            while hot_reads < due {
                let range = hot_reads % HOT_RANGES;
                let hit = touch(
                    &mut fifo,
                    key(1, 1_000_000 + range, HOT_RANGE),
                    AccessIntent::Interactive,
                    &metrics,
                );
                if hot_reads >= HOT_RANGES {
                    run.hot_rereads += 1;
                    run.hot_hits += u64::from(hit);
                }
                hot_reads += 1;
            }
            assert_within_bounds(&fifo);
        }
        (fifo, run)
    }

    fn assert_hot_set_held(run: &HotSetRun, fifo: &S3Fifo<()>) {
        assert!(
            run.hot_rereads >= 10 * HOT_RANGES,
            "too few hot re-reads: {}",
            run.hot_rereads
        );
        let hot_rate = run.hot_hits as f64 / run.hot_rereads as f64;
        assert!(
            hot_rate >= 0.99,
            "hot-set hit rate {hot_rate:.4} ({} of {}) is under 0.99",
            run.hot_hits,
            run.hot_rereads
        );
        // The bulk loop must actually have been cached beside the hot set, or
        // the hot set held only because the region never filled.
        let bulk_rate = run.bulk_hits as f64 / run.bulk_reads_after_first_pass as f64;
        assert!(
            bulk_rate >= 0.5 * SERVER_MAX_BYTES as f64 / (BULK_LOOP * BULK_OBJECT) as f64,
            "the bulk loop was served at only {bulk_rate:.3}"
        );
        assert!(
            fifo.main_bytes() <= HOT_RANGES * HOT_RANGE,
            "bulk bytes reached main: {} bytes in main",
            fifo.main_bytes()
        );
        assert!(fifo.region_bytes() > SERVER_MAX_BYTES / 2);
    }

    /// FLIP: letting a bulk admission with no higher-ranked region entry to
    /// evict fall back to the interactive eviction order (replacing the final
    /// `return (false, evicted)` in `insert_bulk`'s room loop with the
    /// `evict_from_small`/`evict_from_main` order of `evict_to_bounds`) evicts
    /// hot ranges to make room for 25 MB objects: hot-set hits fall to 0 of
    /// 22,154. Routing the `BulkScan` arm of `get_with_intent` to
    /// `hit_interactive`, so a bulk hit raises `freq` and promotes, moves the
    /// loop into main (11,482,997,376 bytes), which the main-queue assertion
    /// catches; the hot set itself still hits 22,154 of 22,154 there, because
    /// bulk admissions cannot evict main and the rank bound closes once main
    /// is full.
    #[test]
    fn interactive_hot_set_survives_a_bulk_scan_loop_at_server_geometry() {
        let (fifo, run) = run_hot_set_against_bulk_loop(0, 12 * BULK_READS_PER_HOT_READ);
        assert_hot_set_held(&run, &fifo);
    }

    /// The same geometry with the hot set first read while the loop is
    /// running and the region already holds every byte the cache has. The
    /// same FLIPs fail it the same way.
    #[test]
    fn a_hot_set_established_during_a_bulk_scan_survives() {
        let (fifo, run) = run_hot_set_against_bulk_loop(
            3 * BULK_READS_PER_HOT_READ,
            15 * BULK_READS_PER_HOT_READ,
        );
        assert_hot_set_held(&run, &fifo);
    }

    /// Region bookkeeping is charged inside `max_bytes` and never exceeds one
    /// percent of it: under a loop of small entries that would need more, with
    /// more bulk tenants than are tracked at once, and with interactive
    /// admissions filling the rest of the cache.
    ///
    /// FLIP: dropping the `bookkeeping <= self.bookkeeping_budget` term from
    /// `bulk_fits` lets the 4 KiB loop's rank map grow past 1% of max_bytes;
    /// dropping `bookkeeping_bytes()` from `charged_bytes` lets the
    /// interactive fill reach max_bytes of payload on top of it.
    #[test]
    fn bulk_region_bookkeeping_stays_inside_the_cap() {
        const MAX_BYTES: u64 = 64 * 1024 * 1024;
        const SMALL: u64 = 4 * 1024;
        let limits = CacheLimits::new(MAX_BYTES, 1_000_000, 16 * 1024 * 1024);
        let metrics = CacheMetrics::default();
        let mut fifo = S3Fifo::<()>::new(limits);
        let budget = MAX_BYTES / 100;

        // One tenant's loop of small entries: the bookkeeping budget, not the
        // bytes, bounds the region.
        for _pass in 0..3 {
            for n in 0..20_000 {
                touch(
                    &mut fifo,
                    key(1, n, SMALL),
                    AccessIntent::BulkScan,
                    &metrics,
                );
                assert_within_bounds(&fifo);
            }
        }
        assert!(
            fifo.bookkeeping_bytes() + REGION_ENTRY_BOOKKEEPING_BYTES > budget,
            "the budget never bound: {} of {budget}",
            fifo.bookkeeping_bytes()
        );
        assert!(fifo.region_bytes() < MAX_BYTES / 2);

        // More tenants than the region tracks.
        for round in 0..4u64 {
            for tenant in 0..100u8 {
                for n in 0..50 {
                    let k = key(tenant.wrapping_add(10), round * 50 + n, SMALL);
                    touch(&mut fifo, k, AccessIntent::BulkScan, &metrics);
                    assert_within_bounds(&fifo);
                }
            }
            assert!(fifo.region.tenants.len() <= MAX_BULK_TENANTS);
        }

        // Interactive admissions fill the cache: payload stops at max_bytes
        // less the bookkeeping, not at max_bytes.
        const FILL: u64 = 16 * 1024;
        for n in 0..2 * MAX_BYTES / FILL {
            touch(
                &mut fifo,
                key(200, n, FILL),
                AccessIntent::Interactive,
                &metrics,
            );
            assert_within_bounds(&fifo);
        }
        let bookkeeping = fifo.bookkeeping_bytes();
        assert!(bookkeeping > FILL);
        assert!(fifo.total_bytes() + bookkeeping <= MAX_BYTES);
        assert!(
            fifo.total_bytes() + bookkeeping + FILL > MAX_BYTES,
            "the interactive fill did not reach the cap: {} + {bookkeeping}",
            fifo.total_bytes()
        );
    }

    /// A maintenance read admits to the small queue, and neither its own hits
    /// nor a ghost hit move an entry into main, while the same pattern read
    /// interactively does reach main.
    ///
    /// FLIP: bumping `freq` in the `Maintenance` arm of `get_with_intent`
    /// promotes the re-read entries at their small-queue eviction; dropping
    /// `intent == AccessIntent::Interactive &&` from `promote_to_main` admits
    /// the ghost key straight to main. Either makes `main_bytes` nonzero.
    #[test]
    fn maintenance_reads_never_reach_main() {
        const ENTRY: u64 = 1024;
        let limits = CacheLimits::new(100 * ENTRY, 1_000, 10 * ENTRY);
        let metrics = CacheMetrics::default();

        let mut fifo = S3Fifo::<()>::new(limits);
        for n in 0..20 {
            touch(
                &mut fifo,
                key(1, n, ENTRY),
                AccessIntent::Maintenance,
                &metrics,
            );
        }
        for _ in 0..3 {
            for n in 0..20 {
                assert!(touch(
                    &mut fifo,
                    key(1, n, ENTRY),
                    AccessIntent::Maintenance,
                    &metrics
                ));
            }
        }
        let mut last_evicted = None;
        for n in 1_000..1_300 {
            let (_, evicted) = fifo.insert_with_intent(
                key(1, n, ENTRY),
                (),
                ENTRY,
                AccessIntent::Maintenance,
                &metrics,
            );
            last_evicted = evicted.last().copied().or(last_evicted);
            assert_eq!(fifo.main_bytes(), 0, "a maintenance read reached main");
        }
        let ghost_key = last_evicted.expect("the scan evicted from probation");
        assert!(fifo.ghost_set.contains(&ghost_key));
        fifo.insert_with_intent(ghost_key, (), ENTRY, AccessIntent::Maintenance, &metrics);
        assert_eq!(
            fifo.main_bytes(),
            0,
            "a ghost hit promoted a maintenance read"
        );

        // Control: the interactive reading of the same pattern does promote.
        let mut fifo = S3Fifo::<()>::new(limits);
        for n in 0..20 {
            touch(
                &mut fifo,
                key(1, n, ENTRY),
                AccessIntent::Interactive,
                &metrics,
            );
            touch(
                &mut fifo,
                key(1, n, ENTRY),
                AccessIntent::Interactive,
                &metrics,
            );
        }
        for n in 1_000..1_300 {
            fifo.insert_with_intent(
                key(1, n, ENTRY),
                (),
                ENTRY,
                AccessIntent::Interactive,
                &metrics,
            );
        }
        assert!(fifo.main_bytes() > 0);
    }

    /// Bulk admissions below the region's share evict small-queue entries and
    /// never main-queue ones.
    ///
    /// FLIP: letting a bulk admission with no higher-ranked region entry to
    /// evict fall back to the `evict_to_bounds` order (the replacement the
    /// hot-set tests name) leaves 448 of the 600 main-queue entries.
    #[test]
    fn bulk_admissions_never_evict_main() {
        const ENTRY: u64 = 64 * 1024;
        let limits = CacheLimits::new(1_000 * ENTRY, 1_000_000, 64 * 1024 * 1024);
        let metrics = CacheMetrics::default();
        let mut fifo = S3Fifo::<()>::new(limits);
        let working_set: Vec<CacheKey> = (0..600).map(|n| key(1, n, ENTRY)).collect();
        for k in &working_set {
            touch(&mut fifo, *k, AccessIntent::Interactive, &metrics);
            assert!(touch(&mut fifo, *k, AccessIntent::Interactive, &metrics));
        }
        for n in 10_000..10_500 {
            touch(
                &mut fifo,
                key(1, n, ENTRY),
                AccessIntent::Interactive,
                &metrics,
            );
        }
        assert_eq!(
            fifo.main_bytes(),
            600 * ENTRY,
            "precondition: the set is in main"
        );

        for _pass in 0..2 {
            for n in 0..3_000 {
                touch(
                    &mut fifo,
                    key(2, n, ENTRY),
                    AccessIntent::BulkScan,
                    &metrics,
                );
                assert_within_bounds(&fifo);
            }
        }
        assert!(
            fifo.region_bytes() > 300 * ENTRY,
            "the region took the small queue's bytes"
        );
        assert_eq!(fifo.main_bytes(), 600 * ENTRY);
        assert!(working_set.iter().all(|k| fifo.contains(k)));
    }

    /// Interactive admissions take back the bytes the region holds above its
    /// share and no more: under an interactive stream larger than the cache,
    /// the region ends within one entry of its share.
    ///
    /// FLIP: evicting the region's top entry whenever it is non-empty, rather
    /// than only above `region_share_bytes`, in `evict_to_bounds` empties the
    /// region.
    #[test]
    fn interactive_admissions_reclaim_bulk_bytes_above_the_share_only() {
        const ENTRY: u64 = 64 * 1024;
        let limits = CacheLimits::new(1_000 * ENTRY, 1_000_000, 64 * 1024 * 1024);
        let metrics = CacheMetrics::default();
        let mut fifo = S3Fifo::<()>::new(limits);
        for n in 0..900 {
            touch(
                &mut fifo,
                key(1, n, ENTRY),
                AccessIntent::BulkScan,
                &metrics,
            );
        }
        assert_eq!(
            fifo.region_bytes(),
            900 * ENTRY,
            "the region grew past its share"
        );
        for n in 10_000..13_000 {
            touch(
                &mut fifo,
                key(2, n, ENTRY),
                AccessIntent::Interactive,
                &metrics,
            );
            assert_within_bounds(&fifo);
        }
        let share = 500 * ENTRY;
        assert!(
            fifo.region_bytes() <= share,
            "region {}",
            fifo.region_bytes()
        );
        assert!(
            fifo.region_bytes() + ENTRY > share,
            "region {}",
            fifo.region_bytes()
        );
    }

    /// A tenant whose bulk reads move to other keys gets the region back: its
    /// old loop's entries go at the window rotation after the one they were
    /// last read in, and the new loop is then served as fully as the first
    /// was. The window that spans the switch counts both loops' bytes, so it
    /// runs about four passes of the new loop, which is served 494 touches a
    /// pass until then.
    ///
    /// FLIP: removing the `evict_unread` call from `observe_bulk` leaves 503
    /// of the old loop's keys resident at the end.
    #[test]
    fn a_tenant_that_moves_to_another_loop_gets_the_region_back() {
        const N: u64 = 2_000;
        const C: u64 = 1_000;
        const ENTRY: u64 = 64 * 1024;
        let limits = CacheLimits::new(C * ENTRY, 1_000_000, 64 * 1024 * 1024);
        let metrics = CacheMetrics::default();
        let mut fifo = S3Fifo::<()>::new(limits);
        for _pass in 0..3 {
            for n in 0..N {
                touch(
                    &mut fifo,
                    key(1, n, ENTRY),
                    AccessIntent::BulkScan,
                    &metrics,
                );
            }
        }
        let mut served = Vec::new();
        for _pass in 0..12 {
            let mut hits = BTreeSet::new();
            for n in 100_000..100_000 + N {
                if touch(
                    &mut fifo,
                    key(1, n, ENTRY),
                    AccessIntent::BulkScan,
                    &metrics,
                ) {
                    hits.insert(n);
                }
            }
            served.push(hits);
        }
        let old_resident = (0..N).filter(|&n| fifo.contains(&key(1, n, ENTRY))).count();
        assert_eq!(old_resident, 0, "the old loop still holds region entries");
        assert!(
            served[11].len() >= (0.8 * C as f64) as usize,
            "served {}",
            served[11].len()
        );
        assert_eq!(served[10], served[11]);
    }

    /// A tenant with no bulk read for sixteen times `max_bytes` of other
    /// tenants' bulk traffic is retired, and its region entries with it.
    ///
    /// FLIP: removing the `retire_idle` call from `observe_bulk` keeps the
    /// idle tenant's pass state and entries.
    #[test]
    fn an_idle_bulk_tenant_is_retired_with_its_entries() {
        const ENTRY: u64 = 64 * 1024;
        let limits = CacheLimits::new(1_000 * ENTRY, 1_000_000, 64 * 1024 * 1024);
        let metrics = CacheMetrics::default();
        let mut fifo = S3Fifo::<()>::new(limits);
        for n in 0..400 {
            touch(
                &mut fifo,
                key(1, n, ENTRY),
                AccessIntent::BulkScan,
                &metrics,
            );
        }
        assert!(fifo.region.tenants.contains_key(&[1u8; 16]));
        for _pass in 0..10 {
            for n in 0..2_000 {
                touch(
                    &mut fifo,
                    key(2, n, ENTRY),
                    AccessIntent::BulkScan,
                    &metrics,
                );
            }
        }
        assert!(!fifo.region.tenants.contains_key(&[1u8; 16]));
        assert!((0..400).all(|n| !fifo.contains(&key(1, n, ENTRY))));
        assert!(
            fifo.region_bytes() > 900 * ENTRY,
            "the active tenant has the region"
        );
    }

    /// The bulk flag and window stamp fit in the padding an entry already
    /// had: the sizes are those of the three fields before them.
    #[test]
    fn entry_bulk_fields_fit_in_padding() {
        assert_eq!(std::mem::size_of::<Entry<()>>(), 16);
        assert_eq!(std::mem::size_of::<Entry<[u64; 5]>>(), 56);
    }

    /// An interactive hit on a region entry moves it to the small queue with
    /// the hit counted, so it is promoted to main at its eviction from there.
    #[test]
    fn an_interactive_hit_moves_a_region_entry_to_the_small_queue() {
        const ENTRY: u64 = 64 * 1024;
        let limits = CacheLimits::new(1_000 * ENTRY, 1_000_000, 1_000 * ENTRY);
        let metrics = CacheMetrics::default();
        let mut fifo = S3Fifo::<()>::new(limits);
        let k = key(1, 1, ENTRY);
        fifo.insert_with_intent(k, (), ENTRY, AccessIntent::BulkScan, &metrics);
        assert_eq!(fifo.region_bytes(), ENTRY);
        assert!(fifo.get(&k).is_some());
        assert_eq!(fifo.region_bytes(), 0);
        assert_eq!(fifo.total_bytes(), ENTRY);
        for n in 10..2_010 {
            fifo.insert(key(1, n, ENTRY), (), ENTRY, &metrics);
        }
        assert!(
            fifo.contains(&k),
            "the read entry was promoted, not evicted"
        );
        assert_eq!(fifo.main_bytes(), ENTRY);
    }

    /// The lazy admission builds its value exactly when it stores a new
    /// entry: over a sweep of keys, sizes and intents that crosses every
    /// decline (over size, already resident, rank bound, no room), `make`
    /// runs on precisely the admissions after which the key is newly
    /// resident.
    ///
    /// FLIP: calling `make()` at the top of `insert_bulk` (a copy before the
    /// decision) runs it for the bulk keys the rank bound declines, and the
    /// first such admission fails the equality.
    #[test]
    fn a_lazy_admission_builds_its_value_only_when_it_stores_one() {
        const ENTRY: u64 = 16 * 1024;
        let limits = CacheLimits::new(64 * ENTRY, 1_000_000, 4 * ENTRY);
        let metrics = CacheMetrics::default();
        let mut fifo = S3Fifo::<u64>::new(limits);
        let intents = [
            AccessIntent::BulkScan,
            AccessIntent::Interactive,
            AccessIntent::BulkScan,
            AccessIntent::Maintenance,
        ];
        let (mut built, mut declined) = (0u32, 0u32);
        for pass in 0..3u64 {
            for n in 0..600u64 {
                let size = [ENTRY, 2 * ENTRY, 5 * ENTRY][(n % 3) as usize];
                let k = key(1 + (n % 2) as u8, n, size);
                let intent = intents[((n + pass) % 4) as usize];
                let resident_before = fifo.contains(&k);
                let mut made = false;
                fifo.insert_with_intent_lazy(k, size, intent, &metrics, || {
                    made = true;
                    n
                });
                let stored = !resident_before && fifo.contains(&k);
                assert_eq!(
                    made, stored,
                    "pass {pass} key {n} size {size} intent {intent:?}"
                );
                if made {
                    built += 1;
                } else {
                    declined += 1;
                }
                assert_within_bounds(&fifo);
            }
        }
        assert!(
            built > 0 && declined > 0,
            "{built} built, {declined} declined"
        );
    }

    /// An admission of a ghosted key under any intent drops it from the ghost
    /// queue, so a key is never resident and a ghost at once. A maintenance
    /// admission that left the ghost entry behind put the key in the ghost
    /// queue twice at its next eviction; trimming the older copy then cleared
    /// the set while the newer copy stayed, and an interactive re-admission
    /// within the ghost window was not promoted.
    ///
    /// FLIP: removing the `forget_ghost` call from `insert_bulk` fails the
    /// bulk assertion; reverting the interactive path to
    /// `intent == AccessIntent::Interactive && self.ghost_set.remove(&key)`
    /// (the ghost entry kept for maintenance) fails the maintenance one and
    /// leaves the re-admitted key out of main.
    #[test]
    fn a_maintenance_or_bulk_admission_clears_the_ghost_entry() {
        const ENTRY: u64 = 64 * 1024;
        // Ghost window max_bytes / max_entry_bytes = 20 keys.
        let limits = CacheLimits::new(100 * ENTRY, 1_000, 5 * ENTRY);
        let metrics = CacheMetrics::default();

        for intent in [AccessIntent::Maintenance, AccessIntent::BulkScan] {
            let mut fifo = S3Fifo::<()>::new(limits);
            assert_eq!(fifo.ghost_capacity, 20);
            for n in 0..90 {
                fifo.insert(key(1, n, ENTRY), (), ENTRY, &metrics);
                assert!(fifo.get(&key(1, n, ENTRY)).is_some());
            }
            let mut filler = 1_000;
            let mut insert_filler = |fifo: &mut S3Fifo<()>| {
                filler += 1;
                fifo.insert(key(1, filler, ENTRY), (), ENTRY, &metrics).1
            };
            for _ in 0..20 {
                insert_filler(&mut fifo);
            }
            assert_eq!(fifo.main_bytes(), 90 * ENTRY, "a stable main queue");

            let ghosted = key(2, 1, ENTRY);
            fifo.insert(ghosted, (), ENTRY, &metrics);
            while !insert_filler(&mut fifo).contains(&ghosted) {}
            assert!(fifo.ghost_set.contains(&ghosted));

            let (stored, _) = fifo.insert_with_intent(ghosted, (), ENTRY, intent, &metrics);
            assert!(stored && fifo.contains(&ghosted), "{intent:?} admitted it");
            assert_eq!(fifo.main_bytes(), 90 * ENTRY, "{intent:?} never promotes");
            assert!(
                !fifo.ghost_set.contains(&ghosted) && !fifo.ghost.contains(&ghosted),
                "{intent:?} admission left the key in the ghost queue"
            );
            if intent != AccessIntent::Maintenance {
                continue;
            }

            // Evict it again, then re-admit it interactively inside the ghost
            // window, after the older ghost copy would have been trimmed.
            while !insert_filler(&mut fifo).contains(&ghosted) {}
            assert_eq!(fifo.ghost.iter().filter(|k| **k == ghosted).count(), 1);
            for _ in 0..fifo.ghost_capacity - 2 {
                insert_filler(&mut fifo);
            }
            fifo.insert(ghosted, (), ENTRY, &metrics);
            assert!(
                fifo.main.contains(&ghosted),
                "a ghost hit inside the window promotes to main"
            );
        }
    }

    /// A region entry no read touches lives one to two pass windows of its
    /// tenant's bulk reads, and a window is at most sixteen times
    /// `max_bytes`: a loop of twelve times `max_bytes` is re-read within
    /// that life and served, one of forty times is served nothing.
    ///
    /// FLIP: raising `WINDOW_CEILING_MULTIPLE` to 64 serves the forty-times
    /// loop on its third pass.
    #[test]
    fn a_bulk_loop_past_32_times_the_cap_is_served_nothing() {
        const C: u64 = 100;
        const ENTRY: u64 = 64 * 1024;
        let limits = CacheLimits::new(C * ENTRY, 1_000_000, 64 * 1024 * 1024);
        let metrics = CacheMetrics::default();
        let served_on_last_pass = |multiple: u64| {
            let mut fifo = S3Fifo::<()>::new(limits);
            let mut served = 0;
            for pass in 0..3 {
                served = 0;
                for n in 0..multiple * C {
                    if touch(
                        &mut fifo,
                        key(1, n, ENTRY),
                        AccessIntent::BulkScan,
                        &metrics,
                    ) {
                        served += 1;
                    }
                }
                if pass == 0 {
                    assert_eq!(served, 0);
                }
            }
            served
        };
        assert!(served_on_last_pass(12) > 0, "a loop of 12x is served");
        assert_eq!(served_on_last_pass(40), 0, "a loop of 40x is not");
    }

    /// `f64::clamp` passes NaN through, and NaN as `u64` is 0, so a NaN
    /// share would have left no region at all; it reads as the default.
    ///
    /// FLIP: dropping the `is_nan` arm of `effective_bulk_share` gives a
    /// region share of 0.
    #[test]
    fn a_nan_bulk_share_is_not_a_zero_region() {
        let limits = CacheLimits::new(1_000_000, 1_000, 1_000);
        let nan = S3Fifo::<()>::new(limits.with_bulk_share(f64::NAN));
        let default = S3Fifo::<()>::new(limits);
        assert_eq!(nan.region_share_bytes, 500_000);
        assert_eq!(nan.region_share_bytes, default.region_share_bytes);
        let clamped = S3Fifo::<()>::new(limits.with_bulk_share(7.0));
        assert_eq!(clamped.region_share_bytes, 1_000_000);
        let clamped = S3Fifo::<()>::new(limits.with_bulk_share(-1.0));
        assert_eq!(clamped.region_share_bytes, 0);
    }
}
