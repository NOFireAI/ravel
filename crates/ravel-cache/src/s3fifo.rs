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
//! Repeated scans larger than the cache (ADR-0046 decision 6, loop
//! amendment): a hot query run re-reads the same N objects in the same
//! order over a cache that holds C < N of them. Plain S3-FIFO serves none
//! of them when the ghost window is shorter than the loop, because every
//! entry leaves probation untouched before its next touch, a pass apart.
//! Two rules make it serve a stable subset instead:
//!
//! - While main has room, an untouched entry leaving probation moves into
//!   main rather than out of the cache. Room here means room that evicts
//!   nothing, so this displaces no other entry.
//! - Once main is full, a ghost hit takes a main slot only from an entry
//!   left idle for longer than the returning key's own reuse distance, both
//!   measured in touches. Otherwise the returning key starts over in
//!   probation. Under a loop every main entry is touched once per pass, so
//!   its idle time is shorter than a pass and no ghost hit displaces it; a
//!   hot key with a short reuse distance still displaces cold main entries.

use std::collections::{HashMap, VecDeque};

use crate::key::CacheKey;
use crate::limits::CacheLimits;
use crate::metrics::CacheMetrics;

const MAX_FREQ: u8 = 3;

struct Entry<V> {
    value: V,
    size: u64,
    freq: u8,
    /// Value of [`S3Fifo::tick`] at this entry's latest touch.
    last_touch: u64,
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

pub(crate) struct S3Fifo<V> {
    entries: HashMap<CacheKey, Entry<V>>,
    small: VecDeque<CacheKey>,
    main: VecDeque<CacheKey>,
    ghost: VecDeque<CacheKey>,
    /// Ghost keys, each with the tick of its last touch while resident.
    ghost_last_touch: HashMap<CacheKey, u64>,
    small_bytes: u64,
    main_bytes: u64,
    small_quota_bytes: u64,
    main_quota_bytes: u64,
    main_quota_entries: usize,
    ghost_capacity: usize,
    /// Logical clock: advances by one on every hit and every admission.
    tick: u64,
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
        S3Fifo {
            entries: HashMap::new(),
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_last_touch: HashMap::new(),
            small_bytes: 0,
            main_bytes: 0,
            small_quota_bytes,
            main_quota_bytes: limits.max_bytes.saturating_sub(small_quota_bytes),
            main_quota_entries: limits
                .max_entries
                .saturating_sub((limits.max_entries / 10).max(1)),
            ghost_capacity,
            tick: 0,
            limits,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        self.small_bytes + self.main_bytes
    }

    pub(crate) fn contains(&self, key: &CacheKey) -> bool {
        self.entries.contains_key(key)
    }

    pub(crate) fn get(&mut self, key: &CacheKey) -> Option<V> {
        let entry = self.entries.get_mut(key)?;
        self.tick += 1;
        entry.freq = entry.freq.saturating_add(1).min(MAX_FREQ);
        entry.last_touch = self.tick;
        Some(entry.value.clone())
    }

    /// Admit `value` (of `size` bytes) under `key`. Returns whether it was
    /// admitted (`false` only if `size` exceeds `max_entry_bytes`, which is
    /// not an error, just a decline to cache) and the keys, if any, evicted
    /// to make or keep room -- the caller owns whatever those keys' values
    /// referenced (an in-memory `Bytes` just drops; the disk tier deletes
    /// the corresponding file).
    pub(crate) fn insert(
        &mut self,
        key: CacheKey,
        value: V,
        size: u64,
        metrics: &CacheMetrics,
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

        self.tick += 1;
        let now = self.tick;
        let mut evicted = Vec::new();
        let ghost_touch = self.ghost_last_touch.remove(&key);
        if ghost_touch.is_some()
            && let Some(pos) = self.ghost.iter().position(|k| *k == key)
        {
            self.ghost.remove(pos);
        }
        let promote_to_main = match ghost_touch {
            None => false,
            Some(_) if self.main_has_room(size) => true,
            Some(last_touch) => match self.displace_idle_main(now - last_touch, now, metrics) {
                Some(victim) => {
                    evicted.push(victim);
                    true
                }
                None => false,
            },
        };

        self.entries.insert(
            key,
            Entry {
                value,
                size,
                freq: 0,
                last_touch: now,
            },
        );
        if promote_to_main {
            self.main.push_back(key);
            self.main_bytes += size;
        } else {
            self.small.push_back(key);
            self.small_bytes += size;
        }
        metrics.record_admission(size);

        evicted.extend(self.evict_to_bounds(metrics));
        (true, evicted)
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
        self.entries.insert(
            key,
            Entry {
                value,
                size,
                freq: 0,
                last_touch: self.tick,
            },
        );
        self.small.push_back(key);
        self.small_bytes += size;
    }

    /// Drops `key` from this structure's accounting regardless of which
    /// queue holds it, without evicting anything else or touching metrics:
    /// for a caller that has independently decided an entry is gone (e.g.
    /// the backing file failed to open or verify), not for ordinary
    /// capacity-driven eviction.
    pub(crate) fn remove(&mut self, key: &CacheKey) -> bool {
        let Some(entry) = self.entries.remove(key) else {
            return false;
        };
        if let Some(pos) = self.small.iter().position(|k| k == key) {
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

    fn evict_to_bounds(&mut self, metrics: &CacheMetrics) -> Vec<CacheKey> {
        let mut evicted_keys = Vec::new();
        while self.total_bytes() > self.limits.max_bytes
            || self.entries.len() > self.limits.max_entries
        {
            let step = if self.small_bytes > self.small_quota_bytes && !self.small.is_empty() {
                self.evict_from_small(metrics)
            } else if !self.main.is_empty() {
                self.evict_from_main(metrics)
            } else if !self.small.is_empty() {
                self.evict_from_small(metrics)
            } else {
                Step::Empty
            };
            match step {
                Step::Empty => break,
                Step::Skipped => {}
                Step::Evicted(key) => evicted_keys.push(key),
            }
        }
        evicted_keys
    }

    /// Whether main can take `size` more bytes and one more entry while
    /// staying inside its share of the bounds, so nothing is evicted to make
    /// room for it.
    fn main_has_room(&self, size: u64) -> bool {
        self.main_bytes.saturating_add(size) <= self.main_quota_bytes
            && self.main.len() < self.main_quota_entries
    }

    /// For a ghost hit when main is full: evicts the main entry at the front
    /// if it has been idle for longer than `reuse_distance` ticks, the
    /// returning key's own gap between touches. A front entry touched more
    /// recently than that moves to the back instead, and nothing is evicted.
    fn displace_idle_main(
        &mut self,
        reuse_distance: u64,
        now: u64,
        metrics: &CacheMetrics,
    ) -> Option<CacheKey> {
        loop {
            let key = self.main.pop_front()?;
            let Some(entry) = self.entries.get(&key) else {
                continue;
            };
            if now - entry.last_touch <= reuse_distance {
                self.main.push_back(key);
                return None;
            }
            let size = entry.size;
            self.entries.remove(&key);
            self.main_bytes -= size;
            metrics.record_eviction();
            return Some(key);
        }
    }

    /// Pop the front of the small queue. Promotes it to main if it was
    /// touched again after admission (`freq > 0`), or moves it there if main
    /// has room; otherwise evicts it and remembers the key in the ghost
    /// queue.
    fn evict_from_small(&mut self, metrics: &CacheMetrics) -> Step {
        let Some(key) = self.small.pop_front() else {
            return Step::Empty;
        };
        let Some(entry) = self.entries.remove(&key) else {
            return Step::Skipped;
        };
        let size = entry.size;
        self.small_bytes -= size;
        if entry.freq > 0 || self.main_has_room(size) {
            self.entries.insert(
                key,
                Entry {
                    value: entry.value,
                    size,
                    freq: 0,
                    last_touch: entry.last_touch,
                },
            );
            self.main.push_back(key);
            self.main_bytes += size;
            Step::Skipped
        } else {
            metrics.record_eviction();
            if self
                .ghost_last_touch
                .insert(key, entry.last_touch)
                .is_none()
            {
                self.ghost.push_back(key);
            }
            while self.ghost.len() > self.ghost_capacity {
                if let Some(oldest) = self.ghost.pop_front() {
                    self.ghost_last_touch.remove(&oldest);
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
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn key(n: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([7u8; 16], content_hash, 0, 0)
    }

    /// One touch as a read funnel makes it: a hit is served, a miss fetches
    /// and admits. Returns whether the touch was served from the cache.
    fn touch(fifo: &mut S3Fifo<()>, n: u64, size: u64, metrics: &CacheMetrics) -> bool {
        if fifo.get(&key(n)).is_some() {
            return true;
        }
        let (admitted, _) = fifo.insert(key(n), (), size, metrics);
        assert!(admitted, "every loop entry fits max_entry_bytes");
        false
    }

    /// ADR-2677 decision 3: a hot run re-reads the same objects in the same
    /// order. The geometry is the measured one: 2,617 objects of 3.8 MB under
    /// the server's 64 MiB `max_entry_bytes`, with the cache holding about
    /// half the loop (#2615 on 16 GB) and about 0.9 of it (#2639 W2). The
    /// last case caps entries at their own size, which widens the ghost to
    /// the whole cache so returning loop keys do hit it.
    #[test]
    fn repeated_scan_larger_than_the_cache_serves_a_stable_subset() {
        const N: u64 = 2_617;
        const ENTRY: u64 = 3_800_000;
        const SERVER_MAX_ENTRY: u64 = 64 * 1024 * 1024;
        for (resident, max_entry_bytes) in [
            (N / 2, SERVER_MAX_ENTRY),
            (N * 9 / 10, SERVER_MAX_ENTRY),
            (N * 9 / 10, ENTRY),
        ] {
            let limits = CacheLimits::new(resident * ENTRY, 1_000_000, max_entry_bytes);
            let metrics = CacheMetrics::default();
            let mut fifo: S3Fifo<()> = S3Fifo::new(limits);

            let mut served: Vec<BTreeSet<u64>> = Vec::new();
            for _pass in 0..3 {
                let hits = (0..N)
                    .filter(|&n| touch(&mut fifo, n, ENTRY, &metrics))
                    .collect();
                served.push(hits);
            }

            let floor = 0.8 * resident as f64 / N as f64;
            for pass in [1, 2] {
                let fraction = served[pass].len() as f64 / N as f64;
                assert!(
                    fraction >= floor,
                    "C={resident}, max_entry_bytes={max_entry_bytes}: pass {} served {fraction:.3} of the loop, floor {floor:.3}",
                    pass + 1
                );
            }
            assert!(
                served[1] == served[2],
                "C={resident}, max_entry_bytes={max_entry_bytes}: the served set changed between passes 2 and 3 \
                 ({} then {} entries, {} in common)",
                served[1].len(),
                served[2].len(),
                served[1].intersection(&served[2]).count()
            );
        }
    }

    /// ADR-0046 decision 6 with the cache already full of cold data: a hot
    /// working set that arrives later, while a cold scan keeps running,
    /// must take residency from the cold entries and keep it. A policy that
    /// pins whatever filled the cache first serves this working set nothing.
    #[test]
    fn background_cold_scan_does_not_evict_the_hot_working_set() {
        const ENTRY: u64 = 1024;
        const HOT: u64 = 10;
        const SCAN_PER_ROUND: u64 = 20;
        const ROUNDS: u64 = 50;
        let limits = CacheLimits::new(200 * ENTRY, 1_000, 10 * ENTRY);
        let metrics = CacheMetrics::default();
        let mut fifo: S3Fifo<()> = S3Fifo::new(limits);

        let mut cold = 1_000_000u64;
        for _ in 0..1_000 {
            touch(&mut fifo, cold, ENTRY, &metrics);
            cold += 1;
        }

        let mut late_hits = 0u64;
        for round in 0..ROUNDS {
            for n in 0..HOT {
                if touch(&mut fifo, n, ENTRY, &metrics) && round >= ROUNDS - 10 {
                    late_hits += 1;
                }
            }
            for _ in 0..SCAN_PER_ROUND {
                touch(&mut fifo, cold, ENTRY, &metrics);
                cold += 1;
            }
        }

        let resident = (0..HOT).filter(|&n| fifo.contains(&key(n))).count();
        assert_eq!(
            resident, HOT as usize,
            "the hot working set lost residency to the cold scan"
        );
        assert_eq!(
            late_hits,
            10 * HOT,
            "every hot touch in the last ten rounds must be served"
        );
    }
}
