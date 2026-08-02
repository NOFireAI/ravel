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

use std::collections::{HashMap, HashSet, VecDeque};

use crate::key::CacheKey;
use crate::limits::CacheLimits;
use crate::metrics::CacheMetrics;

const MAX_FREQ: u8 = 3;

struct Entry<V> {
    value: V,
    size: u64,
    freq: u8,
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
    ghost_set: HashSet<CacheKey>,
    small_bytes: u64,
    main_bytes: u64,
    small_quota_bytes: u64,
    ghost_capacity: usize,
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
            ghost_set: HashSet::new(),
            small_bytes: 0,
            main_bytes: 0,
            small_quota_bytes,
            ghost_capacity,
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
        entry.freq = entry.freq.saturating_add(1).min(MAX_FREQ);
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

        let promote_to_main = self.ghost_set.remove(&key);
        if promote_to_main && let Some(pos) = self.ghost.iter().position(|k| *k == key) {
            self.ghost.remove(pos);
        }

        self.entries.insert(
            key,
            Entry {
                value,
                size,
                freq: 0,
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

        let evicted = self.evict_to_bounds(metrics);
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
            self.entries.insert(
                key,
                Entry {
                    value: entry.value,
                    size,
                    freq: 0,
                },
            );
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
}
