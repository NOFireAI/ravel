//! S3-FIFO eviction (ADR-0046 decision 6): a small FIFO probation queue, a
//! main FIFO for entries that proved themselves, and a ghost queue of
//! keys evicted from probation before they got a second chance.
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

use bytes::Bytes;

use crate::key::CacheKey;
use crate::limits::CacheLimits;
use crate::metrics::CacheMetrics;

const MAX_FREQ: u8 = 3;

struct Entry {
    value: Bytes,
    freq: u8,
}

pub(crate) struct S3Fifo {
    entries: HashMap<CacheKey, Entry>,
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

impl S3Fifo {
    pub(crate) fn new(limits: CacheLimits) -> Self {
        // 10% probation / 90% main, the split the S3-FIFO paper uses for
        // skewed access patterns; at least 1 byte so a tiny configured
        // cache still has a probation queue to evict from.
        let small_quota_bytes = (limits.max_bytes / 10).max(1);
        S3Fifo {
            entries: HashMap::new(),
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_set: HashSet::new(),
            small_bytes: 0,
            main_bytes: 0,
            small_quota_bytes,
            ghost_capacity: limits.max_entries.max(1),
            limits,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        self.small_bytes + self.main_bytes
    }

    pub(crate) fn get(&mut self, key: &CacheKey) -> Option<Bytes> {
        let entry = self.entries.get_mut(key)?;
        entry.freq = entry.freq.saturating_add(1).min(MAX_FREQ);
        Some(entry.value.clone())
    }

    /// Admit `value` under `key`. Returns `false` without touching any
    /// queue if `value` is larger than `max_entry_bytes`: that is not an
    /// error, the entry is simply not cached.
    pub(crate) fn insert(&mut self, key: CacheKey, value: Bytes, metrics: &CacheMetrics) -> bool {
        let size = value.len() as u64;
        if size > self.limits.max_entry_bytes {
            metrics.record_rejected_size();
            return false;
        }
        if self.entries.contains_key(&key) {
            // Content-addressed: an existing entry for this key is
            // already these exact bytes. Leave its queue position alone.
            return true;
        }

        let promote_to_main = self.ghost_set.remove(&key);
        if promote_to_main && let Some(pos) = self.ghost.iter().position(|k| *k == key) {
            self.ghost.remove(pos);
        }

        self.entries.insert(key, Entry { value, freq: 0 });
        if promote_to_main {
            self.main.push_back(key);
            self.main_bytes += size;
        } else {
            self.small.push_back(key);
            self.small_bytes += size;
        }
        metrics.record_admission(size);

        self.evict_to_bounds(metrics);
        true
    }

    fn evict_to_bounds(&mut self, metrics: &CacheMetrics) {
        while self.total_bytes() > self.limits.max_bytes
            || self.entries.len() > self.limits.max_entries
        {
            let evicted = if self.small_bytes > self.small_quota_bytes && !self.small.is_empty() {
                self.evict_from_small(metrics)
            } else if !self.main.is_empty() {
                self.evict_from_main(metrics)
            } else if !self.small.is_empty() {
                self.evict_from_small(metrics)
            } else {
                false
            };
            if !evicted {
                break;
            }
        }
    }

    /// Pop the front of the small queue. Promotes it to main if it was
    /// touched again after admission (`freq > 0`); otherwise evicts it and
    /// remembers the key in the ghost queue. Returns whether anything was
    /// in the small queue to process at all.
    fn evict_from_small(&mut self, metrics: &CacheMetrics) -> bool {
        let Some(key) = self.small.pop_front() else {
            return false;
        };
        let Some(entry) = self.entries.remove(&key) else {
            return true;
        };
        let size = entry.value.len() as u64;
        self.small_bytes -= size;
        if entry.freq > 0 {
            self.entries.insert(
                key,
                Entry {
                    value: entry.value,
                    freq: 0,
                },
            );
            self.main.push_back(key);
            self.main_bytes += size;
        } else {
            metrics.record_eviction();
            self.ghost.push_back(key);
            self.ghost_set.insert(key);
            while self.ghost.len() > self.ghost_capacity {
                if let Some(oldest) = self.ghost.pop_front() {
                    self.ghost_set.remove(&oldest);
                }
            }
        }
        true
    }

    /// CLOCK sweep over the main queue: an entry with `freq > 0` gets one
    /// more lap with `freq` decremented; the first with `freq == 0` is
    /// evicted permanently (no ghost entry; ghost exists to give
    /// probation entries a fair second chance, not to remember main
    /// evictions). Returns whether anything was evicted.
    fn evict_from_main(&mut self, metrics: &CacheMetrics) -> bool {
        loop {
            let Some(key) = self.main.pop_front() else {
                return false;
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
            self.main_bytes -= entry.value.len() as u64;
            metrics.record_eviction();
            return true;
        }
    }
}
