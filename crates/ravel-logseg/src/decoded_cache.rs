//! A process-wide cache of decoded columns, keyed per (tenant, object, block,
//! column) (ADR-3326). Stage 0 measurement prototype.
//!
//! An entry is one column of one block as [`decode_columns`] produced it,
//! shared by `Arc`: a hit clones the handle, never the data. Only a whole-block
//! decode on the columnar scan path populates it, and never with a column a
//! Contains arm defers (its dictionary is narrowed to survivors after decode).
//!
//! Admission follows ADR-3326 decision 3: an interactive statement admits on
//! first decode, any other intent on a second touch through a ghost key list
//! whose window spans the cap in bytes. Eviction is CLOCK weighted by bytes.
//! Each entry holds a best-effort reservation on the process
//! [`MemoryBudget`]; an entry that cannot reserve is not admitted.
//!
//! [`decode_columns`]: crate::block

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use ravel_memory::{MemoryBudget, Reservation};
use ravel_types::accounting::AccessIntent;

use crate::block::CachedColumn;

/// The decoded layout entries are stored in. Bumped when the decoder's output
/// shape for a column kind changes, so an entry of the old shape never hits.
pub const LAYOUT_TAG: u8 = 0;

/// One entry's identity (ADR-3326 decision 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DecodedKey {
    pub tenant: [u8; 16],
    pub object: [u8; 32],
    pub block: u32,
    pub column: u32,
    pub layout: u8,
}

/// Per-statement counters, shared by every scan of one statement.
#[derive(Debug, Default)]
pub struct DecodedCacheCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    admissions: AtomicU64,
    evictions: AtomicU64,
    ghost_hits: AtomicU64,
}

/// A point-in-time copy of [`DecodedCacheCounters`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecodedCacheSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub admissions: u64,
    pub evictions: u64,
    pub ghost_hits: u64,
}

impl DecodedCacheSnapshot {
    /// Field-wise saturating sum, for totalling several handles' counters.
    #[must_use]
    pub fn saturating_add(&self, other: &DecodedCacheSnapshot) -> DecodedCacheSnapshot {
        DecodedCacheSnapshot {
            hits: self.hits.saturating_add(other.hits),
            misses: self.misses.saturating_add(other.misses),
            admissions: self.admissions.saturating_add(other.admissions),
            evictions: self.evictions.saturating_add(other.evictions),
            ghost_hits: self.ghost_hits.saturating_add(other.ghost_hits),
        }
    }
}

impl DecodedCacheCounters {
    pub fn snapshot(&self) -> DecodedCacheSnapshot {
        DecodedCacheSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            admissions: self.admissions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            ghost_hits: self.ghost_hits.load(Ordering::Relaxed),
        }
    }

    fn bump(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

struct Slot {
    key: DecodedKey,
    col: CachedColumn,
    bytes: u64,
    referenced: bool,
    _reservation: Option<Reservation>,
}

#[derive(Default)]
struct Inner {
    index: HashMap<DecodedKey, usize>,
    slots: Vec<Option<Slot>>,
    free: Vec<usize>,
    hand: usize,
    resident: u64,
    /// Ghost keys of first touches by a non-interactive statement, each with
    /// the generation it was recorded at so a FIFO entry superseded by a later
    /// record of the same key does not remove that later record.
    ghost: HashMap<DecodedKey, (u64, u64)>,
    ghost_fifo: VecDeque<(DecodedKey, u64)>,
    ghost_bytes: u64,
    ghost_gen: u64,
    column_hits: HashMap<u32, u64>,
    process: DecodedCacheSnapshot,
}

/// The process-wide decoded-column cache.
pub struct DecodedColumnCache {
    max_bytes: u64,
    budget: Option<Arc<MemoryBudget>>,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for DecodedColumnCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedColumnCache")
            .field("max_bytes", &self.max_bytes)
            .field("resident_bytes", &self.resident_bytes())
            .finish()
    }
}

impl DecodedColumnCache {
    /// A cache holding at most `max_bytes` of decoded columns, each entry
    /// reserved best effort on `budget` when one is given.
    pub fn new(max_bytes: u64, budget: Option<Arc<MemoryBudget>>) -> Self {
        DecodedColumnCache {
            max_bytes,
            budget,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Bytes of the entries the index holds.
    pub fn resident_bytes(&self) -> u64 {
        self.lock().resident
    }

    /// Entries the index holds.
    pub fn len(&self) -> usize {
        self.lock().index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Process-wide counters since construction.
    pub fn process_counters(&self) -> DecodedCacheSnapshot {
        self.lock().process
    }

    /// Hits served for column id `column`, over every object and tenant.
    pub fn column_hits(&self, column: u32) -> u64 {
        self.lock().column_hits.get(&column).copied().unwrap_or(0)
    }

    /// The bytes the entry for `key` is charged, without touching it.
    #[cfg(test)]
    pub(crate) fn entry_bytes(&self, key: &DecodedKey) -> Option<u64> {
        let inner = self.lock();
        let i = *inner.index.get(key)?;
        inner.slots.get(i)?.as_ref().map(|s| s.bytes)
    }

    /// The entry for `key`, marking it referenced for CLOCK.
    pub(crate) fn lookup(
        &self,
        key: &DecodedKey,
        counters: &DecodedCacheCounters,
    ) -> Option<CachedColumn> {
        let mut inner = self.lock();
        let found = inner.index.get(key).copied().and_then(|i| {
            inner.slots.get_mut(i).and_then(Option::as_mut).map(|s| {
                s.referenced = true;
                s.col.clone()
            })
        });
        if found.is_some() {
            inner.process.hits += 1;
            *inner.column_hits.entry(key.column).or_insert(0) += 1;
            DecodedCacheCounters::bump(&counters.hits, 1);
        } else {
            inner.process.misses += 1;
            DecodedCacheCounters::bump(&counters.misses, 1);
        }
        found
    }

    /// Offers a freshly decoded column for admission under `intent`'s rule.
    pub(crate) fn offer(
        &self,
        key: DecodedKey,
        col: &CachedColumn,
        intent: AccessIntent,
        counters: &DecodedCacheCounters,
    ) {
        let bytes = col.heap_bytes() as u64;
        if bytes > self.max_bytes {
            return;
        }
        let mut inner = self.lock();
        if inner.index.contains_key(&key) {
            return;
        }
        if intent != AccessIntent::Interactive {
            match inner.ghost.remove(&key) {
                Some((_, ghost_bytes)) => {
                    inner.ghost_bytes = inner.ghost_bytes.saturating_sub(ghost_bytes);
                    inner.process.ghost_hits += 1;
                    DecodedCacheCounters::bump(&counters.ghost_hits, 1);
                }
                None => {
                    self.record_ghost(&mut inner, key, bytes);
                    return;
                }
            }
        }
        let reservation = match &self.budget {
            Some(budget) => match budget.reserve_best_effort(bytes) {
                Ok(r) => Some(r),
                Err(_) => return,
            },
            None => None,
        };
        let evicted = Self::evict_until(&mut inner, self.max_bytes.saturating_sub(bytes));
        inner.process.evictions += evicted;
        DecodedCacheCounters::bump(&counters.evictions, evicted);
        let slot = Slot {
            key,
            col: col.clone(),
            bytes,
            referenced: false,
            _reservation: reservation,
        };
        let idx = match inner.free.pop() {
            Some(i) => {
                inner.slots[i] = Some(slot);
                i
            }
            None => {
                inner.slots.push(Some(slot));
                inner.slots.len() - 1
            }
        };
        inner.index.insert(key, idx);
        inner.resident += bytes;
        inner.process.admissions += 1;
        DecodedCacheCounters::bump(&counters.admissions, 1);
    }

    fn record_ghost(&self, inner: &mut Inner, key: DecodedKey, bytes: u64) {
        if inner.ghost.contains_key(&key) {
            return;
        }
        inner.ghost_gen += 1;
        let generation = inner.ghost_gen;
        inner.ghost.insert(key, (generation, bytes));
        inner.ghost_fifo.push_back((key, generation));
        inner.ghost_bytes += bytes;
        while inner.ghost_bytes > self.max_bytes {
            let Some((old, g)) = inner.ghost_fifo.pop_front() else {
                break;
            };
            if inner.ghost.get(&old).is_some_and(|(cur, _)| *cur == g)
                && let Some((_, b)) = inner.ghost.remove(&old)
            {
                inner.ghost_bytes = inner.ghost_bytes.saturating_sub(b);
            }
        }
    }

    /// CLOCK: sweep the hand, clearing reference bits and evicting entries
    /// whose bit is already clear, until resident bytes are at most `target`.
    /// Returns the number of entries evicted.
    fn evict_until(inner: &mut Inner, target: u64) -> u64 {
        let mut evicted = 0;
        while inner.resident > target && !inner.index.is_empty() {
            let n = inner.slots.len();
            if n == 0 {
                break;
            }
            let i = inner.hand % n;
            inner.hand = (i + 1) % n;
            let Some(slot) = inner.slots[i].as_mut() else {
                continue;
            };
            if slot.referenced {
                slot.referenced = false;
                continue;
            }
            if let Some(slot) = inner.slots[i].take() {
                inner.index.remove(&slot.key);
                inner.resident = inner.resident.saturating_sub(slot.bytes);
                inner.free.push(i);
                evicted += 1;
            }
        }
        evicted
    }
}

/// What a scan needs to read and fill the cache for one object.
#[derive(Clone, Debug)]
pub struct DecodedCacheAttach {
    pub cache: Arc<DecodedColumnCache>,
    pub tenant: [u8; 16],
    pub object: [u8; 32],
    pub intent: AccessIntent,
    pub counters: Arc<DecodedCacheCounters>,
}

impl DecodedCacheAttach {
    pub(crate) fn key(&self, block: u32, column: u32) -> DecodedKey {
        DecodedKey {
            tenant: self.tenant,
            object: self.object,
            block,
            column,
            layout: LAYOUT_TAG,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::block::I64Column;

    fn key(block: u32, column: u32) -> DecodedKey {
        DecodedKey {
            tenant: [1; 16],
            object: [2; 32],
            block,
            column,
            layout: LAYOUT_TAG,
        }
    }

    fn col(rows: usize) -> CachedColumn {
        CachedColumn::I64(Arc::new(I64Column::from_options(&vec![Some(7); rows])))
    }

    #[test]
    fn interactive_admits_on_first_decode_and_hit_shares_the_arc() {
        let cache = DecodedColumnCache::new(1 << 20, None);
        let c = DecodedCacheCounters::default();
        let v = col(8);
        cache.offer(key(0, 1), &v, AccessIntent::Interactive, &c);
        let hit = cache.lookup(&key(0, 1), &c).expect("hit");
        assert!(hit.ptr_eq(&v));
        assert!(cache.lookup(&key(1, 1), &c).is_none());
        let s = c.snapshot();
        assert_eq!((s.hits, s.misses, s.admissions), (1, 1, 1));
        assert_eq!(cache.resident_bytes(), v.heap_bytes() as u64);
        assert_eq!(cache.column_hits(1), 1);
    }

    #[test]
    fn bulk_scan_admits_on_second_touch_through_the_ghost() {
        let cache = DecodedColumnCache::new(1 << 20, None);
        let c = DecodedCacheCounters::default();
        let v = col(8);
        cache.offer(key(0, 1), &v, AccessIntent::BulkScan, &c);
        assert!(cache.is_empty());
        cache.offer(key(0, 1), &v, AccessIntent::BulkScan, &c);
        assert_eq!(cache.len(), 1);
        let s = c.snapshot();
        assert_eq!((s.ghost_hits, s.admissions), (1, 1));
    }

    #[test]
    fn ghost_window_spans_the_cap_in_bytes() {
        let v = col(8);
        let bytes = v.heap_bytes() as u64;
        let cache = DecodedColumnCache::new(bytes * 2, None);
        let c = DecodedCacheCounters::default();
        for b in 0..3 {
            cache.offer(key(b, 1), &v, AccessIntent::BulkScan, &c);
        }
        // Block 0's ghost fell out of a window two entries wide.
        cache.offer(key(0, 1), &v, AccessIntent::BulkScan, &c);
        assert!(cache.is_empty());
        cache.offer(key(2, 1), &v, AccessIntent::BulkScan, &c);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn clock_evicts_unreferenced_first_and_respects_the_cap() {
        let v = col(8);
        let bytes = v.heap_bytes() as u64;
        let cache = DecodedColumnCache::new(bytes * 2, None);
        let c = DecodedCacheCounters::default();
        cache.offer(key(0, 1), &v, AccessIntent::Interactive, &c);
        cache.offer(key(1, 1), &v, AccessIntent::Interactive, &c);
        assert!(cache.lookup(&key(0, 1), &c).is_some());
        cache.offer(key(2, 1), &v, AccessIntent::Interactive, &c);
        assert!(cache.resident_bytes() <= bytes * 2);
        assert!(cache.lookup(&key(0, 1), &c).is_some());
        assert!(cache.lookup(&key(1, 1), &c).is_none());
        assert_eq!(c.snapshot().evictions, 1);
    }

    #[test]
    fn an_entry_that_cannot_reserve_is_not_admitted() {
        let v = col(8);
        let budget = Arc::new(MemoryBudget::new(1));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        cache.offer(key(0, 1), &v, AccessIntent::Interactive, &c);
        assert!(cache.is_empty());
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn an_admitted_entry_holds_its_reservation_until_evicted() {
        let v = col(8);
        let bytes = v.heap_bytes() as u64;
        let budget = Arc::new(MemoryBudget::new(1 << 30));
        let cache = DecodedColumnCache::new(bytes, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        cache.offer(key(0, 1), &v, AccessIntent::Interactive, &c);
        assert_eq!(budget.reserved(), bytes);
        cache.offer(key(1, 1), &v, AccessIntent::Interactive, &c);
        assert_eq!(budget.reserved(), bytes);
        assert_eq!(cache.len(), 1);
    }
}
