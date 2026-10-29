//! A process-wide cache of decoded columns, keyed per (tenant, object, block,
//! column, layout) (ADR-3326).
//!
//! An entry is one column of one block as [`decode_columns`] produced it,
//! shared by `Arc`: a hit clones handles, never data. Only a whole-block
//! decode on the columnar scan path reads or fills it, and never with a
//! column a Contains arm defers (its dictionary is narrowed to survivors
//! after decode). The row path neither reads nor fills it.
//!
//! A hit skips the whole-block CRC cross-check for that block. Per-page CRCs
//! still cover every byte the scan reads, and a cached column was verified
//! when it was first decoded.
//!
//! Admission follows ADR-3326 decision 3: an interactive statement admits on
//! first decode, any other intent on a second touch through a ghost key list
//! whose window spans the cap in bytes. Eviction is CLOCK weighted by bytes.
//!
//! Each entry holds a reservation on the process [`MemoryBudget`], taken
//! best effort and only when it leaves [`admission_margin`] unreserved
//! (decision 4). The reservation lives in the [`DecodedEntry`] itself, not in
//! the index slot, so it is released when the last handle to the entry
//! drops: an entry a scan still holds keeps its bytes charged after the index
//! has let it go. [`DecodedColumnCache::evict_for_pressure`] is the budget's
//! evictor and frees only entries no scan holds.
//!
//! Entries age out on the ravel-cache bound (ADR-0064): an entry older than
//! [`DEFAULT_MAX_ENTRY_AGE_NS`] is dropped at its first access, and
//! [`DecodedColumnCache::sweep_expired`], run every
//! [`DEFAULT_SWEEP_INTERVAL_NS`] by the server, drops the ones nobody reads.
//!
//! [`decode_columns`]: crate::block
//! [`DEFAULT_SWEEP_INTERVAL_NS`]: ravel_cache::DEFAULT_SWEEP_INTERVAL_NS

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use ravel_cache::{Clock, DEFAULT_MAX_ENTRY_AGE_NS, SystemClock};
use ravel_memory::{ExhaustionCause, MemoryBudget, Reservation};
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

/// One cached column and the budget reservation for its bytes. Dropping the
/// last `Arc` to it releases the reservation.
pub struct DecodedEntry {
    col: CachedColumn,
    bytes: u64,
    inserted_ns: u64,
    _reservation: Option<Reservation>,
}

impl DecodedEntry {
    pub(crate) fn column(&self) -> &CachedColumn {
        &self.col
    }

    /// The bytes the entry is charged.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl std::fmt::Debug for DecodedEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedEntry")
            .field("bytes", &self.bytes)
            .field("inserted_ns", &self.inserted_ns)
            .finish()
    }
}

/// Per-statement counters, shared by every scan of one statement.
#[derive(Debug, Default)]
pub struct DecodedCacheCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    admissions: AtomicU64,
    evictions: AtomicU64,
    ghost_hits: AtomicU64,
    margin_skips: AtomicU64,
}

/// A point-in-time copy of [`DecodedCacheCounters`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecodedCacheSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub admissions: u64,
    pub evictions: u64,
    pub ghost_hits: u64,
    /// Admissions skipped because the reservation would have left the budget
    /// under [`admission_margin`], though its limit would have admitted it.
    pub margin_skips: u64,
}

/// The unreserved bytes an admission must leave on a budget of `limit`, and
/// the slack the pressure evictor frees beyond a refused caller's shortfall
/// (ADR-3326 decision 4): `max(1 GiB, 5% of limit)`.
#[must_use]
pub fn admission_margin(limit: u64) -> u64 {
    (limit / 20).max(1 << 30)
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
            margin_skips: self.margin_skips.saturating_add(other.margin_skips),
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
            margin_skips: self.margin_skips.load(Ordering::Relaxed),
        }
    }

    fn bump(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

struct Slot {
    key: DecodedKey,
    entry: Arc<DecodedEntry>,
    referenced: bool,
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
    expired_drops: u64,
}

impl Inner {
    /// Takes slot `i` out of the index. The entry is returned rather than
    /// dropped so the caller can drop it after releasing the lock.
    fn remove(&mut self, i: usize) -> Option<Arc<DecodedEntry>> {
        let slot = self.slots.get_mut(i)?.take()?;
        self.index.remove(&slot.key);
        self.resident = self.resident.saturating_sub(slot.entry.bytes);
        self.free.push(i);
        Some(slot.entry)
    }
}

/// Process-wide totals of [`DecodedColumnCache::evict_for_pressure`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PressureSnapshot {
    pub evictions: u64,
    pub freed_bytes: u64,
}

impl PressureSnapshot {
    /// The figures moved since `mark`, saturating.
    #[must_use]
    pub fn since(&self, mark: &PressureSnapshot) -> PressureSnapshot {
        PressureSnapshot {
            evictions: self.evictions.saturating_sub(mark.evictions),
            freed_bytes: self.freed_bytes.saturating_sub(mark.freed_bytes),
        }
    }
}

/// The process-wide decoded-column cache.
pub struct DecodedColumnCache {
    max_bytes: u64,
    max_entry_age_ns: u64,
    budget: Option<Arc<MemoryBudget>>,
    clock: Arc<dyn Clock>,
    inner: Mutex<Inner>,
    pressure_evictions: AtomicU64,
    pressure_freed_bytes: AtomicU64,
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
    /// reserved on `budget` when one is given, aged on the system clock.
    pub fn new(max_bytes: u64, budget: Option<Arc<MemoryBudget>>) -> Self {
        Self::with_clock(max_bytes, budget, Arc::new(SystemClock))
    }

    /// [`Self::new`] on an injected clock.
    pub fn with_clock(
        max_bytes: u64,
        budget: Option<Arc<MemoryBudget>>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        DecodedColumnCache {
            max_bytes,
            max_entry_age_ns: DEFAULT_MAX_ENTRY_AGE_NS,
            budget,
            clock,
            inner: Mutex::new(Inner::default()),
            pressure_evictions: AtomicU64::new(0),
            pressure_freed_bytes: AtomicU64::new(0),
        }
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn expired(&self, entry: &DecodedEntry, now: u64) -> bool {
        now.saturating_sub(entry.inserted_ns) > self.max_entry_age_ns
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

    /// Entries dropped for age since construction, at access or by a sweep.
    pub fn expired_drops(&self) -> u64 {
        self.lock().expired_drops
    }

    /// Pressure evictions since construction.
    pub fn pressure_counters(&self) -> PressureSnapshot {
        PressureSnapshot {
            evictions: self.pressure_evictions.load(Ordering::Relaxed),
            freed_bytes: self.pressure_freed_bytes.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn entry_bytes(&self, key: &DecodedKey) -> Option<u64> {
        let inner = self.lock();
        let i = *inner.index.get(key)?;
        inner.slots.get(i)?.as_ref().map(|s| s.entry.bytes)
    }

    /// Hits served for column id `column`, over every object and tenant.
    pub fn column_hits(&self, column: u32) -> u64 {
        self.lock().column_hits.get(&column).copied().unwrap_or(0)
    }

    /// Drops every entry older than the age bound and returns how many it
    /// dropped. The server runs it every `DEFAULT_SWEEP_INTERVAL_NS`.
    pub fn sweep_expired(&self) -> usize {
        let now = self.clock.now_ns();
        let mut dropped = Vec::new();
        {
            let mut inner = self.lock();
            for i in 0..inner.slots.len() {
                let old = inner.slots[i]
                    .as_ref()
                    .is_some_and(|s| self.expired(&s.entry, now));
                if old && let Some(entry) = inner.remove(i) {
                    dropped.push(entry);
                }
            }
            inner.expired_drops += dropped.len() as u64;
        }
        dropped.len()
    }

    /// The budget's pressure evictor (ADR-3326 decision 4): evicts, in CLOCK
    /// order, entries no scan holds until `want` bytes are freed or none
    /// remains, and returns the bytes freed. An entry a scan still holds
    /// stays, and its bytes count for nothing: its reservation would not be
    /// released by removing it from the index. Each pass logs one INFO line;
    /// `slack` is the part of `want` the budget asks beyond the refused
    /// caller's shortfall (logged only).
    pub fn evict_for_pressure(&self, want: u64, slack: u64, cause: ExhaustionCause) -> u64 {
        let mut evicted = Vec::new();
        let mut freed = 0u64;
        let mut held = 0u64;
        let resident_after;
        {
            let mut inner = self.lock();
            let n = inner.slots.len();
            // Two sweeps: the first may only clear reference bits. Held
            // entries are counted on the first, so none counts twice.
            for step in 0..n.saturating_mul(2) {
                if freed >= want {
                    break;
                }
                let i = inner.hand % n;
                inner.hand = (i + 1) % n;
                let Some(slot) = inner.slots[i].as_mut() else {
                    continue;
                };
                if Arc::strong_count(&slot.entry) > 1 {
                    if step < n {
                        held = held.saturating_add(slot.entry.bytes);
                    }
                    continue;
                }
                if slot.referenced {
                    slot.referenced = false;
                    continue;
                }
                if let Some(entry) = inner.remove(i) {
                    freed = freed.saturating_add(entry.bytes);
                    evicted.push(entry);
                }
            }
            resident_after = inner.resident;
        }
        tracing::info!(
            target: "ravel_logseg::decoded_cache",
            asked_bytes = want,
            slack_bytes = slack,
            freed_bytes = freed,
            resident_bytes_after = resident_after,
            held_bytes = held,
            cause = cause.name(),
            "decoded cache pressure eviction"
        );
        self.pressure_evictions
            .fetch_add(evicted.len() as u64, Ordering::Relaxed);
        self.pressure_freed_bytes
            .fetch_add(freed, Ordering::Relaxed);
        // The entries were the index's last handles: dropping them here,
        // outside the lock, releases their reservations.
        drop(evicted);
        freed
    }

    /// The entry for `key`, marking it referenced for CLOCK. `rows` is the
    /// block's record count; an entry of another length is not served. An
    /// entry past the age bound is dropped and reads as a miss.
    pub(crate) fn lookup(
        &self,
        key: &DecodedKey,
        rows: usize,
        counters: &DecodedCacheCounters,
    ) -> Option<Arc<DecodedEntry>> {
        let now = self.clock.now_ns();
        let mut expired = None;
        let found = {
            let mut inner = self.lock();
            let mut found = None;
            if let Some(i) = inner.index.get(key).copied() {
                let old = inner.slots[i]
                    .as_ref()
                    .is_some_and(|s| self.expired(&s.entry, now));
                if old {
                    expired = inner.remove(i);
                    inner.expired_drops += 1;
                } else if let Some(s) = inner.slots[i].as_mut()
                    && s.entry.col.rows() == rows
                {
                    s.referenced = true;
                    found = Some(Arc::clone(&s.entry));
                }
            }
            if found.is_some() {
                inner.process.hits += 1;
                *inner.column_hits.entry(key.column).or_insert(0) += 1;
                DecodedCacheCounters::bump(&counters.hits, 1);
            } else {
                inner.process.misses += 1;
                DecodedCacheCounters::bump(&counters.misses, 1);
            }
            found
        };
        drop(expired);
        found
    }

    /// Offers a freshly decoded column for admission under `intent`'s rule,
    /// returning the admitted entry so the caller can hold it for as long as
    /// it uses the column.
    pub(crate) fn offer(
        &self,
        key: DecodedKey,
        col: &CachedColumn,
        intent: AccessIntent,
        counters: &DecodedCacheCounters,
    ) -> Option<Arc<DecodedEntry>> {
        let bytes = col.heap_bytes() as u64;
        if bytes > self.max_bytes {
            return None;
        }
        let mut evicted = Vec::new();
        {
            let mut inner = self.lock();
            if inner.index.contains_key(&key) {
                return None;
            }
            if intent != AccessIntent::Interactive && !inner.ghost.contains_key(&key) {
                self.record_ghost(&mut inner, key, bytes);
                return None;
            }
            self.evict_for(&mut inner, bytes, &mut evicted, counters);
        }
        // Evicted entries release their reservations here, before the new
        // entry reserves, so the cache at its cap can swap one entry for
        // another instead of counting every resident byte against the margin.
        // If the reservation below is still refused, the eviction stands and
        // nothing is admitted.
        drop(evicted);

        let reservation = match &self.budget {
            Some(budget) => {
                match budget.reserve_best_effort_keeping(bytes, admission_margin(budget.limit())) {
                    Ok(r) => Some(r),
                    Err(refusal) => {
                        if refusal.cause == ExhaustionCause::Accounted
                            && refusal
                                .reserved
                                .checked_add(bytes)
                                .is_some_and(|total| total <= refusal.limit)
                        {
                            let mut inner = self.lock();
                            inner.process.margin_skips += 1;
                            DecodedCacheCounters::bump(&counters.margin_skips, 1);
                        }
                        return None;
                    }
                }
            }
            None => None,
        };

        let mut evicted = Vec::new();
        let admitted = {
            let mut inner = self.lock();
            if inner.index.contains_key(&key) {
                // Admitted concurrently; `reservation` drops with this frame.
                return None;
            }
            // The reservation succeeded: the touch is spent either way now,
            // so the ghost record (if the FIFO hasn't already dropped it) is
            // released and the hit counted here, not when the ghost was
            // merely found.
            if intent != AccessIntent::Interactive {
                if let Some((_, ghost_bytes)) = inner.ghost.remove(&key) {
                    inner.ghost_bytes = inner.ghost_bytes.saturating_sub(ghost_bytes);
                }
                inner.process.ghost_hits += 1;
                DecodedCacheCounters::bump(&counters.ghost_hits, 1);
            }
            // Entries admitted between the two locks may have refilled the cap.
            self.evict_for(&mut inner, bytes, &mut evicted, counters);
            let entry = Arc::new(DecodedEntry {
                col: col.clone(),
                bytes,
                inserted_ns: self.clock.now_ns(),
                _reservation: reservation,
            });
            let slot = Slot {
                key,
                entry: Arc::clone(&entry),
                referenced: false,
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
            entry
        };
        drop(evicted);
        Some(admitted)
    }

    /// Evicts until `bytes` more fit under the cap and counts the evictions.
    fn evict_for(
        &self,
        inner: &mut Inner,
        bytes: u64,
        evicted: &mut Vec<Arc<DecodedEntry>>,
        counters: &DecodedCacheCounters,
    ) {
        let before = evicted.len();
        Self::evict_until(inner, self.max_bytes.saturating_sub(bytes), evicted);
        let n = (evicted.len() - before) as u64;
        inner.process.evictions += n;
        DecodedCacheCounters::bump(&counters.evictions, n);
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
    /// Evicted entries go to `evicted` for the caller to drop outside the
    /// lock; one a scan still holds keeps its reservation until that scan
    /// lets go.
    fn evict_until(inner: &mut Inner, target: u64, evicted: &mut Vec<Arc<DecodedEntry>>) {
        while inner.resident > target && !inner.index.is_empty() {
            let n = inner.slots.len();
            let i = inner.hand % n;
            inner.hand = (i + 1) % n;
            let Some(slot) = inner.slots[i].as_mut() else {
                continue;
            };
            if slot.referenced {
                slot.referenced = false;
                continue;
            }
            if let Some(entry) = inner.remove(i) {
                evicted.push(entry);
            }
        }
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
    use ravel_cache::DEFAULT_SWEEP_INTERVAL_NS;

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
        CachedColumn::I64(I64Column::from_options(&vec![Some(7); rows]))
    }

    /// A clock a test advances by hand.
    #[derive(Default)]
    struct TestClock(AtomicU64);

    impl Clock for TestClock {
        fn now_ns(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    impl TestClock {
        fn set(&self, ns: u64) {
            self.0.store(ns, Ordering::Relaxed);
        }
    }

    #[test]
    fn interactive_admits_on_first_decode_and_hit_shares_the_buffers() {
        let cache = DecodedColumnCache::new(1 << 20, None);
        let c = DecodedCacheCounters::default();
        let v = col(8);
        cache.offer(key(0, 1), &v, AccessIntent::Interactive, &c);
        let hit = cache.lookup(&key(0, 1), 8, &c).expect("hit");
        assert!(hit.column().shares_buffer_with(&v));
        assert!(cache.lookup(&key(1, 1), 8, &c).is_none());
        let s = c.snapshot();
        assert_eq!((s.hits, s.misses, s.admissions), (1, 1, 1));
        assert_eq!(cache.resident_bytes(), v.heap_bytes() as u64);
        assert_eq!(cache.column_hits(1), 1);
    }

    #[test]
    fn an_entry_of_another_row_count_is_not_served() {
        let cache = DecodedColumnCache::new(1 << 20, None);
        let c = DecodedCacheCounters::default();
        cache.offer(key(0, 1), &col(8), AccessIntent::Interactive, &c);
        assert!(cache.lookup(&key(0, 1), 9, &c).is_none());
        let s = c.snapshot();
        assert_eq!((s.hits, s.misses), (0, 1));
    }

    #[test]
    fn bulk_scan_admits_on_second_touch_through_the_ghost() {
        let cache = DecodedColumnCache::new(1 << 20, None);
        let c = DecodedCacheCounters::default();
        let v = col(8);
        assert!(
            cache
                .offer(key(0, 1), &v, AccessIntent::BulkScan, &c)
                .is_none()
        );
        assert!(cache.is_empty());
        assert!(
            cache
                .offer(key(0, 1), &v, AccessIntent::BulkScan, &c)
                .is_some()
        );
        assert_eq!(cache.len(), 1);
        let s = c.snapshot();
        assert_eq!((s.ghost_hits, s.admissions), (1, 1));
    }

    /// A refused reservation must not spend the ghost touch: the record
    /// stays and the hit is not counted, so a later touch of the same key
    /// still admits on it rather than needing a fresh pair.
    ///
    /// FLIP: bump `ghost_hits` and remove the ghost record as soon as the key
    /// is found, before the reservation is attempted, and the refused second
    /// touch spends the credit: the third touch records a fresh ghost entry
    /// instead of admitting.
    #[test]
    fn a_refused_reservation_keeps_the_ghost_record_and_its_hit_uncounted() {
        const LIMIT: u64 = 4 << 30;
        let margin = admission_margin(LIMIT);
        let v = col(8);
        let bytes = v.heap_bytes() as u64;
        let budget = Arc::new(MemoryBudget::new(LIMIT));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();

        // First touch: records the ghost key, admits nothing.
        assert!(
            cache
                .offer(key(0, 1), &v, AccessIntent::BulkScan, &c)
                .is_none()
        );
        assert_eq!(c.snapshot().ghost_hits, 0);

        // Second touch: the reservation is refused, one byte under the
        // margin, the way `admission_is_skipped_exactly_below_the_margin`
        // drives a margin skip.
        let filler = LIMIT - (margin + bytes - 1);
        budget.reserve_unchecked(filler);
        assert!(
            cache
                .offer(key(0, 1), &v, AccessIntent::BulkScan, &c)
                .is_none()
        );
        let s = c.snapshot();
        assert_eq!((s.admissions, s.ghost_hits), (0, 0));

        // Free the room back up: the next touch of the same key is admitted
        // on the ghost record the first touch left, not a fresh one.
        budget.release(filler);
        assert!(
            cache
                .offer(key(0, 1), &v, AccessIntent::BulkScan, &c)
                .is_some()
        );
        let s = c.snapshot();
        assert_eq!((s.admissions, s.ghost_hits), (1, 1));
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
        assert!(cache.lookup(&key(0, 1), 8, &c).is_some());
        cache.offer(key(2, 1), &v, AccessIntent::Interactive, &c);
        assert!(cache.resident_bytes() <= bytes * 2);
        assert!(cache.lookup(&key(0, 1), 8, &c).is_some());
        assert!(cache.lookup(&key(1, 1), 8, &c).is_none());
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

    /// The reservation travels with the bytes (ADR-3326 decision 4): an entry
    /// a scan holds past its eviction from the index keeps its bytes reserved
    /// until the scan's handle, the last one, drops.
    ///
    /// FLIP: move the reservation from `DecodedEntry` into `Slot` and the
    /// budget reads `bytes` short right after the eviction.
    #[test]
    fn the_reservation_is_released_by_the_last_handle_not_by_eviction() {
        let v = col(8);
        let bytes = v.heap_bytes() as u64;
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::new(bytes, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        let held = cache
            .offer(key(0, 1), &v, AccessIntent::Interactive, &c)
            .expect("admitted");
        drop(v);
        assert_eq!(budget.reserved(), bytes);

        // Admitting a second entry evicts the first from the index.
        let second = cache
            .offer(key(1, 1), &col(8), AccessIntent::Interactive, &c)
            .expect("admitted");
        drop(second);
        assert_eq!(c.snapshot().evictions, 1);
        assert!(cache.lookup(&key(0, 1), 8, &c).is_none(), "evicted");
        assert_eq!(cache.resident_bytes(), bytes);
        assert_eq!(
            budget.reserved(),
            2 * bytes,
            "the evicted entry is still held, so its bytes stay reserved"
        );

        drop(held);
        assert_eq!(budget.reserved(), bytes, "the last handle released it");
    }

    /// The pressure evictor skips an entry a scan still holds and counts only
    /// the bytes of the entry it held the last handle to, whose reservation it
    /// released. Asked for more than every unheld entry holds, it returns
    /// what it freed rather than the request.
    ///
    /// FLIP: drop the `strong_count > 1` skip in `evict_for_pressure` and
    /// the held entry is evicted, `freed` reads `2 * bytes`.
    #[test]
    fn pressure_eviction_never_evicts_or_counts_an_entry_a_scan_holds() {
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        let held = cache
            .offer(key(0, 1), &col(8), AccessIntent::Interactive, &c)
            .expect("admitted");
        let bytes = held.bytes();
        drop(cache.offer(key(1, 1), &col(8), AccessIntent::Interactive, &c));
        assert_eq!(budget.reserved(), 2 * bytes);

        let freed = cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted);
        assert_eq!(freed, bytes, "only the unheld entry frees");
        assert_eq!(budget.reserved(), bytes, "its reservation is released");
        assert_eq!(cache.len(), 1, "held stays");
        assert!(cache.lookup(&key(1, 1), 8, &c).is_none());
        assert_eq!(
            cache.pressure_counters(),
            PressureSnapshot {
                evictions: 1,
                freed_bytes: bytes,
            }
        );

        drop(held);
        assert_eq!(
            cache.evict_for_pressure(1, 0, ExhaustionCause::Accounted),
            bytes,
            "unheld now"
        );
        assert!(cache.is_empty());
        assert_eq!(budget.reserved(), 0);
        assert_eq!(
            cache.evict_for_pressure(1, 0, ExhaustionCause::Accounted),
            0,
            "nothing left"
        );
        assert_eq!(cache.pressure_counters().evictions, 2);
    }

    /// An entry is served up to the age bound and dropped at the first access
    /// past it, releasing its reservation; a later offer admits it afresh.
    ///
    /// FLIP: return `false` from `expired` and the access one nanosecond past
    /// the bound is a hit.
    #[test]
    fn an_entry_past_the_age_bound_is_dropped_at_its_first_access() {
        let clock = Arc::new(TestClock::default());
        clock.set(1_000);
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::with_clock(
            1 << 20,
            Some(Arc::clone(&budget)),
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        let c = DecodedCacheCounters::default();
        drop(cache.offer(key(0, 1), &col(8), AccessIntent::Interactive, &c));
        assert!(budget.reserved() > 0);

        clock.set(1_000 + DEFAULT_MAX_ENTRY_AGE_NS);
        assert!(cache.lookup(&key(0, 1), 8, &c).is_some(), "at the bound");
        assert_eq!(cache.expired_drops(), 0);

        clock.set(1_000 + DEFAULT_MAX_ENTRY_AGE_NS + 1);
        assert!(cache.lookup(&key(0, 1), 8, &c).is_none(), "past the bound");
        assert_eq!(cache.expired_drops(), 1);
        assert!(cache.is_empty());
        assert_eq!(cache.resident_bytes(), 0);
        assert_eq!(budget.reserved(), 0);
        let s = c.snapshot();
        assert_eq!((s.hits, s.misses), (1, 1));
    }

    /// The sweep drops exactly the entries past the age bound, whether or not
    /// anything reads them.
    ///
    /// FLIP: make `sweep_expired` return without removing and the old entry is
    /// still resident.
    #[test]
    fn the_sweep_drops_only_entries_past_the_age_bound() {
        let clock = Arc::new(TestClock::default());
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::with_clock(
            1 << 20,
            Some(Arc::clone(&budget)),
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        let c = DecodedCacheCounters::default();
        drop(cache.offer(key(0, 1), &col(8), AccessIntent::Interactive, &c));
        clock.set(DEFAULT_SWEEP_INTERVAL_NS);
        let young = cache
            .offer(key(1, 1), &col(8), AccessIntent::Interactive, &c)
            .expect("admitted");
        let bytes = young.bytes();
        drop(young);
        assert_eq!(cache.sweep_expired(), 0, "nothing is old yet");

        clock.set(DEFAULT_MAX_ENTRY_AGE_NS + 1);
        assert_eq!(cache.sweep_expired(), 1);
        assert_eq!(cache.expired_drops(), 1);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.resident_bytes(), bytes);
        assert_eq!(budget.reserved(), bytes);
        assert!(cache.lookup(&key(1, 1), 8, &c).is_some(), "young survives");

        clock.set(DEFAULT_SWEEP_INTERVAL_NS + DEFAULT_MAX_ENTRY_AGE_NS + 1);
        assert_eq!(cache.sweep_expired(), 1);
        assert!(cache.is_empty());
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn the_margin_is_a_gib_or_five_percent_of_the_limit() {
        assert_eq!(admission_margin(64 << 20), 1 << 30);
        assert_eq!(admission_margin(20 << 30), 1 << 30);
        assert_eq!(admission_margin(40 << 30), 2 << 30);
        assert_eq!(admission_margin(u64::MAX), u64::MAX / 20);
    }

    /// One offer of `v` against a 4 GiB budget with `free` bytes unreserved:
    /// whether it was admitted, and the counters.
    fn offer_with_free(v: &CachedColumn, free: u64) -> (bool, DecodedCacheSnapshot) {
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        budget.reserve_unchecked((4 << 30) - free);
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        drop(cache.offer(key(0, 1), v, AccessIntent::Interactive, &c));
        let admitted = cache.len() == 1;
        (admitted, c.snapshot())
    }

    /// Admission is skipped, and counted as a margin skip, exactly when the
    /// entry's reservation would leave fewer than the margin unreserved: one
    /// byte under skips, exactly at the margin and one byte above admit. A
    /// refusal the limit alone makes is not a margin skip.
    ///
    /// FLIP: make `admission_margin` return 0 and the one-byte-under offer is
    /// admitted.
    #[test]
    fn admission_is_skipped_exactly_below_the_margin() {
        let v = col(8);
        let b = v.heap_bytes() as u64;
        // The 1 GiB floor on a 4 GiB budget, written out so a changed margin
        // moves the outcome rather than the inputs.
        let margin: u64 = 1 << 30;

        let (admitted, s) = offer_with_free(&v, margin + b - 1);
        assert!(!admitted, "one byte under the margin");
        assert_eq!((s.admissions, s.margin_skips), (0, 1));

        let (admitted, s) = offer_with_free(&v, margin + b);
        assert!(admitted, "exactly at the margin");
        assert_eq!((s.admissions, s.margin_skips), (1, 0));

        let (admitted, s) = offer_with_free(&v, margin + b + 1);
        assert!(admitted, "one byte above the margin");
        assert_eq!((s.admissions, s.margin_skips), (1, 0));

        let (admitted, s) = offer_with_free(&v, b - 1);
        assert!(!admitted, "the limit refuses");
        assert_eq!((s.admissions, s.margin_skips), (0, 0));
    }

    /// A closed host gate skips admission whatever the margin, and it is not
    /// a margin skip.
    #[test]
    fn a_closed_gate_skips_admission() {
        let v = col(8);
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        budget.set_host_memory_gate(false, 4, 5);
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        cache.offer(key(0, 1), &v, AccessIntent::Interactive, &c);
        assert!(cache.is_empty());
        assert_eq!(c.snapshot().margin_skips, 0);
        assert_eq!(budget.reserved(), 0);
    }

    /// The race the margin closes. A query holds the whole budget but the one
    /// cache entry, and a reservation of the entry's size is refused. The
    /// budget's evictor (the server's wiring: evict through a weak handle)
    /// frees the entry and then, before it returns and so before the refused
    /// caller retries, offers a new column of the same size, standing in for
    /// another partition's miss landing between the eviction and the retry.
    /// The margin skips that admission, so the freed bytes are still there
    /// and the retry is admitted.
    ///
    /// FLIP: make `admission_margin` return 0 and the in-between offer takes
    /// the freed bytes: the retry is refused and the second `expect` fails.
    #[test]
    fn an_admission_between_eviction_and_retry_does_not_take_the_freed_bytes() {
        const LIMIT: u64 = 4 << 30;
        let budget = Arc::new(MemoryBudget::new(LIMIT));
        let cache = Arc::new(DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget))));
        let c = Arc::new(DecodedCacheCounters::default());
        let b = cache
            .offer(key(0, 1), &col(8), AccessIntent::Interactive, &c)
            .expect("admitted")
            .bytes();
        assert_eq!(cache.len(), 1);

        let offered = Arc::new(AtomicU64::new(0));
        let evictee = Arc::downgrade(&cache);
        let (between, counted) = (Arc::clone(&c), Arc::clone(&offered));
        let margin = admission_margin(LIMIT);
        assert!(
            budget
                .register_evictor(
                    Box::new(move |want, cause| {
                        let Some(cache) = evictee.upgrade() else {
                            return 0;
                        };
                        let freed = cache.evict_for_pressure(want, margin, cause);
                        drop(cache.offer(key(1, 1), &col(8), AccessIntent::Interactive, &between));
                        counted.fetch_add(1, Ordering::Relaxed);
                        freed
                    }),
                    margin,
                )
                .is_ok()
        );

        budget
            .try_reserve(LIMIT - b)
            .expect("the query takes the rest");
        assert_eq!(budget.unreserved(), 0);
        budget
            .try_reserve(b)
            .expect("the freed entry's bytes reach the refused caller");
        assert_eq!(
            offered.load(Ordering::Relaxed),
            1,
            "an offer ran in between"
        );
        let s = c.snapshot();
        assert_eq!((s.admissions, s.margin_skips), (1, 1));
        assert!(cache.is_empty());
        assert_eq!(budget.reserved(), LIMIT);
        assert_eq!(budget.accounted_refusals(), 0);
    }

    /// A cache at its cap swaps one entry for another when the budget leaves
    /// room for the cap plus the margin but not for one more entry on top: the
    /// evicted entry's reservation is released before the new one is taken, so
    /// the offer is admitted, is not a margin skip, and the budget does not
    /// grow past the cap plus the unrelated reservation.
    ///
    /// FLIP: reserve before evicting and the final offer is a margin skip.
    #[test]
    fn a_full_cache_swaps_an_entry_when_the_budget_has_no_room_for_both() {
        const LIMIT: u64 = 4 << 30;
        let margin = admission_margin(LIMIT);
        let b = col(64).heap_bytes() as u64;
        let cap = b * 8;
        let unrelated = LIMIT - margin - cap;
        let budget = Arc::new(MemoryBudget::new(LIMIT));
        budget.reserve_unchecked(unrelated);
        let cache = DecodedColumnCache::new(cap, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        for i in 0..8 {
            assert!(
                cache
                    .offer(key(i, 1), &col(64), AccessIntent::Interactive, &c)
                    .is_some(),
                "fill {i}"
            );
        }
        assert_eq!(cache.resident_bytes(), cap);
        assert_eq!(budget.reserved(), cap + unrelated);

        let admitted = cache.offer(key(100, 1), &col(64), AccessIntent::Interactive, &c);
        assert!(admitted.is_some(), "the swap is admitted");
        let s = c.snapshot();
        assert!(s.evictions >= 1, "evictions {}", s.evictions);
        assert_eq!(s.margin_skips, 0);
        assert_eq!(s.admissions, 9);
        assert!(budget.reserved() <= cap + unrelated, "reserved grew");
    }

    /// Many threads offering and evicting at once never leave the budget's
    /// reservation out of step with the bytes still held: once every handle
    /// is gone and the index is emptied, nothing stays reserved.
    #[test]
    fn concurrent_offers_and_pressure_leave_no_reservation_behind() {
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = Arc::new(DecodedColumnCache::new(
            col(64).heap_bytes() as u64 * 16,
            Some(Arc::clone(&budget)),
        ));
        let threads: Vec<_> = (0..8u32)
            .map(|t| {
                let cache = Arc::clone(&cache);
                std::thread::spawn(move || {
                    let c = DecodedCacheCounters::default();
                    let mut held = Vec::new();
                    for i in 0..200u32 {
                        let k = key(t * 1000 + i % 40, 1);
                        match cache.lookup(&k, 64, &c) {
                            Some(e) => held.push(e),
                            None => {
                                if let Some(e) =
                                    cache.offer(k, &col(64), AccessIntent::Interactive, &c)
                                {
                                    held.push(e);
                                }
                            }
                        }
                        if i % 7 == 0 {
                            held.clear();
                            cache.evict_for_pressure(1 << 12, 0, ExhaustionCause::Accounted);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("thread");
        }
        assert_eq!(budget.reserved(), cache.resident_bytes());
        cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted);
        assert!(cache.is_empty());
        assert_eq!(budget.reserved(), 0);
    }
}
