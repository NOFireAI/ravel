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
//! [`MemoryBudget`]; an entry that cannot reserve, or whose reservation would
//! leave the budget under [`admission_margin`], is not admitted.
//!
//! [`decode_columns`]: crate::block

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

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

/// The unreserved bytes an admission must leave on a budget of `limit`
/// (ADR-3326 amendment "admission keeps a headroom margin"):
/// `max(1 GiB, 5% of limit)`.
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
    budget: Option<Arc<MemoryBudget>>,
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
    /// reserved best effort on `budget` when one is given.
    pub fn new(max_bytes: u64, budget: Option<Arc<MemoryBudget>>) -> Self {
        DecodedColumnCache {
            max_bytes,
            budget,
            inner: Mutex::new(Inner::default()),
            pressure_evictions: AtomicU64::new(0),
            pressure_freed_bytes: AtomicU64::new(0),
        }
    }

    /// Pressure evictions since construction.
    pub fn pressure_counters(&self) -> PressureSnapshot {
        PressureSnapshot {
            evictions: self.pressure_evictions.load(Ordering::Relaxed),
            freed_bytes: self.pressure_freed_bytes.load(Ordering::Relaxed),
        }
    }

    /// The budget's pressure evictor (ADR-3326 decision 4): evicts, in CLOCK
    /// order, entries no live scan holds until `want` bytes are freed or none
    /// remains, releasing each one's reservation, and returns the bytes
    /// freed. An entry a scan still holds stays and counts for nothing.
    /// Each pass logs one INFO line; `cause` is the refusal it answers and
    /// `slack` the part of `want` the budget asks beyond the refused caller's
    /// shortfall (logged only).
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
                if slot.col.strong_count() > 1 {
                    if step < n {
                        held = held.saturating_add(slot.bytes);
                    }
                    continue;
                }
                if slot.referenced {
                    slot.referenced = false;
                    continue;
                }
                if let Some(slot) = inner.slots[i].take() {
                    inner.index.remove(&slot.key);
                    inner.resident = inner.resident.saturating_sub(slot.bytes);
                    inner.free.push(i);
                    freed = freed.saturating_add(slot.bytes);
                    evicted.push(slot);
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
        // Buffers and reservations go outside the lock.
        drop(evicted);
        freed
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
                            inner.process.margin_skips += 1;
                            DecodedCacheCounters::bump(&counters.margin_skips, 1);
                        }
                        return;
                    }
                }
            }
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

    /// The pressure evictor skips an entry a scan still holds and counts only
    /// the bytes of the entry it held the last handle to, whose reservation it
    /// released. Asked for more than every unheld entry holds, it returns
    /// what it freed rather than the request.
    ///
    /// FLIP: drop the `strong_count() > 1` skip in `evict_for_pressure` and
    /// the held entry is evicted, `freed` reads `2 * bytes`.
    #[test]
    fn pressure_eviction_never_evicts_or_counts_an_entry_a_scan_holds() {
        let held = col(8);
        let idle = col(8);
        let bytes = held.heap_bytes() as u64;
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        cache.offer(key(0, 1), &held, AccessIntent::Interactive, &c);
        cache.offer(key(1, 1), &idle, AccessIntent::Interactive, &c);
        drop(idle);
        assert_eq!(budget.reserved(), 2 * bytes);

        let freed = cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted);
        assert_eq!(freed, bytes, "only the unheld entry frees");
        assert_eq!(budget.reserved(), bytes, "its reservation is released");
        assert_eq!(cache.entry_bytes(&key(0, 1)), Some(bytes), "held stays");
        assert_eq!(cache.entry_bytes(&key(1, 1)), None);
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

    #[test]
    fn an_admitted_entry_holds_its_reservation_until_evicted() {
        let v = col(8);
        let bytes = v.heap_bytes() as u64;
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::new(bytes, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        cache.offer(key(0, 1), &v, AccessIntent::Interactive, &c);
        assert_eq!(budget.reserved(), bytes);
        cache.offer(key(1, 1), &v, AccessIntent::Interactive, &c);
        assert_eq!(budget.reserved(), bytes);
        assert_eq!(cache.len(), 1);
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
        cache.offer(key(0, 1), v, AccessIntent::Interactive, &c);
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

    /// The race the amendment closes. A query holds the whole budget but the
    /// one cache entry, and a reservation of the entry's size is refused. The
    /// budget's evictor (the server's wiring: evict through a weak handle)
    /// frees the entry and then, before it returns and so before the refused
    /// caller retries, offers a new column of the same size, standing in for
    /// another partition's miss landing between the eviction and the retry.
    /// The margin skips that admission, so the freed bytes are still there
    /// and the retry is admitted.
    ///
    /// FLIP: make `admission_margin` return 0 and the in-between offer takes
    /// the freed bytes: the retry is refused and the first `expect` fails.
    #[test]
    fn an_admission_between_eviction_and_retry_does_not_take_the_freed_bytes() {
        const LIMIT: u64 = 4 << 30;
        let v1 = col(8);
        let b = v1.heap_bytes() as u64;
        let budget = Arc::new(MemoryBudget::new(LIMIT));
        let cache = Arc::new(DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget))));
        let c = Arc::new(DecodedCacheCounters::default());
        cache.offer(key(0, 1), &v1, AccessIntent::Interactive, &c);
        drop(v1);
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
                        cache.offer(key(1, 1), &col(8), AccessIntent::Interactive, &between);
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
}
