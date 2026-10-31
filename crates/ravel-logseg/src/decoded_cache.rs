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
//! Admission that needs an eviction is also gated on frequency (the
//! LIKE-deferred amendment, decision 1). Every resident slot and ghost record
//! carries an access count, a `u8` saturating at [`MAX_COUNT`], which a
//! columnar lookup adds one to: a hit to the slot, a miss to the key's ghost
//! record if it has one. An admitted entry starts at its ghost record's count,
//! or 1 (`the_gate_refuses_a_less_used_newcomer_and_ties` pins both). When an
//! offer has to evict, the first unreferenced victim the CLOCK hand reaches
//! whose count is at least the newcomer's ends the walk and refuses the
//! admission, so ties refuse (the same test); victims evicted and reference
//! bits cleared before it stay so
//! (`a_refused_walk_keeps_what_it_evicted_and_cleared`). A refused admission
//! leaves a ghost record carrying the newcomer's count, under any intent, and
//! an admission releases the key's ghost record and its bytes under any intent
//! (`a_gate_refusal_leaves_a_ghost_record_an_admission_releases`). A repeated
//! scan larger than the cap keeps a stable subset resident
//! (`an_over_cap_loop_keeps_a_stable_subset`). Counts are halved when the
//! lookups since the last halving reach [`aging_window`]
//! (`counts_halve_when_lookups_reach_the_aging_window`). The pressure evictor
//! ignores counts
//! (`the_pressure_evictor_frees_the_shortfall_plus_slack_whatever_the_counts`).
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

/// The saturation point of a slot's or ghost record's access count.
pub const MAX_COUNT: u8 = 15;

/// The lookups after which every count is halved, for a cache holding
/// `records` slots plus ghost records: ten per record, at least 65,536
/// (TinyLFU's reset).
#[must_use]
pub fn aging_window(records: usize) -> u64 {
    (records as u64).saturating_mul(10).max(1 << 16)
}

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
    frequency_refusals: AtomicU64,
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
    /// Admissions refused because an eviction would have removed an entry
    /// accessed at least as often as the newcomer.
    pub frequency_refusals: u64,
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
            frequency_refusals: self
                .frequency_refusals
                .saturating_add(other.frequency_refusals),
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
            frequency_refusals: self.frequency_refusals.load(Ordering::Relaxed),
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
    count: u8,
}

/// A key the cache does not hold but has seen: a non-interactive first touch
/// or a gate refusal.
struct Ghost {
    /// The generation it was recorded at, so a FIFO entry superseded by a
    /// later record of the same key does not remove that later record.
    generation: u64,
    bytes: u64,
    count: u8,
}

#[derive(Default)]
struct Inner {
    index: HashMap<DecodedKey, usize>,
    slots: Vec<Option<Slot>>,
    free: Vec<usize>,
    hand: usize,
    resident: u64,
    ghost: HashMap<DecodedKey, Ghost>,
    ghost_fifo: VecDeque<(DecodedKey, u64)>,
    ghost_bytes: u64,
    ghost_gen: u64,
    /// Lookups since counts were last halved.
    lookups_since_aging: u64,
    column_hits: HashMap<u32, u64>,
    process: DecodedCacheSnapshot,
    expired_drops: u64,
}

impl Inner {
    /// The admission rule by intent (ADR-3326 decision 3): an interactive
    /// statement admits on first decode, any other on the touch after its
    /// ghost record.
    fn intent_admits(&self, key: &DecodedKey, intent: AccessIntent) -> bool {
        intent == AccessIntent::Interactive || self.ghost.contains_key(key)
    }

    /// The count an admission of `key` starts at: its ghost record's, or 1.
    fn newcomer_count(&self, key: &DecodedKey) -> u8 {
        self.ghost.get(key).map_or(1, |g| g.count)
    }

    /// Counts one lookup towards the aging window and halves every count
    /// once the window is reached.
    fn age(&mut self) {
        self.lookups_since_aging += 1;
        if self.lookups_since_aging < aging_window(self.index.len() + self.ghost.len()) {
            return;
        }
        self.lookups_since_aging = 0;
        for slot in self.slots.iter_mut().flatten() {
            slot.count /= 2;
        }
        for ghost in self.ghost.values_mut() {
            ghost.count /= 2;
        }
    }

    /// Releases `key`'s ghost record and its bytes, if it has one.
    fn release_ghost(&mut self, key: &DecodedKey) {
        if let Some(g) = self.ghost.remove(key) {
            self.ghost_bytes = self.ghost_bytes.saturating_sub(g.bytes);
        }
    }
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
    /// Run once between the reservation and the second lock of the next
    /// offer, so a test can admit entries in that window.
    #[cfg(test)]
    between_locks: Mutex<Option<Box<dyn FnOnce() + Send>>>,
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
            #[cfg(test)]
            between_locks: Mutex::new(None),
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

    /// The access count of `key`'s slot.
    #[cfg(test)]
    pub(crate) fn count_of(&self, key: &DecodedKey) -> Option<u8> {
        let inner = self.lock();
        let i = *inner.index.get(key)?;
        inner.slots.get(i)?.as_ref().map(|s| s.count)
    }

    /// Whether `key`'s slot has its CLOCK reference bit set.
    #[cfg(test)]
    pub(crate) fn referenced_of(&self, key: &DecodedKey) -> Option<bool> {
        let inner = self.lock();
        let i = *inner.index.get(key)?;
        inner.slots.get(i)?.as_ref().map(|s| s.referenced)
    }

    /// The access count and bytes of `key`'s ghost record.
    #[cfg(test)]
    pub(crate) fn ghost_of(&self, key: &DecodedKey) -> Option<(u8, u64)> {
        self.lock().ghost.get(key).map(|g| (g.count, g.bytes))
    }

    /// Bytes the ghost records hold.
    #[cfg(test)]
    pub(crate) fn ghost_bytes(&self) -> u64 {
        self.lock().ghost_bytes
    }

    #[cfg(test)]
    pub(crate) fn run_between_locks(&self, hook: Box<dyn FnOnce() + Send>) {
        *self
            .between_locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(hook);
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

    /// The entry for `key`, marking it referenced for CLOCK and adding one to
    /// its access count, or on a miss to its ghost record's. `rows` is the
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
                    s.count = s.count.saturating_add(1).min(MAX_COUNT);
                    found = Some(Arc::clone(&s.entry));
                }
            }
            if found.is_none()
                && let Some(g) = inner.ghost.get_mut(key)
            {
                g.count = g.count.saturating_add(1).min(MAX_COUNT);
            }
            inner.age();
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

    /// Whether an [`offer`](Self::offer) of `key` under `intent` would pass
    /// the intent rule now: an interactive intent, or any other intent whose
    /// key holds a ghost record. Reads the same [`Inner::intent_admits`] check
    /// `offer` applies and changes nothing, so the ghost record stays for the
    /// offer to spend. A size, frequency-gate, budget or concurrent-admission
    /// refusal in `offer` can still turn a `true` into no admission: the gate
    /// is not mirrored here (the LIKE-deferred amendment, decision 1).
    pub(crate) fn would_admit(&self, key: &DecodedKey, intent: AccessIntent) -> bool {
        self.lock().intent_admits(key, intent)
    }

    /// Offers a freshly decoded column for admission under `intent`'s rule,
    /// returning the admitted entry so the caller can hold it for as long as
    /// it uses the column.
    ///
    /// The checks run in the LIKE-deferred amendment's order: the size check,
    /// the intent rule, the frequency gate during eviction, the reservation
    /// with the headroom margin, and the gate again during the eviction after
    /// the reservation. A gate refusal at either eviction leaves the key's
    /// ghost record and the ghost-hit counters as they were before the offer
    /// (`a_second_eviction_refusal_leaves_the_ghost_record_and_ghost_hits`),
    /// or records one carrying the newcomer's count when the key had none.
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
            if !inner.intent_admits(&key, intent) {
                self.record_ghost(&mut inner, key, bytes, 1);
                return None;
            }
            let newcomer = inner.newcomer_count(&key);
            if !self.evict_for(&mut inner, bytes, newcomer, &mut evicted, counters) {
                self.refuse_by_frequency(&mut inner, key, bytes, newcomer, counters);
                return None;
            }
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

        #[cfg(test)]
        {
            let hook = self
                .between_locks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }

        let mut evicted = Vec::new();
        let admitted = {
            let mut inner = self.lock();
            if inner.index.contains_key(&key) {
                // Admitted concurrently; `reservation` drops with this frame.
                return None;
            }
            // Entries admitted between the two locks may have refilled the cap.
            let newcomer = inner.newcomer_count(&key);
            if !self.evict_for(&mut inner, bytes, newcomer, &mut evicted, counters) {
                self.refuse_by_frequency(&mut inner, key, bytes, newcomer, counters);
                return None;
            }
            // The touch is spent only now that nothing can refuse it: the
            // ghost record (if the FIFO hasn't already dropped it) is
            // released under every intent, and a non-interactive second
            // touch is counted.
            inner.release_ghost(&key);
            if intent != AccessIntent::Interactive {
                inner.process.ghost_hits += 1;
                DecodedCacheCounters::bump(&counters.ghost_hits, 1);
            }
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
                count: newcomer,
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

    /// Evicts until `bytes` more fit under the cap, gated on a newcomer of
    /// access count `newcomer`, and counts the evictions. Returns whether the
    /// newcomer now fits.
    fn evict_for(
        &self,
        inner: &mut Inner,
        bytes: u64,
        newcomer: u8,
        evicted: &mut Vec<Arc<DecodedEntry>>,
        counters: &DecodedCacheCounters,
    ) -> bool {
        let before = evicted.len();
        let fits = Self::evict_until(
            inner,
            self.max_bytes.saturating_sub(bytes),
            newcomer,
            evicted,
        );
        let n = (evicted.len() - before) as u64;
        inner.process.evictions += n;
        DecodedCacheCounters::bump(&counters.evictions, n);
        fits
    }

    /// Counts a gate refusal and leaves a ghost record carrying the
    /// newcomer's count, unless the key already holds one.
    fn refuse_by_frequency(
        &self,
        inner: &mut Inner,
        key: DecodedKey,
        bytes: u64,
        newcomer: u8,
        counters: &DecodedCacheCounters,
    ) {
        self.record_ghost(inner, key, bytes, newcomer);
        inner.process.frequency_refusals += 1;
        DecodedCacheCounters::bump(&counters.frequency_refusals, 1);
    }

    fn record_ghost(&self, inner: &mut Inner, key: DecodedKey, bytes: u64, count: u8) {
        if inner.ghost.contains_key(&key) {
            return;
        }
        inner.ghost_gen += 1;
        let generation = inner.ghost_gen;
        inner.ghost.insert(
            key,
            Ghost {
                generation,
                bytes,
                count,
            },
        );
        inner.ghost_fifo.push_back((key, generation));
        inner.ghost_bytes += bytes;
        while inner.ghost_bytes > self.max_bytes {
            let Some((old, g)) = inner.ghost_fifo.pop_front() else {
                break;
            };
            if inner.ghost.get(&old).is_some_and(|cur| cur.generation == g) {
                inner.release_ghost(&old);
            }
        }
    }

    /// CLOCK with the frequency gate: sweep the hand, clearing reference
    /// bits and evicting entries whose bit is already clear, until resident
    /// bytes are at most `target`. An unreferenced entry whose access count
    /// is at least `newcomer` ends the walk and the result is `false`; what
    /// the walk evicted and cleared before it stays so. Evicted entries go to
    /// `evicted` for the caller to drop outside the lock; one a scan still
    /// holds keeps its reservation until that scan lets go.
    fn evict_until(
        inner: &mut Inner,
        target: u64,
        newcomer: u8,
        evicted: &mut Vec<Arc<DecodedEntry>>,
    ) -> bool {
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
            if slot.count >= newcomer {
                return false;
            }
            if let Some(entry) = inner.remove(i) {
                evicted.push(entry);
            }
        }
        true
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

    /// Gives fresh key `k` a ghost record of access count `count`: a
    /// bulk-scan first touch records it at 1 and each missed lookup adds one.
    fn ghost_with_count(cache: &DecodedColumnCache, k: DecodedKey, v: &CachedColumn, count: u8) {
        let c = DecodedCacheCounters::default();
        assert!(cache.offer(k, v, AccessIntent::BulkScan, &c).is_none());
        for _ in 1..count {
            assert!(cache.lookup(&k, v.rows(), &c).is_none());
        }
        assert_eq!(cache.ghost_of(&k).map(|(n, _)| n), Some(count));
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

    /// `would_admit` answers exactly what the following `offer` does, for
    /// every intent with and without a ghost record, while the newcomer fits
    /// without an eviction (the frequency gate is not mirrored), and asking does not
    /// record, consume or change a ghost record: asked twice before the
    /// first touch, a non-interactive key still needs that touch.
    #[test]
    fn would_admit_agrees_with_offer_and_changes_nothing() {
        for intent in [
            AccessIntent::Interactive,
            AccessIntent::BulkScan,
            AccessIntent::Maintenance,
        ] {
            let cache = DecodedColumnCache::new(1 << 20, None);
            let c = DecodedCacheCounters::default();
            let v = col(8);
            let interactive = intent == AccessIntent::Interactive;

            // No ghost record.
            assert_eq!(cache.would_admit(&key(0, 1), intent), interactive);
            assert_eq!(cache.would_admit(&key(0, 1), intent), interactive);
            let admitted = cache.offer(key(0, 1), &v, intent, &c).is_some();
            assert_eq!(admitted, interactive, "{intent:?} without a ghost");
            assert_eq!(c.snapshot().ghost_hits, 0);

            // The refused first touch left a ghost record.
            if !interactive {
                assert!(cache.would_admit(&key(0, 1), intent));
                assert!(cache.would_admit(&key(0, 1), intent));
                assert_eq!(c.snapshot().ghost_hits, 0, "asking spends nothing");
                assert!(
                    cache.offer(key(0, 1), &v, intent, &c).is_some(),
                    "{intent:?} with a ghost"
                );
                let s = c.snapshot();
                assert_eq!((s.ghost_hits, s.admissions), (1, 1));
            }

            // An interactive offer on a key with a ghost record admits too.
            let other = DecodedColumnCache::new(1 << 20, None);
            assert!(
                other
                    .offer(key(1, 1), &v, AccessIntent::BulkScan, &c)
                    .is_none()
            );
            assert!(other.would_admit(&key(1, 1), intent));
            assert!(other.offer(key(1, 1), &v, intent, &c).is_some());
        }
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
        // Outranks block 1's count of 1, so the frequency gate lets it in.
        ghost_with_count(&cache, key(2, 1), &v, 2);
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

        // Admitting a second entry, which outranks the first, evicts the
        // first from the index.
        ghost_with_count(&cache, key(1, 1), &col(8), 2);
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

        ghost_with_count(&cache, key(100, 1), &col(64), 2);
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

    /// The frequency gate (the LIKE-deferred amendment, decision 1): with
    /// every victim at count 2, a newcomer at 1 is refused and counted, one
    /// at 2 is refused too (ties refuse), and one at 3 evicts a victim and is
    /// admitted at its ghost record's count.
    ///
    /// FLIP: compare `slot.count > newcomer` in `evict_until` and the tie at
    /// block 3 is admitted: the second `is_none` fails.
    #[test]
    fn the_gate_refuses_a_less_used_newcomer_and_ties() {
        let v = col(8);
        let b = v.heap_bytes() as u64;
        let cache = DecodedColumnCache::new(2 * b, None);
        let c = DecodedCacheCounters::default();
        for k in 0..2 {
            assert!(
                cache
                    .offer(key(k, 1), &v, AccessIntent::Interactive, &c)
                    .is_some()
            );
            assert!(cache.lookup(&key(k, 1), 8, &c).is_some());
            assert_eq!(cache.count_of(&key(k, 1)), Some(2));
        }

        assert!(
            cache
                .offer(key(2, 1), &v, AccessIntent::Interactive, &c)
                .is_none(),
            "count 1 against 2"
        );
        let s = c.snapshot();
        assert_eq!((s.frequency_refusals, s.evictions, s.admissions), (1, 0, 2));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.ghost_of(&key(2, 1)), Some((1, b)));

        ghost_with_count(&cache, key(3, 1), &v, 2);
        assert!(
            cache
                .offer(key(3, 1), &v, AccessIntent::Interactive, &c)
                .is_none(),
            "a tie refuses"
        );
        let s = c.snapshot();
        assert_eq!((s.frequency_refusals, s.evictions, s.admissions), (2, 0, 2));
        assert_eq!(cache.ghost_of(&key(3, 1)), Some((2, b)), "unchanged");

        ghost_with_count(&cache, key(4, 1), &v, 3);
        assert!(
            cache
                .offer(key(4, 1), &v, AccessIntent::Interactive, &c)
                .is_some(),
            "count 3 against 2"
        );
        let s = c.snapshot();
        assert_eq!((s.frequency_refusals, s.evictions, s.admissions), (2, 1, 3));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.count_of(&key(4, 1)), Some(3));
        assert_eq!(cache.ghost_of(&key(4, 1)), None);
        assert_eq!(cache.process_counters().frequency_refusals, 2);
    }

    /// A walk the gate ends keeps what it did before the refusal: block 0
    /// (count 1) is evicted, blocks 1 and 2 lose their reference bits, and
    /// block 1 (count 2) then ties the newcomer and refuses it.
    ///
    /// FLIP: compare the count in `evict_until` before the reference bit, so
    /// a referenced victim can refuse, and the walk stops at block 1 with its
    /// bit still set: `referenced_of(&key(1, 1))` reads `Some(true)`.
    #[test]
    fn a_refused_walk_keeps_what_it_evicted_and_cleared() {
        let v = col(8);
        let b = v.heap_bytes() as u64;
        let big = col(16);
        let n = big.heap_bytes() as u64;
        assert!(b < n && n <= 2 * b, "the newcomer needs two victims' room");
        let cache = DecodedColumnCache::new(3 * b, None);
        let c = DecodedCacheCounters::default();
        for k in 0..3 {
            assert!(
                cache
                    .offer(key(k, 1), &v, AccessIntent::Interactive, &c)
                    .is_some()
            );
            for _ in 0..k {
                assert!(cache.lookup(&key(k, 1), 8, &c).is_some());
            }
        }
        ghost_with_count(&cache, key(3, 1), &big, 2);

        assert!(
            cache
                .offer(key(3, 1), &big, AccessIntent::Interactive, &c)
                .is_none()
        );
        let s = c.snapshot();
        assert_eq!((s.evictions, s.frequency_refusals, s.admissions), (1, 1, 3));
        assert_eq!(cache.count_of(&key(0, 1)), None);
        assert_eq!(cache.referenced_of(&key(1, 1)), Some(false));
        assert_eq!(cache.referenced_of(&key(2, 1)), Some(false));
        assert_eq!(cache.count_of(&key(2, 1)), Some(3));
        assert_eq!(cache.resident_bytes(), 2 * b);
        assert_eq!(cache.ghost_of(&key(3, 1)), Some((2, n)));
    }

    /// A loop over six keys through a cache that holds four keeps the four it
    /// admitted first: the second and third passes hit exactly blocks 0 to 3.
    /// CLOCK alone, which admits every miss, hits nothing on any pass of this
    /// loop, since each admission evicts the entry the loop reaches next.
    ///
    /// FLIP: return `true` from `evict_until` without the count comparison
    /// (CLOCK as before the gate) and the hits per pass read `[0, 0, 0]`.
    #[test]
    fn an_over_cap_loop_keeps_a_stable_subset() {
        let v = col(8);
        let b = v.heap_bytes() as u64;
        let cache = DecodedColumnCache::new(4 * b, None);
        let mut passes = Vec::new();
        for _ in 0..3 {
            let c = DecodedCacheCounters::default();
            let mut hit = Vec::new();
            for k in 0..6 {
                if cache.lookup(&key(k, 1), 8, &c).is_some() {
                    hit.push(k);
                } else {
                    drop(cache.offer(key(k, 1), &v, AccessIntent::Interactive, &c));
                }
            }
            let s = c.snapshot();
            passes.push((hit, s.hits, s.admissions, s.evictions, s.frequency_refusals));
        }
        assert_eq!(
            passes,
            vec![
                (vec![], 0, 4, 0, 2),
                (vec![0, 1, 2, 3], 4, 0, 0, 2),
                (vec![0, 1, 2, 3], 4, 0, 0, 2),
            ]
        );
    }

    /// A gate refusal under an interactive intent leaves a ghost record
    /// carrying the newcomer's count and bytes; missed lookups add to it, and
    /// the interactive admission it then earns starts at that count and
    /// releases the record and its bytes. Only a non-interactive admission is
    /// a ghost hit.
    ///
    /// FLIP: release the ghost record only when `intent != Interactive` (as
    /// before the amendment) and the record, count 3 and `b` bytes, is still
    /// there after the admission.
    #[test]
    fn a_gate_refusal_leaves_a_ghost_record_an_admission_releases() {
        let v = col(8);
        let b = v.heap_bytes() as u64;
        let cache = DecodedColumnCache::new(b, None);
        let c = DecodedCacheCounters::default();
        assert!(
            cache
                .offer(key(0, 1), &v, AccessIntent::Interactive, &c)
                .is_some()
        );
        assert!(cache.lookup(&key(0, 1), 8, &c).is_some());

        assert!(
            cache
                .offer(key(1, 1), &v, AccessIntent::Interactive, &c)
                .is_none()
        );
        assert_eq!(cache.ghost_of(&key(1, 1)), Some((1, b)));
        assert_eq!(cache.ghost_bytes(), b);

        for _ in 0..2 {
            assert!(cache.lookup(&key(1, 1), 8, &c).is_none());
        }
        assert_eq!(cache.ghost_of(&key(1, 1)), Some((3, b)));
        assert!(
            cache
                .offer(key(1, 1), &v, AccessIntent::Interactive, &c)
                .is_some()
        );
        assert_eq!(cache.count_of(&key(1, 1)), Some(3));
        assert_eq!(cache.ghost_of(&key(1, 1)), None);
        assert_eq!(cache.ghost_bytes(), 0);
        let s = c.snapshot();
        assert_eq!(
            (
                s.admissions,
                s.evictions,
                s.frequency_refusals,
                s.ghost_hits
            ),
            (2, 1, 1, 0)
        );
    }

    /// The amendment's required case: a bulk-scan second touch passes the
    /// first eviction and reserves, then entries admitted between the two
    /// locks refill the cap with counts the newcomer only ties. The second
    /// eviction refuses: the ghost record keeps its count, `ghost_hits` stays
    /// 0 and the reservation is dropped. A later touch that out-counts the
    /// residents still admits on that record.
    ///
    /// FLIP: move `release_ghost` and the `ghost_hits` bump in `offer` above
    /// the second `evict_for` and the refusal reads `ghost_hits` 1: the
    /// snapshot assertion after the refusal fails with `(0, 0, 1, 1)`.
    #[test]
    fn a_second_eviction_refusal_leaves_the_ghost_record_and_ghost_hits() {
        let v = col(8);
        let b = v.heap_bytes() as u64;
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = Arc::new(DecodedColumnCache::new(2 * b, Some(Arc::clone(&budget))));
        let other = DecodedCacheCounters::default();
        assert!(
            cache
                .offer(key(9, 1), &v, AccessIntent::Interactive, &other)
                .is_some()
        );
        ghost_with_count(&cache, key(0, 1), &v, 2);

        let between = Arc::downgrade(&cache);
        cache.run_between_locks(Box::new(move || {
            let Some(cache) = between.upgrade() else {
                return;
            };
            let c = DecodedCacheCounters::default();
            drop(cache.offer(key(8, 1), &col(8), AccessIntent::Interactive, &c));
            for k in [8, 9] {
                assert!(cache.lookup(&key(k, 1), 8, &c).is_some());
            }
        }));

        let c = DecodedCacheCounters::default();
        assert!(
            cache
                .offer(key(0, 1), &v, AccessIntent::BulkScan, &c)
                .is_none()
        );
        let s = c.snapshot();
        assert_eq!(
            (
                s.admissions,
                s.evictions,
                s.frequency_refusals,
                s.ghost_hits
            ),
            (0, 0, 1, 0)
        );
        assert_eq!(cache.process_counters().ghost_hits, 0);
        assert_eq!(cache.ghost_of(&key(0, 1)), Some((2, b)));
        assert_eq!(cache.len(), 2);
        assert_eq!(budget.reserved(), 2 * b, "the reservation was dropped");

        assert!(cache.lookup(&key(0, 1), 8, &c).is_none());
        assert!(
            cache
                .offer(key(0, 1), &v, AccessIntent::BulkScan, &c)
                .is_some()
        );
        let s = c.snapshot();
        assert_eq!(
            (s.admissions, s.frequency_refusals, s.ghost_hits),
            (1, 1, 1)
        );
        assert_eq!(cache.count_of(&key(0, 1)), Some(3));
        assert_eq!(cache.ghost_of(&key(0, 1)), None);
    }

    /// Counts saturate at 15 and every count, on slots and on ghost records,
    /// halves on exactly the lookup that reaches the aging window: 65,536
    /// lookups for a cache of two records.
    ///
    /// FLIP: halve when `lookups_since_aging` exceeds the window rather than
    /// reaches it and the counts read 15 and 3 after the 65,536th lookup.
    #[test]
    fn counts_halve_when_lookups_reach_the_aging_window() {
        assert_eq!(aging_window(2), 65_536);
        assert_eq!(aging_window(10_000), 100_000);
        let v = col(8);
        let cache = DecodedColumnCache::new(1 << 20, None);
        let c = DecodedCacheCounters::default();
        assert!(
            cache
                .offer(key(0, 1), &v, AccessIntent::Interactive, &c)
                .is_some()
        );
        for _ in 0..20 {
            assert!(cache.lookup(&key(0, 1), 8, &c).is_some());
        }
        ghost_with_count(&cache, key(1, 1), &v, 3);
        assert_eq!(cache.count_of(&key(0, 1)), Some(MAX_COUNT));

        // 22 lookups so far; 65,513 more on an absent key make 65,535.
        for _ in 0..65_513 {
            assert!(cache.lookup(&key(99, 1), 8, &c).is_none());
        }
        assert_eq!(cache.count_of(&key(0, 1)), Some(15));
        assert_eq!(cache.ghost_of(&key(1, 1)).map(|(n, _)| n), Some(3));

        assert!(cache.lookup(&key(99, 1), 8, &c).is_none());
        assert_eq!(cache.count_of(&key(0, 1)), Some(7));
        assert_eq!(cache.ghost_of(&key(1, 1)).map(|(n, _)| n), Some(1));

        assert!(cache.lookup(&key(99, 1), 8, &c).is_none());
        assert_eq!(cache.count_of(&key(0, 1)), Some(7), "the window restarts");
    }

    /// The pressure evictor ignores counts: asked for a shortfall of two
    /// entries plus one entry of slack, it frees exactly three entries at
    /// the saturated count, which the gate would never let an admission
    /// evict.
    #[test]
    fn the_pressure_evictor_frees_the_shortfall_plus_slack_whatever_the_counts() {
        let v = col(8);
        let b = v.heap_bytes() as u64;
        let cache = DecodedColumnCache::new(8 * b, None);
        let c = DecodedCacheCounters::default();
        for k in 0..8 {
            assert!(
                cache
                    .offer(key(k, 1), &v, AccessIntent::Interactive, &c)
                    .is_some()
            );
            for _ in 0..20 {
                assert!(cache.lookup(&key(k, 1), 8, &c).is_some());
            }
            assert_eq!(cache.count_of(&key(k, 1)), Some(MAX_COUNT));
        }
        let freed = cache.evict_for_pressure(2 * b + b, b, ExhaustionCause::Accounted);
        assert_eq!(freed, 3 * b);
        assert_eq!(cache.len(), 5);
        assert_eq!(cache.resident_bytes(), 5 * b);
        assert_eq!(c.snapshot().frequency_refusals, 0);
    }

    /// The production-shaped traces of the LIKE-deferred amendment, decision
    /// 5, scaled in bytes and not in entries (decision 6): the model's 8 GB
    /// cap over uniform 400 KB entries holds 20,000 entries, and so does this
    /// cache, of 20,000 equal entries. The entry and lookup counts are the
    /// model's, which keeps the aging window above its 65,536 floor.
    const TRACE_SLOTS: u64 = 20_000;

    /// A trace driver over one cache: every access is the columnar path, a
    /// lookup and on a miss an offer under the statement's intent, and every
    /// halving of the counts is recorded at the lookup that caused it.
    struct Trace {
        cache: DecodedColumnCache,
        v: CachedColumn,
        lookups: u64,
        halvings: Vec<u64>,
    }

    impl Trace {
        fn new() -> Self {
            let v = col(8);
            let cache = DecodedColumnCache::new(TRACE_SLOTS * v.heap_bytes() as u64, None);
            Trace {
                cache,
                v,
                lookups: 0,
                halvings: Vec::new(),
            }
        }

        /// Entry `i` of set `set`: keys are distinct per (object, block,
        /// column).
        fn key(set: u8, i: u32) -> DecodedKey {
            DecodedKey {
                tenant: [1; 16],
                object: [set; 32],
                block: i,
                column: 1,
                layout: LAYOUT_TAG,
            }
        }

        /// One access; returns whether it hit.
        fn access(&mut self, k: DecodedKey, intent: AccessIntent) -> bool {
            let c = DecodedCacheCounters::default();
            let hit = self.cache.lookup(&k, 8, &c).is_some();
            self.lookups += 1;
            if self.cache.lock().lookups_since_aging == 0 {
                self.halvings.push(self.lookups);
            }
            if !hit {
                drop(self.cache.offer(k, &self.v, intent, &c));
            }
            hit
        }
    }

    /// Trace B: a 10,000-entry interactive working set W read in full every
    /// round, through a 30,000-entry scan S read once per round, the two
    /// interleaved one W entry to three S entries. Returns W's hit share per
    /// round and the trace.
    fn trace_b(scan: AccessIntent, rounds: usize) -> (Vec<f64>, Trace) {
        const W: u32 = 10_000;
        let mut t = Trace::new();
        let mut shares = Vec::new();
        for _ in 0..rounds {
            let mut hits = 0u32;
            for i in 0..W {
                if t.access(Trace::key(1, i), AccessIntent::Interactive) {
                    hits += 1;
                }
                for j in 0..3 {
                    t.access(Trace::key(2, 3 * i + j), scan);
                }
            }
            shares.push(f64::from(hits) / f64::from(W));
        }
        (shares, t)
    }

    /// Trace E: a 15,000-entry set A read `k` passes, then a disjoint
    /// 15,000-entry set B read up to `max_passes` passes, all interactive.
    /// Returns B's hit share per pass and the trace.
    fn trace_e(k: usize, max_passes: usize) -> (Vec<f64>, Trace) {
        const N: u32 = 15_000;
        let mut t = Trace::new();
        for _ in 0..k {
            for i in 0..N {
                t.access(Trace::key(1, i), AccessIntent::Interactive);
            }
        }
        let mut shares = Vec::new();
        for _ in 0..max_passes {
            let mut hits = 0u32;
            for i in 0..N {
                if t.access(Trace::key(2, i), AccessIntent::Interactive) {
                    hits += 1;
                }
            }
            shares.push(f64::from(hits) / f64::from(N));
        }
        (shares, t)
    }

    /// The 1-based first pass whose hit share reaches 0.90.
    fn first_pass_at_090(shares: &[f64]) -> Option<usize> {
        shares.iter().position(|&s| s >= 0.90).map(|p| p + 1)
    }

    /// Trace B's two registered rows (decision 6): W retained at least 0.90
    /// on every round after the cold first one under a bulk-scan scan (model
    /// 1.000), and at least 0.40 under an interactive scan (model 0.486,
    /// CLOCK 0.000). No test-only switch turns the gate off, so the CLOCK
    /// figure is the model's and is not rerun here.
    ///
    /// The aging window is ten lookups per slot and ghost record, above the
    /// 65,536 floor: 10,000 slots and 20,000 ghost records under a bulk-scan
    /// scan halve every 300,000 lookups, and 20,000 of each under an
    /// interactive scan every 400,000. Twenty rounds of 40,000 lookups put
    /// at least one halving inside the reported rounds of both variants.
    #[test]
    fn trace_b_keeps_the_interactive_set_through_a_scan() {
        const ROUNDS: usize = 20;
        for (scan, band) in [
            (AccessIntent::BulkScan, 0.90),
            (AccessIntent::Interactive, 0.40),
        ] {
            let (shares, t) = trace_b(scan, ROUNDS);
            let after_warmup = &shares[1..];
            let min = after_warmup.iter().copied().fold(f64::INFINITY, f64::min);
            let mean = after_warmup.iter().sum::<f64>() / after_warmup.len() as f64;
            println!(
                "trace B, {scan:?} scan: W hit share per round {shares:.3?}, \
                 after warmup min {min:.3} mean {mean:.3}; lookups {}, halvings at {:?}, \
                 {:?}",
                t.lookups,
                t.halvings,
                t.cache.process_counters(),
            );
            assert_eq!(shares[0], 0.0, "the first round is cold");
            assert!(
                min >= band,
                "{scan:?} scan: W retained {min:.3} on some round, band at least {band}"
            );
            let window = match scan {
                AccessIntent::Interactive => 400_000,
                _ => 300_000,
            };
            assert_eq!(t.halvings.first(), Some(&window), "{scan:?} scan");
        }
    }

    /// Trace E's registered row (decision 6): after A was read 15 times, the
    /// first pass of B at a 0.90 hit share is at most 13 (model 12). k = 4
    /// and k = 30 (model 6 and 9) are reported, not asserted. No test-only
    /// switch turns the gate off, so CLOCK's 3 passes are the model's.
    ///
    /// A's 15,000 slots set a 150,000-lookup aging window, so A's counts
    /// halve during its reads for k = 15 and 30, and every variant halves
    /// again while B displaces A.
    #[test]
    fn trace_e_a_shifted_working_set_displaces_a_stale_one() {
        const MAX_PASSES: usize = 25;
        let mut first = Vec::new();
        for k in [4, 15, 30] {
            let (shares, t) = trace_e(k, MAX_PASSES);
            let pass = first_pass_at_090(&shares);
            println!(
                "trace E, A read {k} times: B hit share per pass {shares:.3?}, \
                 first pass at 0.90 {pass:?}; lookups {}, halvings at {:?}, {:?}",
                t.lookups,
                t.halvings,
                t.cache.process_counters(),
            );
            assert!(
                t.halvings.first().is_some_and(|&l| l > 1 << 16),
                "k = {k}: the window, not its floor, sets the cadence"
            );
            let b_starts = k as u64 * 15_000;
            assert!(
                t.halvings.iter().any(|&l| l > b_starts),
                "k = {k}: counts halve while B is read"
            );
            first.push((k, pass));
        }
        let (_, at_15) = first[1];
        assert!(
            at_15.is_some_and(|p| p <= 13),
            "A read 15 times: first pass of B at 0.90 is {at_15:?}, band at most 13"
        );
    }
}
