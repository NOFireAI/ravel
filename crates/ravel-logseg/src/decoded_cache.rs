//! A process-wide cache of decoded columns, keyed per (tenant, object, block,
//! column, layout) (ADR-3326).
//!
//! An entry is one column of one block as [`decode_columns`] produced it,
//! shared by `Arc`: a hit clones handles, never data. Only a whole-block
//! decode on the columnar scan path reads or fills it. The row path neither
//! reads nor fills it.
//!
//! A column a Contains arm defers is looked up and offered only when its scan
//! was planned to fit: [`DecodedCacheAttach::serve_deferred`], set from the
//! plan pass's estimate of the scan's whole decoded set against 0.95 of the
//! cap (ADR-3326, serve-if-fits amendment). Such a column is offered before
//! its survivor narrowing, so an entry holds the whole column and only the
//! block's own copy is narrowed (`build_survivor_values` in `reader.rs`). In
//! a scan that does not fit it is never looked up or offered.
//! [`DecodedColumnCache::offer`] refuses a dictionary whose entries were not
//! validated, whatever the scan (`offer_refuses_an_unvalidated_dictionary`).
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
//! (decision 4). The reservation lives in a [`ReservationCell`] that the
//! entry, its string column, and every Arrow buffer owner a scan builds over
//! that column's buffers hold (ADR-3326, zero-copy values amendment, decision
//! 5), so it is released when the last of them drops: an entry a scan or a
//! batch still holds keeps its bytes charged after the index has let it go
//! (`the_reservation_is_released_by_the_last_handle_not_by_eviction`).
//! [`DecodedColumnCache::evict_for_pressure`] is the budget's evictor and
//! frees only entries nothing outside the cache holds: no scan holds the
//! entry, nothing beyond the entry holds the column object carrying its
//! reservation cell, and, when its column's [`ValuesMemo`] is filled, the memo
//! reports no buffer held outside it
//! (`pressure_eviction_counts_an_entry_whose_column_is_held_outside_as_held`).
//!
//! A cached string column whose text was validated carries a memo slot
//! ([`MemoSlot`]): `ravel-sql` fills it with an Arrow values array sharing
//! the column's buffers, through [`MemoSource`]. This crate names no Arrow
//! type; the slot holds a [`ValuesMemo`] trait object.
//!
//! Entries age out on the ravel-cache bound (ADR-0064): an entry older than
//! [`DEFAULT_MAX_ENTRY_AGE_NS`] is dropped at its first access, and
//! [`DecodedColumnCache::sweep_expired`], run every
//! [`DEFAULT_SWEEP_INTERVAL_NS`] by the server, drops the ones nobody reads.
//!
//! [`decode_columns`]: crate::block
//! [`DEFAULT_SWEEP_INTERVAL_NS`]: ravel_cache::DEFAULT_SWEEP_INTERVAL_NS

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use ravel_cache::{Clock, DEFAULT_MAX_ENTRY_AGE_NS, SystemClock};
use ravel_memory::{ExhaustionCause, MemoryBudget, Reservation};
use ravel_types::accounting::AccessIntent;

use crate::block::CachedColumn;
use crate::columns::ColumnSelection;

/// The most decoded-set estimates [`DecodedColumnCache`] memoises.
pub const ESTIMATE_MEMO_MAX_ENTRIES: usize = 65_536;

/// The most bytes the distinct selections keying [`DecodedColumnCache`]'s
/// estimate memo may hold, as [`estimate_memo_selection_bytes`] charges them.
pub const ESTIMATE_MEMO_MAX_SELECTION_BYTES: usize = 4 * 1024 * 1024;

/// Charged per distinct selection on top of the selection itself: its outer
/// map slot's inner map header and control byte.
const ESTIMATE_MEMO_SELECTION_NODE_BYTES: usize = 64;

/// Charged per element of a selection's `fixed` or `attrs` set on top of the
/// element itself: its share of the B-tree node.
const ESTIMATE_MEMO_SET_ELEMENT_BYTES: usize = 16;

/// The bytes the estimate memo charges for holding `columns` as a key:
/// [`ESTIMATE_MEMO_SELECTION_NODE_BYTES`] plus the selection's own size, 4
/// bytes plus [`ESTIMATE_MEMO_SET_ELEMENT_BYTES`] per fixed id, and per
/// attribute name its length plus a `String` plus
/// [`ESTIMATE_MEMO_SET_ELEMENT_BYTES`].
fn estimate_memo_selection_bytes(columns: &ColumnSelection) -> usize {
    let fixed = columns
        .fixed_ids()
        .len()
        .saturating_mul(std::mem::size_of::<u32>() + ESTIMATE_MEMO_SET_ELEMENT_BYTES);
    columns.attr_names().iter().fold(
        (std::mem::size_of::<ColumnSelection>() + ESTIMATE_MEMO_SELECTION_NODE_BYTES)
            .saturating_add(fixed),
        |sum, name| {
            sum.saturating_add(name.len())
                .saturating_add(std::mem::size_of::<String>() + ESTIMATE_MEMO_SET_ELEMENT_BYTES)
        },
    )
}

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

/// What a cached string column's memo is to the cache: a value built over the
/// column's own buffers by a crate this one does not depend on (`ravel-sql`'s
/// Arrow values array), held in the column's [`MemoSlot`] for as long as the
/// column lives (ADR-3326, zero-copy values amendment, decision 1).
pub trait ValuesMemo: Send + Sync {
    /// Whether any holder beyond the memo itself shares one of its buffers.
    /// [`DecodedColumnCache::evict_for_pressure`] counts an entry as held
    /// while this is true, since evicting it would free nothing.
    fn held_outside(&self) -> bool;

    /// The implementing type, for its crate to downcast to.
    fn as_any(&self) -> &dyn Any;
}

/// The bytes [`DecodedColumnCache::offer`] adds to a memo-capable column's
/// charge for the memo it may later hold: the Arrow array struct and its
/// buffer headers, which share the column's bytes rather than copy them. Set
/// above the amendment's inferred 200 to 500 bytes; not measured here.
/// `the_entry_charge_includes_the_memo_constant` pins that `offer` adds it.
pub const VALUES_MEMO_BYTES: u64 = 512;

/// A cached string column's memo slot, filled at most once.
#[derive(Default)]
pub struct MemoSlot(OnceLock<Arc<dyn ValuesMemo>>);

impl MemoSlot {
    /// The memo, once filled.
    pub fn get(&self) -> Option<&Arc<dyn ValuesMemo>> {
        self.0.get()
    }

    /// Whether the memo is filled and reports a buffer held outside it.
    pub(crate) fn held_outside(&self) -> bool {
        self.0.get().is_some_and(|m| m.held_outside())
    }

    /// Fills the slot for a test, returning `false` when it was already full.
    #[cfg(test)]
    pub(crate) fn fill_for_test(&self, memo: Arc<dyn ValuesMemo>) -> bool {
        self.0.set(memo).is_ok()
    }
}

impl std::fmt::Debug for MemoSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoSlot")
            .field("filled", &self.0.get().is_some())
            .finish()
    }
}

/// The budget reservation for one entry's bytes and nothing else, so no
/// holder reaches the column through it (ADR-3326, zero-copy values
/// amendment, decisions 3 and 5). Shared by the entry, its string column and
/// every buffer owner over that column's buffers; the reservation is released
/// when the last `Arc` to the cell drops. Filled by
/// [`DecodedColumnCache::offer`] only.
#[derive(Default)]
pub struct ReservationCell(OnceLock<Reservation>);

impl ReservationCell {
    /// Puts `reservation` in the cell, handing it back when the cell already
    /// holds one.
    fn fill(&self, reservation: Reservation) -> Result<(), Reservation> {
        self.0.set(reservation)
    }

    /// Whether the cell holds a reservation.
    pub fn is_filled(&self) -> bool {
        self.0.get().is_some()
    }
}

impl std::fmt::Debug for ReservationCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReservationCell")
            .field("filled", &self.is_filled())
            .finish()
    }
}

/// A cached string column's buffers, each behind its own `Arc`, for building
/// a [`ValuesMemo`] that shares them. No buffer holds the column, so a memo
/// stored in the column cannot keep it alive (decision 3).
#[derive(Clone, Debug)]
pub enum StrBuffers {
    /// A plain column's values and `rows + 1` `u32` offsets, every row
    /// present ([`MemoSource::all_present`] says when).
    Plain {
        values: Arc<String>,
        offsets: Arc<Vec<u32>>,
    },
    /// A dictionary's UTF-8 text and `len + 1` `i32` offsets into it, with
    /// each entry's validity when some entry is not UTF-8 (a non-UTF-8
    /// entry's span is empty).
    Dict {
        values: Arc<String>,
        offsets: Arc<Vec<i32>>,
        valid: Option<Arc<crate::dict_values::Validity>>,
    },
}

/// The memo slot of one string column a decoded-cache entry holds, served or
/// just admitted, with the buffers a memo shares
/// ([`ColumnarBlockView::str_values_memo`](crate::ColumnarBlockView::str_values_memo)).
pub struct MemoSource<'a> {
    slot: &'a MemoSlot,
    cell: &'a Arc<ReservationCell>,
    buffers: MemoBuffers<'a>,
}

/// Where [`MemoSource`] reads its buffers from.
pub(crate) enum MemoBuffers<'a> {
    Plain(&'a crate::block::PlainColumn),
    Dict(&'a crate::dict_values::DictValues),
}

impl<'a> MemoSource<'a> {
    pub(crate) fn new(
        slot: &'a MemoSlot,
        cell: &'a Arc<ReservationCell>,
        buffers: MemoBuffers<'a>,
    ) -> Self {
        MemoSource {
            slot,
            cell,
            buffers,
        }
    }

    /// The memo, once filled.
    pub fn get(&self) -> Option<&'a Arc<dyn ValuesMemo>> {
        self.slot.get()
    }

    /// The memo, built by `build` from the column's buffers and its entry's
    /// reservation cell when the slot is empty. Concurrent callers wait for
    /// the first build (`OnceLock::get_or_init`), so each column builds once.
    pub fn get_or_build(
        &self,
        build: impl FnOnce(StrBuffers, Arc<ReservationCell>) -> Arc<dyn ValuesMemo>,
    ) -> &'a Arc<dyn ValuesMemo> {
        self.slot
            .0
            .get_or_init(|| build(self.buffers.shared(), Arc::clone(self.cell)))
    }

    /// Whether this is a plain column (a dictionary's otherwise).
    pub fn is_plain(&self) -> bool {
        matches!(self.buffers, MemoBuffers::Plain(_))
    }

    /// Whether every row of a plain column is present, by a popcount of its
    /// presence bitmap; `false` for a dictionary.
    pub fn all_present(&self) -> bool {
        match self.buffers {
            MemoBuffers::Plain(p) => p.all_present(),
            MemoBuffers::Dict(_) => false,
        }
    }

    /// A plain column's last offset, the total of its value bytes; `None` for
    /// a dictionary.
    pub fn last_offset(&self) -> Option<u32> {
        match self.buffers {
            MemoBuffers::Plain(p) => Some(p.last_offset()),
            MemoBuffers::Dict(_) => None,
        }
    }
}

impl MemoBuffers<'_> {
    fn shared(&self) -> StrBuffers {
        match self {
            MemoBuffers::Plain(p) => p.shared_buffers(),
            MemoBuffers::Dict(d) => d.shared_buffers(),
        }
    }
}

/// One cached column and the cell holding the budget reservation for its
/// bytes. Dropping the last `Arc` to the entry drops its handle on the cell;
/// the reservation goes with the cell's last holder.
pub struct DecodedEntry {
    col: CachedColumn,
    bytes: u64,
    inserted_ns: u64,
    cell: Arc<ReservationCell>,
}

impl DecodedEntry {
    pub(crate) fn column(&self) -> &CachedColumn {
        &self.col
    }

    /// The bytes the entry is charged.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The cell holding the entry's reservation.
    pub fn reservation_cell(&self) -> &Arc<ReservationCell> {
        &self.cell
    }

    /// Whether something outside the cache holds the entry: a handle beyond
    /// the index's own, a handle beyond the entry's on the column object that
    /// shares its reservation cell, or a buffer of its column's memo. Each of
    /// them keeps the reservation alive, so removing the entry would free
    /// nothing (`pressure_eviction_counts_an_entry_whose_column_is_held_outside_as_held`).
    fn held(entry: &Arc<DecodedEntry>) -> bool {
        Arc::strong_count(entry) > 1
            || entry.col.cell_column_held_outside()
            || entry.col.memo_held_outside()
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
    unvalidated_refusals: AtomicU64,
    filled_cell_refusals: AtomicU64,
    deferred_set_estimate_bytes: AtomicU64,
    deferred_estimated: AtomicBool,
    deferred_served: AtomicBool,
    deferred_partial: AtomicBool,
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
    /// Offers refused because the column was a dictionary whose entries were
    /// not validated (ADR-3326, LIKE-deferred amendment, decision 4).
    pub unvalidated_refusals: u64,
    /// Offers refused because the column already carries a filled
    /// reservation cell: it was admitted concurrently under another key, or
    /// its entry was evicted while something still held the column
    /// (ADR-3326, zero-copy values amendment, decision 5).
    pub filled_cell_refusals: u64,
    /// The plan-time estimate of the decoded set of the statement's scans
    /// that carry a Contains arm, summed over those scans; `None` when no
    /// such scan made a verdict (ADR-3326, serve-if-fits amendment). The SQL
    /// logs scan makes one in its plan pass, and on the whole-segment fast
    /// path in `compute_fast_path_verdict`.
    pub deferred_set_estimate_bytes: Option<u64>,
    /// Whether a scan of the statement serves its Contains-deferred columns
    /// from the cache, its estimate being at most 0.95 of the cap.
    pub deferred_served: bool,
    /// Whether a scan of the statement stopped reading segment directories
    /// once its running estimate passed 0.95 of the cap, so that
    /// `deferred_set_estimate_bytes` is a lower bound of its decoded set.
    pub deferred_set_estimate_partial: bool,
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
            unvalidated_refusals: self
                .unvalidated_refusals
                .saturating_add(other.unvalidated_refusals),
            filled_cell_refusals: self
                .filled_cell_refusals
                .saturating_add(other.filled_cell_refusals),
            deferred_set_estimate_bytes: match (
                self.deferred_set_estimate_bytes,
                other.deferred_set_estimate_bytes,
            ) {
                (Some(a), Some(b)) => Some(a.saturating_add(b)),
                (a, b) => a.or(b),
            },
            deferred_served: self.deferred_served || other.deferred_served,
            deferred_set_estimate_partial: self.deferred_set_estimate_partial
                || other.deferred_set_estimate_partial,
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
            unvalidated_refusals: self.unvalidated_refusals.load(Ordering::Relaxed),
            filled_cell_refusals: self.filled_cell_refusals.load(Ordering::Relaxed),
            deferred_set_estimate_bytes: self
                .deferred_estimated
                .load(Ordering::Relaxed)
                .then(|| self.deferred_set_estimate_bytes.load(Ordering::Relaxed)),
            deferred_served: self.deferred_served.load(Ordering::Relaxed),
            deferred_set_estimate_partial: self.deferred_partial.load(Ordering::Relaxed),
        }
    }

    /// Records one scan's plan-time decoded-set estimate and whether that
    /// scan serves its Contains-deferred columns (ADR-3326, serve-if-fits
    /// amendment). Called once per scan with a Contains arm, by its plan pass
    /// or, on the whole-segment fast path, by its directory-read pass.
    /// `partial` is whether that pass stopped before reading every segment,
    /// making `estimate_bytes` a lower bound.
    pub fn record_deferred_verdict(&self, estimate_bytes: u64, served: bool, partial: bool) {
        Self::bump(&self.deferred_set_estimate_bytes, estimate_bytes);
        self.deferred_estimated.store(true, Ordering::Relaxed);
        if served {
            self.deferred_served.store(true, Ordering::Relaxed);
        }
        if partial {
            self.deferred_partial.store(true, Ordering::Relaxed);
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
    /// The admission rule by intent (ADR-3326 decision 3): an interactive
    /// statement admits on first decode, any other on the touch after its
    /// ghost record.
    fn intent_admits(&self, key: &DecodedKey, intent: AccessIntent) -> bool {
        intent == AccessIntent::Interactive || self.ghost.contains_key(key)
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

/// Memoised whole-segment decoded-set estimates, by projection and then by
/// the (tenant, object) of [`DecodedCacheAttach::key`]. `len` counts the
/// (tenant, object, projection) entries over every projection;
/// `selection_bytes` sums [`estimate_memo_selection_bytes`] over the keys of
/// `by_columns`.
#[derive(Default)]
struct EstimateMemo {
    by_columns: HashMap<ColumnSelection, HashMap<ObjectId, u64>>,
    len: usize,
    selection_bytes: usize,
}

impl EstimateMemo {
    fn clear(&mut self) {
        self.by_columns.clear();
        self.len = 0;
        self.selection_bytes = 0;
    }
}

/// The (tenant, object) of [`DecodedCacheAttach::key`].
type ObjectId = ([u8; 16], [u8; 32]);

/// The process-wide decoded-column cache.
pub struct DecodedColumnCache {
    max_bytes: u64,
    max_entry_age_ns: u64,
    budget: Option<Arc<MemoryBudget>>,
    clock: Arc<dyn Clock>,
    inner: Mutex<Inner>,
    estimates: Mutex<EstimateMemo>,
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
            estimates: Mutex::new(EstimateMemo::default()),
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

    fn lock_estimates(&self) -> MutexGuard<'_, EstimateMemo> {
        self.estimates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The memoised whole-segment decoded-set estimate of `object` under
    /// `columns` (ADR-3326, serve-if-fits amendment), keyed by the tenant and
    /// object of [`DecodedCacheAttach::key`] and the whole selection. The
    /// estimate is a function of the immutable object and the selection
    /// alone, so an entry never goes stale.
    ///
    /// The memo holds at most [`ESTIMATE_MEMO_MAX_ENTRIES`] entries, 56 bytes
    /// of key and value each, plus at most
    /// [`ESTIMATE_MEMO_MAX_SELECTION_BYTES`] (4 MiB) of selection keys as
    /// [`estimate_memo_selection_bytes`] charges them: 3,670,016 + 4,194,304
    /// = 7,864,320 bytes (7.5 MiB) at both bounds, before the entries' map
    /// overhead. Neither is charged to `max_bytes` or the memory budget.
    pub fn memoised_estimate(
        &self,
        tenant: [u8; 16],
        object: [u8; 32],
        columns: &ColumnSelection,
    ) -> Option<u64> {
        self.lock_estimates()
            .by_columns
            .get(columns)?
            .get(&(tenant, object))
            .copied()
    }

    /// Memoises `bytes` as `object`'s estimate under `columns`
    /// ([`Self::memoised_estimate`]). A new entry that would pass
    /// [`ESTIMATE_MEMO_MAX_ENTRIES`], or whose selection is not yet a key and
    /// would take the keys past [`ESTIMATE_MEMO_MAX_SELECTION_BYTES`], clears
    /// the whole memo first. A selection charged above
    /// [`ESTIMATE_MEMO_MAX_SELECTION_BYTES`] on its own is never memoised.
    pub fn memoise_estimate(
        &self,
        tenant: [u8; 16],
        object: [u8; 32],
        columns: &ColumnSelection,
        bytes: u64,
    ) {
        let mut memo = self.lock_estimates();
        if let Some(slot) = memo
            .by_columns
            .get_mut(columns)
            .and_then(|by_object| by_object.get_mut(&(tenant, object)))
        {
            *slot = bytes;
            return;
        }
        // A selection already keyed was charged, and admitted, when its first
        // entry went in.
        let mut charge = if memo.by_columns.contains_key(columns) {
            None
        } else {
            let charge = estimate_memo_selection_bytes(columns);
            if charge > ESTIMATE_MEMO_MAX_SELECTION_BYTES {
                return;
            }
            Some(charge)
        };
        if memo.len >= ESTIMATE_MEMO_MAX_ENTRIES
            || memo.selection_bytes.saturating_add(charge.unwrap_or(0))
                > ESTIMATE_MEMO_MAX_SELECTION_BYTES
        {
            memo.clear();
            charge = Some(charge.unwrap_or_else(|| estimate_memo_selection_bytes(columns)));
        }
        memo.selection_bytes += charge.unwrap_or(0);
        memo.len += 1;
        memo.by_columns
            .entry(columns.clone())
            .or_default()
            .insert((tenant, object), bytes);
    }

    /// Entries the estimate memo holds.
    pub fn memoised_estimates(&self) -> usize {
        self.lock_estimates().len
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
    /// order, entries nothing outside the cache holds until `want` bytes are
    /// freed or none remains, and returns the bytes freed. An entry a scan
    /// still holds, whose column object carrying its reservation cell has a
    /// holder beyond the entry, or whose column's memo reports a buffer
    /// held outside it ([`ValuesMemo::held_outside`]), stays, and its bytes
    /// count for nothing: its reservation would not be released by removing
    /// it from the index ([`DecodedEntry::held`];
    /// `pressure_eviction_counts_an_entry_whose_column_is_held_outside_as_held`).
    /// Each pass logs one INFO line;
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
                if DecodedEntry::held(&slot.entry) {
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
        // `held` passed them over, so the index held each entry's last handle
        // and nothing else held its cell's column or a memo buffer: dropping
        // them here, outside the lock, releases their reservations.
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

    /// Whether an [`offer`](Self::offer) of `key` under `intent` would pass
    /// the intent rule now: an interactive intent, or any other intent whose
    /// key holds a ghost record. Reads the same [`Inner::intent_admits`] check
    /// `offer` applies and changes nothing, so the ghost record stays for the
    /// offer to spend. A size, budget or concurrent-admission refusal in
    /// `offer` can still turn a `true` into no admission.
    pub(crate) fn would_admit(&self, key: &DecodedKey, intent: AccessIntent) -> bool {
        self.lock().intent_admits(key, intent)
    }

    /// Offers a freshly decoded column for admission under `intent`'s rule,
    /// returning the admitted entry so the caller can hold it for as long as
    /// it uses the column. A dictionary whose entries were not validated is
    /// refused after the intent rule, so a non-interactive first touch still
    /// records its ghost record, and counted in `unvalidated_refusals`.
    pub(crate) fn offer(
        &self,
        key: DecodedKey,
        col: &CachedColumn,
        intent: AccessIntent,
        counters: &DecodedCacheCounters,
    ) -> Option<Arc<DecodedEntry>> {
        // The memo a string column may hold later is charged now, since the
        // charge is fixed here (ADR-3326, zero-copy values amendment).
        let memo = if col.memo_capable() {
            VALUES_MEMO_BYTES
        } else {
            0
        };
        let bytes = (col.heap_bytes() as u64).saturating_add(memo);
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
                self.record_ghost(&mut inner, key, bytes);
                return None;
            }
            // A dictionary built unvalidated marks no entry valid, so a later
            // scan without the Contains arm would read every row as absent.
            if col.is_unvalidated_dict() {
                inner.process.unvalidated_refusals += 1;
                DecodedCacheCounters::bump(&counters.unvalidated_refusals, 1);
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
            // The column's own cell when it has one, so the Arrow buffer
            // owners a scan builds over its buffers hold the reservation too.
            let cell = col.reservation_cell().unwrap_or_default();
            if let Some(reservation) = reservation
                && cell.fill(reservation).is_err()
            {
                // The column already carries a reservation: it was admitted
                // concurrently under another key, or its entry was evicted
                // while something outside the cache still held the column.
                // The new reservation drops here and nothing is admitted.
                inner.process.filled_cell_refusals += 1;
                DecodedCacheCounters::bump(&counters.filled_cell_refusals, 1);
                return None;
            }
            // Entries admitted between the two locks may have refilled the cap.
            self.evict_for(&mut inner, bytes, &mut evicted, counters);
            let entry = Arc::new(DecodedEntry {
                col: col.clone(),
                bytes,
                inserted_ns: self.clock.now_ns(),
                cell,
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
    /// Whether the scan's Contains-deferred columns are looked up and
    /// offered too: true only when the plan pass estimated the scan's whole
    /// decoded set at most 0.95 of the cap (ADR-3326, serve-if-fits
    /// amendment). Read once per block by `BlockScan::decode_block`.
    pub serve_deferred: bool,
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

    /// ADR-3326, LIKE-deferred amendment, decision 4: an unvalidated
    /// dictionary offered under an intent that admits is not admitted, and the
    /// refusal is counted; the same dictionary built validated is admitted. A
    /// non-interactive first touch of the unvalidated one still records its
    /// ghost record, since the intent rule runs first.
    #[test]
    fn offer_refuses_an_unvalidated_dictionary() {
        let entries = vec![b"a".to_vec(), b"b".to_vec()];
        let raw = CachedColumn::dict_for_test(&entries, &[0, 1, 1, 0], false).expect("raw");
        let checked = CachedColumn::dict_for_test(&entries, &[0, 1, 1, 0], true).expect("valid");
        assert!(raw.is_unvalidated_dict());
        assert!(!checked.is_unvalidated_dict());

        let cache = DecodedColumnCache::new(1 << 20, None);
        let c = DecodedCacheCounters::default();
        assert!(
            cache
                .offer(key(0, 1), &raw, AccessIntent::Interactive, &c)
                .is_none()
        );
        assert!(cache.is_empty(), "no entry");
        let s = c.snapshot();
        assert_eq!((s.unvalidated_refusals, s.admissions), (1, 0));
        assert_eq!(cache.process_counters().unvalidated_refusals, 1);

        assert!(
            cache
                .offer(key(0, 1), &checked, AccessIntent::Interactive, &c)
                .is_some()
        );
        assert_eq!(c.snapshot().admissions, 1);

        // Bulk scan: the first touch only ghosts and is not a refusal; the
        // second passes the intent rule and is refused for the dictionary.
        let bulk = DecodedCacheCounters::default();
        assert!(!cache.would_admit(&key(1, 1), AccessIntent::BulkScan));
        assert!(
            cache
                .offer(key(1, 1), &raw, AccessIntent::BulkScan, &bulk)
                .is_none()
        );
        assert_eq!(bulk.snapshot().unvalidated_refusals, 0);
        assert!(cache.would_admit(&key(1, 1), AccessIntent::BulkScan));
        assert!(
            cache
                .offer(key(1, 1), &raw, AccessIntent::BulkScan, &bulk)
                .is_none()
        );
        assert_eq!(bulk.snapshot().unvalidated_refusals, 1);
        assert_eq!(cache.len(), 1);
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
    /// every intent with and without a ghost record, and asking does not
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

    /// The reservation travels with the bytes (ADR-3326 decision 4, and the
    /// zero-copy values amendment's decision 5, which moves it into the
    /// [`ReservationCell`]): an entry a scan holds past its eviction from the
    /// index keeps its bytes reserved until the scan's handle, the last one,
    /// drops. A second case, a plain string column whose cell an outside
    /// holder shares (what an Arrow buffer owner over the column's buffers
    /// is; `ravel-sql`'s
    /// `an_arrow_array_is_the_last_holder_of_an_evicted_entrys_reservation`
    /// runs it with a real Arrow array), keeps the bytes reserved after the
    /// entry and the column are gone, until that holder drops.
    ///
    /// FLIP: move the reservation from the cell into `Slot` and the budget
    /// reads `bytes` short right after the eviction; fill a fresh cell in
    /// `offer` instead of the column's and the plain case fails "one cell".
    #[test]
    fn the_reservation_is_released_by_the_last_handle_not_by_eviction() {
        // An Arrow buffer owner holds the cell and the column's buffers, never
        // the column or the entry.
        {
            let v = CachedColumn::plain_for_test(&[Some(b"ab".to_vec()), Some(b"c".to_vec())])
                .expect("plain");
            let budget = Arc::new(MemoryBudget::new(4 << 30));
            let bytes = v.heap_bytes() as u64 + VALUES_MEMO_BYTES;
            let cache = DecodedColumnCache::new(bytes, Some(Arc::clone(&budget)));
            let c = DecodedCacheCounters::default();
            let entry = cache
                .offer(key(0, 1), &v, AccessIntent::Interactive, &c)
                .expect("admitted");
            assert_eq!(entry.bytes(), bytes);
            let owner = Arc::clone(v.reservation_cell().as_ref().expect("plain has a cell"));
            assert!(Arc::ptr_eq(&owner, entry.reservation_cell()), "one cell");
            drop((v, entry));

            // Admitting a second entry evicts the first; the index held its
            // last handle, and the column goes with it.
            let second = cache
                .offer(key(1, 1), &col(8), AccessIntent::Interactive, &c)
                .expect("admitted");
            let second_bytes = second.bytes();
            drop(second);
            assert_eq!(c.snapshot().evictions, 1);
            assert!(cache.lookup(&key(0, 1), 2, &c).is_none(), "evicted");
            assert_eq!(
                budget.reserved(),
                bytes + second_bytes,
                "the outside holder of the cell keeps the bytes reserved"
            );
            drop(owner);
            assert_eq!(
                budget.reserved(),
                second_bytes,
                "the last holder released it"
            );
        }
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

    /// A memo whose outside-held answer the test sets.
    struct FlagMemo(Arc<AtomicBool>);

    impl ValuesMemo for FlagMemo {
        fn held_outside(&self) -> bool {
            self.0.load(Ordering::Relaxed)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn plain(cells: &[&[u8]]) -> CachedColumn {
        let cells: Vec<Option<Vec<u8>>> = cells.iter().map(|c| Some(c.to_vec())).collect();
        CachedColumn::plain_for_test(&cells).expect("plain")
    }

    /// ADR-3326, zero-copy values amendment, "Memory": `offer` charges a
    /// column that can hold a memo its `heap_bytes` plus
    /// [`VALUES_MEMO_BYTES`], and the reservation and the resident total
    /// carry the same figure. A validated plain column and a block's own
    /// validated dictionary can hold one; an integer column, a plain column
    /// with a non-UTF-8 cell and an unvalidated dictionary cannot, and are
    /// charged `heap_bytes` alone.
    ///
    /// FLIP: drop the constant from `offer` and the first assertion reads
    /// `heap_bytes` for the plain column.
    #[test]
    fn the_entry_charge_includes_the_memo_constant() {
        let entries = vec![b"a".to_vec(), b"bc".to_vec()];
        let raw_plain = CachedColumn::plain_for_test(&[Some(vec![b'a', 0xff])]).expect("plain");
        let cases = [
            (plain(&[b"ab", b"", b"c"]), true),
            (
                CachedColumn::dict_for_test(&entries, &[0, 1, 1], true).expect("dict"),
                true,
            ),
            (col(8), false),
            (raw_plain, false),
        ];
        for (i, (v, memo)) in cases.iter().enumerate() {
            assert_eq!(v.memo_capable(), *memo, "case {i}");
            let budget = Arc::new(MemoryBudget::new(4 << 30));
            let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
            let c = DecodedCacheCounters::default();
            let entry = cache
                .offer(key(0, 1), v, AccessIntent::Interactive, &c)
                .expect("admitted");
            let want = v.heap_bytes() as u64 + if *memo { VALUES_MEMO_BYTES } else { 0 };
            assert_eq!(entry.bytes(), want, "case {i}");
            assert_eq!(budget.reserved(), want, "case {i}");
            assert_eq!(cache.resident_bytes(), want, "case {i}");
        }
    }

    /// ADR-3326, zero-copy values amendment, decision 5: the pressure pass
    /// counts an entry as held while its column's memo reports a buffer held
    /// outside it, though no scan holds the entry, and frees it once the memo
    /// reports none.
    ///
    /// FLIP: drop the `memo_held_outside` term from `DecodedEntry::held` and
    /// the first pass frees the entry's bytes.
    #[test]
    fn pressure_eviction_counts_an_entry_whose_memo_is_held_outside_as_held() {
        let v = plain(&[b"ab", b"c"]);
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        let bytes = cache
            .offer(key(0, 1), &v, AccessIntent::Interactive, &c)
            .expect("admitted")
            .bytes();
        let flag = Arc::new(AtomicBool::new(true));
        assert!(
            v.memo_slot()
                .expect("a validated plain column has a slot")
                .fill_for_test(Arc::new(FlagMemo(Arc::clone(&flag))))
        );
        drop(v);

        assert_eq!(
            cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted),
            0,
            "held through its memo"
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(budget.reserved(), bytes);

        flag.store(false, Ordering::Relaxed);
        assert_eq!(
            cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted),
            bytes
        );
        assert!(cache.is_empty());
        assert_eq!(budget.reserved(), 0);
    }

    /// ADR-3326, zero-copy values amendment, decision 5: the column object
    /// that carries an entry's reservation cell keeps the reservation alive,
    /// so the pressure pass counts the entry as held while anything beyond
    /// the entry holds that column, with no scan holding the entry and no
    /// memo built: a holder of a block's own dictionary's `DictValues`, as a
    /// scan's `DictCache` holds a row group's, and a holder of a plain
    /// column. Nothing is reported freed
    /// for it, the resident total does not drop, and the budget's reserved
    /// figure matches it. Once the outside holder drops, the pass frees it.
    ///
    /// FLIP: drop the `cell_column_held_outside` term from
    /// `DecodedEntry::held` and the first pass of each case reports the
    /// entry's bytes freed ("held through its column").
    #[test]
    fn pressure_eviction_counts_an_entry_whose_column_is_held_outside_as_held() {
        let entries = vec![b"a".to_vec(), b"bc".to_vec()];
        let dict = CachedColumn::dict_for_test(&entries, &[0, 1, 1], true).expect("dict");
        let dict_holder: Box<dyn Any> =
            Box::new(dict.dict_values_for_test().expect("a dictionary column"));
        let plain_col = plain(&[b"ab", b"c"]);
        let plain_holder: Box<dyn Any> = Box::new(plain_col.clone());
        for (case, v, holder) in [
            ("dict", dict, dict_holder),
            ("plain", plain_col, plain_holder),
        ] {
            let budget = Arc::new(MemoryBudget::new(4 << 30));
            let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
            let c = DecodedCacheCounters::default();
            let bytes = cache
                .offer(key(0, 1), &v, AccessIntent::Interactive, &c)
                .expect("admitted")
                .bytes();
            assert!(v.memo_slot().expect("memo capable").get().is_none());
            drop(v);

            assert_eq!(
                cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted),
                0,
                "{case}: held through its column"
            );
            assert_eq!(cache.len(), 1, "{case}");
            assert_eq!(cache.resident_bytes(), bytes, "{case}");
            assert_eq!(budget.reserved(), bytes, "{case}");
            assert_eq!(cache.pressure_counters().freed_bytes, 0, "{case}");

            drop(holder);
            assert_eq!(
                cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted),
                bytes,
                "{case}"
            );
            assert!(cache.is_empty(), "{case}");
            assert_eq!(budget.reserved(), 0, "{case}");
        }
    }

    /// An offer of a column whose reservation cell is already filled is
    /// refused and counted in `filled_cell_refusals`, on the statement's
    /// counters and the process's, and the reservation it took is released:
    /// here the same column object offered under a second key.
    ///
    /// FLIP: drop the two bumps from the `cell.fill` failure arm of `offer`
    /// and both counters read 0.
    #[test]
    fn an_offer_of_a_column_with_a_filled_cell_is_refused_and_counted() {
        let v = plain(&[b"ab", b"c"]);
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)));
        let c = DecodedCacheCounters::default();
        let bytes = cache
            .offer(key(0, 1), &v, AccessIntent::Interactive, &c)
            .expect("admitted")
            .bytes();
        assert!(
            cache
                .offer(key(1, 1), &v, AccessIntent::Interactive, &c)
                .is_none()
        );
        let s = c.snapshot();
        assert_eq!((s.filled_cell_refusals, s.admissions), (1, 1));
        assert_eq!(cache.process_counters().filled_cell_refusals, 1);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.resident_bytes(), bytes);
        assert_eq!(budget.reserved(), bytes, "nothing extra stays reserved");
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

    #[test]
    fn the_estimate_memo_keys_on_the_whole_selection_and_clears_at_its_bound() {
        let cache = DecodedColumnCache::new(1 << 20, None);
        let narrow = ColumnSelection::fixed_only().with_attr("d");
        let wide = ColumnSelection::fixed_only().with_attr("d").with_attr("p");
        cache.memoise_estimate([1; 16], [2; 32], &narrow, 10);
        assert_eq!(cache.memoised_estimate([1; 16], [2; 32], &narrow), Some(10));
        assert_eq!(cache.memoised_estimate([1; 16], [2; 32], &wide), None);
        assert_eq!(cache.memoised_estimate([9; 16], [2; 32], &narrow), None);
        assert_eq!(cache.memoised_estimate([1; 16], [9; 32], &narrow), None);
        cache.memoise_estimate([1; 16], [2; 32], &narrow, 10);
        assert_eq!(cache.memoised_estimates(), 1, "a re-insert adds nothing");

        for i in 1..ESTIMATE_MEMO_MAX_ENTRIES {
            let mut object = [0u8; 32];
            object[..8].copy_from_slice(&(i as u64).to_le_bytes());
            cache.memoise_estimate([1; 16], object, &wide, 1);
        }
        assert_eq!(cache.memoised_estimates(), ESTIMATE_MEMO_MAX_ENTRIES);
        assert_eq!(cache.memoised_estimate([1; 16], [2; 32], &narrow), Some(10));
        cache.memoise_estimate([1; 16], [3; 32], &narrow, 7);
        assert_eq!(
            cache.memoised_estimates(),
            1,
            "the new entry cleared the memo"
        );
        assert_eq!(cache.memoised_estimate([1; 16], [2; 32], &narrow), None);
        assert_eq!(cache.memoised_estimate([1; 16], [3; 32], &narrow), Some(7));
        assert_eq!(cache.resident_bytes(), 0, "the memo charges no cache bytes");
    }

    #[test]
    fn the_estimate_memo_charges_a_selection_its_ids_and_names() {
        let selection = ColumnSelection::fixed_only().with_body().with_attr("abc");
        assert_eq!(
            estimate_memo_selection_bytes(&selection),
            std::mem::size_of::<ColumnSelection>() + 64 + (4 + 16) + (3 + 24 + 16)
        );
    }

    #[test]
    fn the_estimate_memo_never_holds_a_selection_above_the_key_bound() {
        let cache = DecodedColumnCache::new(1 << 20, None);
        let small = ColumnSelection::fixed_only().with_attr("d");
        cache.memoise_estimate([1; 16], [2; 32], &small, 10);
        let huge =
            ColumnSelection::fixed_only().with_attr("x".repeat(ESTIMATE_MEMO_MAX_SELECTION_BYTES));
        cache.memoise_estimate([1; 16], [3; 32], &huge, 7);
        assert_eq!(cache.memoised_estimate([1; 16], [3; 32], &huge), None);
        assert_eq!(
            cache.memoised_estimates(),
            1,
            "the huge selection added nothing"
        );
        assert_eq!(
            cache.memoised_estimate([1; 16], [2; 32], &small),
            Some(10),
            "the huge selection cleared nothing"
        );

        let fresh = DecodedColumnCache::new(1 << 20, None);
        fresh.memoise_estimate([1; 16], [3; 32], &huge, 7);
        assert_eq!(fresh.memoised_estimate([1; 16], [3; 32], &huge), None);
        assert_eq!(fresh.memoised_estimates(), 0);
        assert_eq!(fresh.lock_estimates().selection_bytes, 0);
    }

    #[test]
    fn the_estimate_memo_clears_when_its_selection_keys_would_pass_the_byte_bound() {
        let cache = DecodedColumnCache::new(1 << 20, None);
        let selection = |i: usize| ColumnSelection::fixed_only().with_attr(format!("{i:0>1000}"));
        let charge = estimate_memo_selection_bytes(&selection(0));
        let fit = ESTIMATE_MEMO_MAX_SELECTION_BYTES / charge;
        assert!(
            fit < ESTIMATE_MEMO_MAX_ENTRIES / 10,
            "far below the entry bound"
        );
        for i in 0..fit {
            cache.memoise_estimate([1; 16], [2; 32], &selection(i), i as u64);
        }
        assert_eq!(cache.memoised_estimates(), fit);
        assert_eq!(cache.lock_estimates().selection_bytes, fit * charge);
        cache.memoise_estimate([1; 16], [3; 32], &selection(0), 99);
        assert_eq!(
            cache.memoised_estimates(),
            fit + 1,
            "a known selection adds no key bytes"
        );
        assert_eq!(
            cache.memoised_estimate([1; 16], [2; 32], &selection(0)),
            Some(0)
        );

        cache.memoise_estimate([1; 16], [2; 32], &selection(fit), 5);
        assert_eq!(
            cache.memoised_estimates(),
            1,
            "the new selection cleared the memo"
        );
        assert_eq!(
            cache.memoised_estimate([1; 16], [2; 32], &selection(0)),
            None
        );
        assert_eq!(
            cache.memoised_estimate([1; 16], [2; 32], &selection(fit)),
            Some(5)
        );
        assert_eq!(cache.lock_estimates().selection_bytes, charge);
    }
}
