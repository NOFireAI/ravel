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
//! type; the slot holds a [`ValuesMemo`] trait object. A memo's views are
//! charged to a view budget of their own, not to the data cap
//! ([`DecodedColumnCache::reserve_view`];
//! `a_view_fill_is_charged_to_the_view_budget_not_the_data_cap`), and the
//! pressure evictor releases unheld views before any data entry
//! (`pressure_releases_unheld_views_before_any_data_entry`).
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

    /// The view-budget bytes the memo holds beyond the column's own: its
    /// views, as [`DecodedColumnCache::reserve_view`] charged them (ADR-3326,
    /// view memo amendment, decision 7). `Inner::remove` subtracts this from
    /// the view total when the entry leaves the index
    /// (`removing_an_entry_drops_its_views_and_lowers_the_view_total`).
    fn extra_bytes(&self) -> u64 {
        0
    }

    /// Drops the extra bytes when nothing outside the memo holds them and
    /// returns how many it dropped; returns 0, keeping them, while a holder
    /// shares them. [`DecodedColumnCache::evict_for_pressure`] calls it before
    /// it sweeps data entries (`pressure_releases_unheld_views_before_any_data_entry`).
    fn release_extra(&self) -> u64 {
        0
    }
}

/// The bytes [`DecodedColumnCache::offer`] adds to a memo-capable column's
/// charge for the memo it may later hold: the memo struct, the Arrow array
/// and its buffer headers, and a dictionary's keys buffer, all of which share
/// the column's bytes rather than copy them. Measured by `ravel-sql`'s
/// `tests/memo_overhead_alloc_pin.rs`
/// (`values_memo_charge_covers_the_measured_memo_and_no_more`), through the
/// crate's own build, as the heap bytes one memo fill leaves live: 464 for a
/// plain `Text` column; 464 for a block-local `Text` dictionary, and 600 once
/// the key path has also built its keys buffer; 592 for a `Mixed`
/// dictionary, whose validity adds a shared buffer, both with and without the
/// key path, which refuses it and builds no keys buffer. Those include the
/// memo's two empty view memo slots (ADR-3326, view memo amendment); views
/// themselves are charged to the view budget, not here. Every byte allocated
/// stays live. Set to the largest, rounded up to a multiple of 16; that test
/// fails when the constant falls below a measured figure or exceeds the
/// largest by more than 16. `the_entry_charge_includes_the_memo_constant`
/// pins that `offer` adds it.
pub const VALUES_MEMO_BYTES: u64 = 608;

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

    /// The filled memo's [`ValuesMemo::extra_bytes`], 0 when empty.
    fn extra_bytes(&self) -> u64 {
        self.0.get().map_or(0, |m| m.extra_bytes())
    }

    /// The filled memo's [`ValuesMemo::release_extra`], 0 when empty.
    fn release_extra(&self) -> u64 {
        self.0.get().map_or(0, |m| m.release_extra())
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
    served: bool,
    view: Option<ViewHandle>,
}

/// What a memo needs to charge views to the view budget of the cache that
/// holds its entry: the cache, weakly, the entry's key, and the counters of
/// the statement whose scan read the column (ADR-3326, view memo amendment,
/// decision 5). [`Self::reserve_view`] is [`DecodedColumnCache::reserve_view`]
/// for that entry.
#[derive(Clone, Debug)]
pub struct ViewHandle {
    cache: std::sync::Weak<DecodedColumnCache>,
    key: DecodedKey,
    counters: Arc<DecodedCacheCounters>,
}

impl ViewHandle {
    pub(crate) fn new(
        cache: &Arc<DecodedColumnCache>,
        key: DecodedKey,
        counters: Arc<DecodedCacheCounters>,
    ) -> Self {
        ViewHandle {
            cache: Arc::downgrade(cache),
            key,
            counters,
        }
    }

    /// The cache's view budget, 0 once the cache is gone.
    pub fn view_max_bytes(&self) -> u64 {
        self.cache.upgrade().map_or(0, |c| c.view_max_bytes())
    }

    /// [`DecodedColumnCache::reserve_view`] for the handle's entry, with the
    /// same lock-order rules; `None` once the cache is gone.
    pub fn reserve_view<T>(
        &self,
        cell: &Arc<ReservationCell>,
        bytes: u64,
        install: impl FnOnce(Option<Reservation>) -> T,
    ) -> Option<T> {
        self.cache
            .upgrade()?
            .reserve_view(&self.key, cell, bytes, &self.counters, install)
    }
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
            served: false,
            view: None,
        }
    }

    /// This source with whether its column was served from the cache on this
    /// read, and the handle a memo charges views through.
    pub(crate) fn with_view(mut self, served: bool, view: Option<ViewHandle>) -> Self {
        self.served = served;
        self.view = view;
        self
    }

    /// The memo, once filled.
    pub fn get(&self) -> Option<&'a Arc<dyn ValuesMemo>> {
        self.slot.get()
    }

    /// Whether the column was served from the cache on this read, rather than
    /// decoded and just admitted (ADR-3326, view memo amendment, decision 4).
    pub fn served(&self) -> bool {
        self.served
    }

    /// The handle a memo charges views through, `None` for a scan whose
    /// attach the view did not carry.
    pub fn view_handle(&self) -> Option<&ViewHandle> {
        self.view.as_ref()
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
    view_memo_fills: AtomicU64,
    view_memo_refusals: AtomicU64,
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

/// View memo figures (ADR-3326, view memo amendment, decision 7), kept apart
/// from [`DecodedCacheSnapshot`] because views are not data entries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ViewMemoSnapshot {
    /// `viewMemoBytes`: the view total, against the view budget.
    pub bytes: u64,
    /// `viewMemoFills`: fills [`DecodedColumnCache::reserve_view`] charged.
    pub fills: u64,
    /// `viewMemoRefusals`: fills refused by the view budget or the memory
    /// budget.
    pub refusals: u64,
    /// `viewMemoReleases`: view memos [`DecodedColumnCache::evict_for_pressure`]
    /// released.
    pub releases: u64,
}

impl ViewMemoSnapshot {
    /// The counts moved since `mark`, saturating, with this snapshot's
    /// `bytes`, which is a level and not a count.
    #[must_use]
    pub fn since(&self, mark: &ViewMemoSnapshot) -> ViewMemoSnapshot {
        ViewMemoSnapshot {
            bytes: self.bytes,
            fills: self.fills.saturating_sub(mark.fills),
            refusals: self.refusals.saturating_sub(mark.refusals),
            releases: self.releases.saturating_sub(mark.releases),
        }
    }
}

impl DecodedCacheCounters {
    /// This statement's view memo fills and refusals. `bytes` and `releases`
    /// read 0 here: the first is the cache's level and the second happens in
    /// the budget's evictor, not in a statement, so a call site takes both
    /// from [`DecodedColumnCache::view_counters`], as it takes the resident
    /// and pressure figures
    /// (`a_view_fill_is_charged_to_the_view_budget_not_the_data_cap`).
    pub fn view_snapshot(&self) -> ViewMemoSnapshot {
        ViewMemoSnapshot {
            bytes: 0,
            fills: self.view_memo_fills.load(Ordering::Relaxed),
            refusals: self.view_memo_refusals.load(Ordering::Relaxed),
            releases: 0,
        }
    }

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
    /// Bytes of view memos, against `view_max_bytes`; never in `resident`
    /// (ADR-3326, view memo amendment, decision 3;
    /// `a_view_fill_is_charged_to_the_view_budget_not_the_data_cap`).
    view_resident: u64,
    view_fills: u64,
    view_refusals: u64,
    view_releases: u64,
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
    /// dropped so the caller can drop it after releasing the lock. Its memo's
    /// views leave the view total here, since they go with the column
    /// (`removing_an_entry_drops_its_views_and_lowers_the_view_total`).
    fn remove(&mut self, i: usize) -> Option<Arc<DecodedEntry>> {
        let slot = self.slots.get_mut(i)?.take()?;
        self.index.remove(&slot.key);
        self.resident = self.resident.saturating_sub(slot.entry.bytes);
        self.view_resident = self
            .view_resident
            .saturating_sub(slot.entry.col.memo_slot().map_or(0, MemoSlot::extra_bytes));
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
    view_max_bytes: u64,
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
            .field("view_max_bytes", &self.view_max_bytes)
            .field("resident_bytes", &self.resident_bytes())
            .field("view_resident_bytes", &self.view_resident_bytes())
            .finish()
    }
}

impl DecodedColumnCache {
    /// A cache holding at most `max_bytes` of decoded columns, each entry
    /// reserved on `budget` when one is given, aged on the system clock, with
    /// a view budget of 0 ([`Self::with_view_max_bytes`]).
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
            view_max_bytes: 0,
            max_entry_age_ns: DEFAULT_MAX_ENTRY_AGE_NS,
            budget,
            clock,
            inner: Mutex::new(Inner::default()),
            estimates: Mutex::new(EstimateMemo::default()),
            pressure_evictions: AtomicU64::new(0),
            pressure_freed_bytes: AtomicU64::new(0),
        }
    }

    /// This cache with a view budget of `view_max_bytes`, apart from the
    /// data cap (ADR-3326, view memo amendment, decision 3). At 0,
    /// [`Self::reserve_view`] refuses every fill
    /// (`a_view_fill_is_charged_to_the_view_budget_not_the_data_cap`).
    #[must_use]
    pub fn with_view_max_bytes(mut self, view_max_bytes: u64) -> Self {
        self.view_max_bytes = view_max_bytes;
        self
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub fn view_max_bytes(&self) -> u64 {
        self.view_max_bytes
    }

    /// Bytes of the view memos the index's entries hold.
    pub fn view_resident_bytes(&self) -> u64 {
        self.lock().view_resident
    }

    /// Process-wide view memo figures since construction, `bytes` being the
    /// view total now.
    pub fn view_counters(&self) -> ViewMemoSnapshot {
        let inner = self.lock();
        ViewMemoSnapshot {
            bytes: inner.view_resident,
            fills: inner.view_fills,
            refusals: inner.view_refusals,
            releases: inner.view_releases,
        }
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
    /// Before any data entry it releases view memos nothing outside holds
    /// ([`ValuesMemo::release_extra`]), whatever the `cause`, and counts their
    /// bytes toward `want` and in the return value; only the rest of `want`
    /// is swept from data entries (ADR-3326, view memo amendment, decision 7;
    /// `pressure_releases_unheld_views_before_any_data_entry`,
    /// `a_shortfall_above_the_views_frees_the_same_data_with_or_without_views`).
    /// [`Self::pressure_counters`] counts data entries only.
    /// Each pass logs one INFO line;
    /// `slack` is the part of `want` the budget asks beyond the refused
    /// caller's shortfall (logged only).
    pub fn evict_for_pressure(&self, want: u64, slack: u64, cause: ExhaustionCause) -> u64 {
        let mut evicted = Vec::new();
        let mut freed = 0u64;
        let mut held = 0u64;
        let mut views_released = 0u64;
        let resident_after;
        {
            let mut inner = self.lock();
            let n = inner.slots.len();
            let mut releases = 0u64;
            for step in 0..n {
                if freed >= want {
                    break;
                }
                let i = (inner.hand + step) % n;
                let released = inner.slots[i].as_ref().map_or(0, |slot| {
                    slot.entry
                        .col
                        .memo_slot()
                        .map_or(0, MemoSlot::release_extra)
                });
                if released > 0 {
                    inner.view_resident = inner.view_resident.saturating_sub(released);
                    views_released = views_released.saturating_add(released);
                    freed = freed.saturating_add(released);
                    releases += 1;
                }
            }
            inner.view_releases += releases;
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
        let data_freed = freed.saturating_sub(views_released);
        tracing::info!(
            target: "ravel_logseg::decoded_cache",
            asked_bytes = want,
            slack_bytes = slack,
            freed_bytes = data_freed,
            view_released_bytes = views_released,
            resident_bytes_after = resident_after,
            held_bytes = held,
            cause = cause.name(),
            "decoded cache pressure eviction"
        );
        self.pressure_evictions
            .fetch_add(evicted.len() as u64, Ordering::Relaxed);
        self.pressure_freed_bytes
            .fetch_add(data_freed, Ordering::Relaxed);
        // `held` passed them over, so the index held each entry's last handle
        // and nothing else held its cell's column or a memo buffer: dropping
        // them here, outside the lock, releases their reservations.
        drop(evicted);
        freed
    }

    /// Charges `bytes` of views to the view budget for the memo of `key`'s
    /// entry, and on success hands `install` the views' reservation (`None`
    /// without a budget) to move into the views buffer's owner (ADR-3326, view
    /// memo amendment, decisions 3, 4 and 6). `None`, with `install` not run,
    /// when:
    ///
    /// - `key`'s entry is gone, holds another `cell`, has no filled memo, or
    ///   its memo already reports views; none of these is counted
    ///   (`a_fill_for_no_entry_another_cell_or_a_filled_memo_is_not_charged`);
    /// - the view total plus `bytes` would pass the view budget, or the
    ///   memory budget refuses `bytes` while keeping [`admission_margin`]
    ///   unreserved. Each is a `viewMemoRefusals`
    ///   (`a_view_fill_is_charged_to_the_view_budget_not_the_data_cap`).
    ///
    /// The reservation is [`MemoryBudget::reserve_best_effort_keeping`], which
    /// never runs the budget's evictor, and nothing here evicts, so a fill
    /// never costs a data entry or another view memo
    /// (`a_view_fill_never_evicts_a_data_entry`).
    ///
    /// Lock order: the cache's lock comes before any lock of the memo's own,
    /// since `Inner::remove` and the pressure pass call
    /// [`ValuesMemo::extra_bytes`], [`ValuesMemo::release_extra`] and
    /// [`ValuesMemo::held_outside`] under it. So a caller must not hold the
    /// memo's own lock while it calls this, and `install`, which runs under
    /// the cache's lock and may take the memo's, must not call back into the
    /// cache.
    ///
    /// After `install` the memo's [`ValuesMemo::extra_bytes`] must equal
    /// `bytes`; the view total then grows by `bytes`
    /// (`a_view_fill_is_charged_to_the_view_budget_not_the_data_cap`), which
    /// is the figure `Inner::remove` and [`ValuesMemo::release_extra`] later
    /// take back off
    /// (`removing_an_entry_drops_its_views_and_lowers_the_view_total`,
    /// `releasing_a_view_memo_lowers_the_view_total_and_the_budget_by_its_bytes`).
    /// A memo that reports another figure has its views released through
    /// [`ValuesMemo::release_extra`], `install`'s result is dropped with the
    /// reservation it may carry, the fill counts as a refusal and `None` is
    /// returned; the view total grows only by what the memo still reports
    /// after that release, so it keeps matching what `Inner::remove` takes off
    /// (`a_memo_reporting_other_bytes_than_reserved_is_refused`).
    pub fn reserve_view<T>(
        &self,
        key: &DecodedKey,
        cell: &Arc<ReservationCell>,
        bytes: u64,
        counters: &DecodedCacheCounters,
        install: impl FnOnce(Option<Reservation>) -> T,
    ) -> Option<T> {
        {
            let mut inner = self.lock();
            Self::fillable_memo(&inner, key, cell)?;
            if inner
                .view_resident
                .checked_add(bytes)
                .is_none_or(|total| total > self.view_max_bytes)
            {
                inner.view_refusals += 1;
                DecodedCacheCounters::bump(&counters.view_memo_refusals, 1);
                return None;
            }
            inner.view_resident += bytes;
        }
        let reservation = match &self.budget {
            Some(budget) => {
                match budget.reserve_best_effort_keeping(bytes, admission_margin(budget.limit())) {
                    Ok(r) => Some(r),
                    Err(_) => {
                        let mut inner = self.lock();
                        inner.view_resident = inner.view_resident.saturating_sub(bytes);
                        inner.view_refusals += 1;
                        DecodedCacheCounters::bump(&counters.view_memo_refusals, 1);
                        return None;
                    }
                }
            }
            None => None,
        };
        let mut inner = self.lock();
        inner.view_resident = inner.view_resident.saturating_sub(bytes);
        // The entry may have left the index, or another fill landed, while
        // the reservation was taken; `reservation` then drops here.
        let memo = Arc::clone(Self::fillable_memo(&inner, key, cell)?);
        let installed = install(reservation);
        let charged = memo.extra_bytes();
        if charged != bytes {
            memo.release_extra();
            inner.view_resident = inner.view_resident.saturating_add(memo.extra_bytes());
            inner.view_refusals += 1;
            DecodedCacheCounters::bump(&counters.view_memo_refusals, 1);
            drop(inner);
            drop(installed);
            return None;
        }
        inner.view_resident = inner.view_resident.saturating_add(charged);
        if charged > 0 {
            inner.view_fills += 1;
            DecodedCacheCounters::bump(&counters.view_memo_fills, 1);
        }
        Some(installed)
    }

    /// The memo of `key`'s entry when that entry holds `cell` and its memo is
    /// filled and reports no views yet.
    fn fillable_memo<'a>(
        inner: &'a Inner,
        key: &DecodedKey,
        cell: &Arc<ReservationCell>,
    ) -> Option<&'a Arc<dyn ValuesMemo>> {
        let i = *inner.index.get(key)?;
        let entry = &inner.slots.get(i)?.as_ref()?.entry;
        if !Arc::ptr_eq(&entry.cell, cell) {
            return None;
        }
        entry
            .col
            .memo_slot()
            .and_then(MemoSlot::get)
            .filter(|m| m.extra_bytes() == 0)
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
    use std::sync::Weak;

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

    /// Rows of every view-memo fixture column, and the bytes of its views at
    /// 16 B a row.
    const ROWS: usize = 64;
    const VIEW_BYTES: u64 = 16 * ROWS as u64;

    /// A test memo's views as a `StringViewArray` holds them: the views buffer
    /// behind an owner carrying the views' reservation, and the block list
    /// holding the memo's values buffer. Cloning it is what cloning the array
    /// does: the views buffer's count moves, the values buffer's does not.
    #[derive(Clone)]
    struct TestViews {
        views: Arc<ViewsOwner>,
        _blocks: Arc<[Arc<String>]>,
    }

    struct ViewsOwner {
        _views: Vec<u128>,
        _reservation: Option<Reservation>,
    }

    /// A memo that can hold views, held outside by decision 7's rule: the
    /// values count above 1, or above 2 while views are filled, or the views
    /// buffer's count above 1.
    struct ViewTestMemo {
        values: Arc<String>,
        views: Mutex<Option<(TestViews, u64)>>,
    }

    impl ViewTestMemo {
        fn slot(&self) -> MutexGuard<'_, Option<(TestViews, u64)>> {
            self.views.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// A batch's clone of the views array.
        fn batch(&self) -> Option<TestViews> {
            self.slot().as_ref().map(|(v, _)| v.clone())
        }
    }

    impl ValuesMemo for ViewTestMemo {
        fn held_outside(&self) -> bool {
            match &*self.slot() {
                Some((v, _)) => {
                    Arc::strong_count(&self.values) > 2 || Arc::strong_count(&v.views) > 1
                }
                None => Arc::strong_count(&self.values) > 1,
            }
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn extra_bytes(&self) -> u64 {
            self.slot().as_ref().map_or(0, |(_, b)| *b)
        }

        fn release_extra(&self) -> u64 {
            let mut slot = self.slot();
            match &*slot {
                Some((v, b)) if Arc::strong_count(&v.views) == 1 => {
                    let b = *b;
                    *slot = None;
                    b
                }
                _ => 0,
            }
        }
    }

    fn plain_rows() -> CachedColumn {
        plain(&[&b"ab"[..]; ROWS])
    }

    /// The bytes [`admit_memo`] charges an entry.
    fn memo_entry_bytes() -> u64 {
        plain_rows().heap_bytes() as u64 + VALUES_MEMO_BYTES
    }

    /// Admits a plain column under `k` with a [`ViewTestMemo`] in its slot and
    /// no views, holding nothing of it, and returns the memo weakly.
    fn admit_memo(
        cache: &DecodedColumnCache,
        k: DecodedKey,
        c: &DecodedCacheCounters,
    ) -> Weak<ViewTestMemo> {
        let v = plain_rows();
        let entry = cache
            .offer(k, &v, AccessIntent::Interactive, c)
            .expect("admitted");
        assert_eq!(entry.bytes(), memo_entry_bytes());
        let memo = Arc::new(ViewTestMemo {
            values: Arc::new("ab".repeat(ROWS)),
            views: Mutex::new(None),
        });
        assert!(
            v.memo_slot()
                .expect("a validated plain column has a slot")
                .fill_for_test(Arc::clone(&memo) as Arc<dyn ValuesMemo>)
        );
        Arc::downgrade(&memo)
    }

    /// Fills `k`'s memo with [`VIEW_BYTES`] of views through
    /// [`DecodedColumnCache::reserve_view`], the reservation moved into the
    /// views buffer's owner. Whether the fill was charged.
    fn fill_at(
        cache: &DecodedColumnCache,
        k: DecodedKey,
        memo: &Weak<ViewTestMemo>,
        c: &DecodedCacheCounters,
    ) -> bool {
        let memo = memo.upgrade().expect("the memo lives with its entry");
        let cell = cell_of(cache, k);
        cache
            .reserve_view(&k, &cell, VIEW_BYTES, c, |reservation| {
                let views = TestViews {
                    views: Arc::new(ViewsOwner {
                        _views: vec![0; ROWS],
                        _reservation: reservation,
                    }),
                    _blocks: Arc::from(vec![Arc::clone(&memo.values)]),
                };
                *memo.slot() = Some((views, VIEW_BYTES));
            })
            .is_some()
    }

    /// The reservation cell of `k`'s entry, read without marking it
    /// referenced.
    fn cell_of(cache: &DecodedColumnCache, k: DecodedKey) -> Arc<ReservationCell> {
        let inner = cache.lock();
        let i = *inner.index.get(&k).expect("resident");
        Arc::clone(&inner.slots[i].as_ref().expect("slot").entry.cell)
    }

    fn extra(memo: &Weak<ViewTestMemo>) -> u64 {
        memo.upgrade().map_or(0, |m| m.extra_bytes())
    }

    /// Whether `evict_for_pressure` counts `k`'s entry as held.
    fn entry_held(cache: &DecodedColumnCache, k: DecodedKey) -> bool {
        let inner = cache.lock();
        let i = *inner.index.get(&k).expect("resident");
        DecodedEntry::held(&inner.slots[i].as_ref().expect("slot").entry)
    }

    /// A memo whose `install` records another figure than the bytes reserved
    /// is refused: its views are released, the reservation goes back, the
    /// view total does not move, and one refusal and no fill are counted.
    #[test]
    fn a_memo_reporting_other_bytes_than_reserved_is_refused() {
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)))
            .with_view_max_bytes(1 << 20);
        let c = DecodedCacheCounters::default();
        let k = key(0, 1);
        let memo = admit_memo(&cache, k, &c);
        let reserved = budget.reserved();
        let m = memo.upgrade().expect("the memo lives with its entry");
        let cell = cell_of(&cache, k);
        let installed = cache.reserve_view(&k, &cell, VIEW_BYTES, &c, |reservation| {
            let views = TestViews {
                views: Arc::new(ViewsOwner {
                    _views: vec![0; ROWS],
                    _reservation: reservation,
                }),
                _blocks: Arc::from(vec![Arc::clone(&m.values)]),
            };
            *m.slot() = Some((views, VIEW_BYTES / 2));
        });
        assert!(installed.is_none(), "the mismatched fill is refused");
        assert_eq!(m.extra_bytes(), 0, "the memo's views were released");
        assert!(m.batch().is_none());
        assert_eq!(cache.view_resident_bytes(), 0);
        assert_eq!(budget.reserved(), reserved, "the reservation went back");
        assert_eq!(
            cache.view_counters(),
            ViewMemoSnapshot {
                bytes: 0,
                fills: 0,
                refusals: 1,
                releases: 0,
            }
        );
        assert_eq!(c.view_snapshot().refusals, 1);
        assert_eq!(c.view_snapshot().fills, 0);
        // A matching fill afterwards is charged as usual.
        assert!(fill_at(&cache, k, &memo, &c));
        assert_eq!(cache.view_resident_bytes(), VIEW_BYTES);
    }

    /// ADR-3326, view memo amendment, T3: a fill grows the view total and the
    /// budget by exactly the views' bytes and moves no data figure; a data
    /// admission at the cap evicts what it evicts with no views; a fill the
    /// memory budget or the view budget refuses leaves no views and counts one
    /// refusal; and a batch's views clone keeps the views reserved past the
    /// entry's eviction, since the reservation sits on the views buffer's
    /// owner.
    #[test]
    fn a_view_fill_is_charged_to_the_view_budget_not_the_data_cap() {
        let e = memo_entry_bytes();
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache =
            DecodedColumnCache::new(2 * e, Some(Arc::clone(&budget))).with_view_max_bytes(1 << 20);
        let c = DecodedCacheCounters::default();
        admit_memo(&cache, key(0, 1), &c);
        let m1 = admit_memo(&cache, key(1, 1), &c);
        let reserved = budget.reserved();
        let process = cache.process_counters();
        let pressure = cache.pressure_counters();
        let data = c.snapshot();
        assert_eq!(cache.resident_bytes(), 2 * e);
        assert!(fill_at(&cache, key(1, 1), &m1, &c));
        assert_eq!(cache.view_resident_bytes(), VIEW_BYTES);
        assert_eq!(budget.reserved(), reserved + VIEW_BYTES);
        assert_eq!(cache.resident_bytes(), 2 * e, "resident is data only");
        assert_eq!(cache.process_counters(), process);
        assert_eq!(cache.pressure_counters(), pressure);
        assert_eq!(c.snapshot(), data);
        assert_eq!(
            c.view_snapshot(),
            ViewMemoSnapshot {
                bytes: 0,
                fills: 1,
                refusals: 0,
                releases: 0,
            }
        );
        assert_eq!(
            cache.view_counters(),
            ViewMemoSnapshot {
                bytes: VIEW_BYTES,
                fills: 1,
                refusals: 0,
                releases: 0,
            }
        );

        // The cache at its cap: the next admission evicts block 0 alone,
        // with views on block 1 or without.
        let mut outcomes = Vec::new();
        for views in [false, true] {
            let budget = Arc::new(MemoryBudget::new(4 << 30));
            let cache = DecodedColumnCache::new(2 * e, Some(budget)).with_view_max_bytes(1 << 20);
            let c = DecodedCacheCounters::default();
            admit_memo(&cache, key(0, 1), &c);
            let m1 = admit_memo(&cache, key(1, 1), &c);
            if views {
                assert!(fill_at(&cache, key(1, 1), &m1, &c));
            }
            let next = DecodedCacheCounters::default();
            drop(cache.offer(key(2, 1), &plain_rows(), AccessIntent::Interactive, &next));
            let kept = [0, 1, 2].map(|b| cache.entry_bytes(&key(b, 1)).is_some());
            outcomes.push((next.snapshot().evictions, kept));
        }
        assert_eq!(outcomes[0], (1, [false, true, true]));
        assert_eq!(outcomes[1], outcomes[0], "views cost no data entry");

        // A memory budget one byte short of the views beyond the margin.
        const LIMIT: u64 = 4 << 30;
        let budget = Arc::new(MemoryBudget::new(LIMIT));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)))
            .with_view_max_bytes(1 << 20);
        let c = DecodedCacheCounters::default();
        let m0 = admit_memo(&cache, key(0, 1), &c);
        budget.reserve_unchecked(
            LIMIT - budget.reserved() - (admission_margin(LIMIT) + VIEW_BYTES - 1),
        );
        let before = budget.reserved();
        assert!(!fill_at(&cache, key(0, 1), &m0, &c), "refused");
        let memo = m0.upgrade().expect("resident");
        assert!(memo.batch().is_none(), "no views exist");
        assert_eq!(memo.extra_bytes(), 0);
        drop(memo);
        assert_eq!(budget.reserved(), before, "nothing stays reserved");
        assert_eq!(
            cache.view_counters(),
            ViewMemoSnapshot {
                bytes: 0,
                fills: 0,
                refusals: 1,
                releases: 0,
            }
        );
        assert_eq!(c.view_snapshot().refusals, 1);
        budget.release(1);
        assert!(fill_at(&cache, key(0, 1), &m0, &c), "exactly at the margin");
        assert_eq!(budget.reserved(), before - 1 + VIEW_BYTES);

        // A view budget of 0, as `new` builds, and one byte short of the
        // views.
        for view_max in [None, Some(VIEW_BYTES - 1)] {
            let cache = DecodedColumnCache::new(1 << 20, None);
            let cache = match view_max {
                Some(v) => cache.with_view_max_bytes(v),
                None => cache,
            };
            let c = DecodedCacheCounters::default();
            let m0 = admit_memo(&cache, key(0, 1), &c);
            assert!(!fill_at(&cache, key(0, 1), &m0, &c), "{view_max:?}");
            assert_eq!(extra(&m0), 0, "{view_max:?}");
            assert_eq!(cache.view_counters().refusals, 1, "{view_max:?}");
            assert_eq!(cache.view_resident_bytes(), 0, "{view_max:?}");
        }

        // The reservation goes with the views buffer's last holder.
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache =
            DecodedColumnCache::new(e, Some(Arc::clone(&budget))).with_view_max_bytes(1 << 20);
        let c = DecodedCacheCounters::default();
        let m0 = admit_memo(&cache, key(0, 1), &c);
        assert!(fill_at(&cache, key(0, 1), &m0, &c));
        let batch = m0.upgrade().and_then(|m| m.batch()).expect("views");
        admit_memo(&cache, key(1, 1), &c);
        assert_eq!(c.snapshot().evictions, 1);
        assert!(cache.entry_bytes(&key(0, 1)).is_none(), "evicted by CLOCK");
        assert!(m0.upgrade().is_none(), "the memo went with its column");
        assert_eq!(
            budget.reserved(),
            e + VIEW_BYTES,
            "the batch's views clone keeps the views reserved"
        );
        drop(batch);
        assert_eq!(budget.reserved(), e);
    }

    /// ADR-3326, view memo amendment, T4: an entry whose views a batch holds,
    /// as a clone of the views array, is held, and the pressure pass frees
    /// neither its views nor its data; with no holder it is evictable, and
    /// the pass frees both.
    #[test]
    fn an_entry_whose_views_a_batch_holds_is_held() {
        let e = memo_entry_bytes();
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)))
            .with_view_max_bytes(1 << 20);
        let c = DecodedCacheCounters::default();
        let m0 = admit_memo(&cache, key(0, 1), &c);
        assert!(fill_at(&cache, key(0, 1), &m0, &c));
        assert!(!entry_held(&cache, key(0, 1)), "filled, with no holder");

        let batch = m0.upgrade().and_then(|m| m.batch()).expect("views");
        assert!(entry_held(&cache, key(0, 1)), "a views clone holds it");
        assert_eq!(
            cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted),
            0
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(extra(&m0), VIEW_BYTES);
        assert_eq!(cache.view_resident_bytes(), VIEW_BYTES);
        assert_eq!(budget.reserved(), e + VIEW_BYTES);
        assert_eq!(cache.view_counters().releases, 0);

        drop(batch);
        assert!(!entry_held(&cache, key(0, 1)));
        assert_eq!(
            cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted),
            VIEW_BYTES + e
        );
        assert!(cache.is_empty());
        assert_eq!(budget.reserved(), 0);
        assert_eq!(cache.view_resident_bytes(), 0);
        assert_eq!(
            cache.pressure_counters(),
            PressureSnapshot {
                evictions: 1,
                freed_bytes: e,
            }
        );
        assert_eq!(cache.view_counters().releases, 1);
    }

    /// The cache as `budget`'s evictor, as the server wires it, with `slack`.
    fn register(budget: &MemoryBudget, cache: &Arc<DecodedColumnCache>, slack: u64) {
        let evictee = Arc::downgrade(cache);
        assert!(
            budget
                .register_evictor(
                    Box::new(move |want, cause| {
                        evictee
                            .upgrade()
                            .map_or(0, |c| c.evict_for_pressure(want, slack, cause))
                    }),
                    slack,
                )
                .is_ok()
        );
    }

    /// ADR-3326, view memo amendment, T5: a fill the margin refuses, and one
    /// the limit refuses, run no evictor, though one is registered over
    /// unheld data entries, and a fill the full view budget refuses evicts
    /// nothing either: `evictions` and the pressure evictions stay 0 and
    /// every data entry still hits.
    #[test]
    fn a_view_fill_never_evicts_a_data_entry() {
        const LIMIT: u64 = 4 << 30;
        let margin = admission_margin(LIMIT);
        for (case, free) in [
            ("margin", margin + VIEW_BYTES - 1),
            ("limit", VIEW_BYTES - 1),
        ] {
            let budget = Arc::new(MemoryBudget::new(LIMIT));
            let cache = Arc::new(
                DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)))
                    .with_view_max_bytes(1 << 20),
            );
            register(&budget, &cache, margin);
            let c = DecodedCacheCounters::default();
            let memos: Vec<_> = (0..4).map(|b| admit_memo(&cache, key(b, 1), &c)).collect();
            budget.reserve_unchecked(LIMIT - budget.reserved() - free);
            assert!(
                !fill_at(&cache, key(3, 1), &memos[3], &c),
                "{case}: refused"
            );
            assert_eq!(extra(&memos[3]), 0, "{case}");
            assert_eq!(c.snapshot().evictions, 0, "{case}");
            assert_eq!(cache.pressure_counters().evictions, 0, "{case}");
            for b in 0..4 {
                assert!(
                    cache.lookup(&key(b, 1), ROWS, &c).is_some(),
                    "{case}: {b} hits"
                );
            }
            assert_eq!(cache.view_counters().refusals, 1, "{case}");
            assert_eq!(cache.view_resident_bytes(), 0, "{case}");
        }

        // A full view budget.
        let budget = Arc::new(MemoryBudget::new(LIMIT));
        let cache = Arc::new(
            DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)))
                .with_view_max_bytes(VIEW_BYTES),
        );
        register(&budget, &cache, margin);
        let c = DecodedCacheCounters::default();
        let memos: Vec<_> = (0..4).map(|b| admit_memo(&cache, key(b, 1), &c)).collect();
        assert!(fill_at(&cache, key(0, 1), &memos[0], &c));
        assert!(
            !fill_at(&cache, key(3, 1), &memos[3], &c),
            "the view budget is full"
        );
        assert_eq!(c.snapshot().evictions, 0);
        assert_eq!(cache.pressure_counters().evictions, 0);
        for b in 0..4 {
            assert!(cache.lookup(&key(b, 1), ROWS, &c).is_some(), "{b} hits");
        }
        assert_eq!(extra(&memos[0]), VIEW_BYTES);
        assert_eq!(cache.view_resident_bytes(), VIEW_BYTES);
        assert_eq!(cache.view_counters().refusals, 1);
    }

    /// A refusal cause for the pressure tests, and how each sets its budget.
    #[derive(Clone, Copy, Debug)]
    enum Refusal {
        Ledger,
        Gate,
    }

    const GATE_FLOOR: u64 = 1 << 30;

    /// A 4 GiB budget with the cache as its evictor at slack 0, holding `n`
    /// unheld entries, each with views when `views`.
    fn pressure_fixture(
        n: u32,
        views: bool,
    ) -> (
        Arc<MemoryBudget>,
        Arc<DecodedColumnCache>,
        Vec<Weak<ViewTestMemo>>,
    ) {
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = Arc::new(
            DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)))
                .with_view_max_bytes(1 << 20),
        );
        register(&budget, &cache, 0);
        let c = DecodedCacheCounters::default();
        let memos: Vec<_> = (0..n)
            .map(|b| {
                let m = admit_memo(&cache, key(b, 1), &c);
                if views {
                    assert!(fill_at(&cache, key(b, 1), &m, &c));
                }
                m
            })
            .collect();
        (budget, cache, memos)
    }

    /// Sets `budget` so the request it returns is refused for `cause` with a
    /// shortfall of `shortfall` plus `views`. A ledger refusal reserves the
    /// limit less the request plus `shortfall`, on top of what the budget holds
    /// other than `views`; a gate refusal closes the gate with `available`
    /// below `GATE_FLOOR` by `shortfall` less the request, and lower again by
    /// `views`, since views are resident memory.
    fn refuse(budget: &MemoryBudget, cause: Refusal, shortfall: u64, views: u64) -> u64 {
        match cause {
            Refusal::Ledger => {
                let n = 1 << 20;
                let held = budget.reserved() - views;
                budget.reserve_unchecked(budget.limit() - held - n + shortfall);
                n
            }
            Refusal::Gate => {
                let n = 64;
                budget.set_host_memory_gate(false, GATE_FLOOR + n - shortfall - views, GATE_FLOOR);
                n
            }
        }
    }

    /// ADR-3326, view memo amendment, T6: under a ledger refusal and under a
    /// host-gate refusal whose shortfall the unheld views cover, the pass
    /// releases one view memo and evicts no data entry, and the request is
    /// then admitted. A view memo a batch holds is skipped: the next one is
    /// released instead and the held one keeps its views.
    #[test]
    fn pressure_releases_unheld_views_before_any_data_entry() {
        for cause in [Refusal::Ledger, Refusal::Gate] {
            for held in [false, true] {
                let (budget, cache, memos) = pressure_fixture(8, true);
                let batch =
                    held.then(|| memos[0].upgrade().and_then(|m| m.batch()).expect("views"));
                let n = refuse(&budget, cause, VIEW_BYTES, 0);
                budget
                    .try_reserve(n)
                    .unwrap_or_else(|e| panic!("{cause:?} held {held}: {e:?}"));
                let what = format!("{cause:?} held {held}");
                assert_eq!(cache.view_counters().releases, 1, "{what}");
                assert_eq!(cache.pressure_counters().evictions, 0, "{what}");
                assert_eq!(cache.len(), 8, "{what}");
                assert_eq!(cache.view_resident_bytes(), 7 * VIEW_BYTES, "{what}");
                let released = usize::from(held);
                for (i, m) in memos.iter().enumerate() {
                    let want = if i == released { 0 } else { VIEW_BYTES };
                    assert_eq!(extra(m), want, "{what}: memo {i}");
                }
                assert_eq!(
                    (budget.accounted_refusals(), budget.gate_refusals()),
                    (0, 0),
                    "{what}"
                );
                drop(batch);
            }
        }
    }

    /// ADR-3326, view memo amendment, T6: a shortfall above the views, under
    /// each cause, against A with no views and B with the same data entries
    /// and 8 unheld view memos, whose budget (ledger) or host reading (gate)
    /// carries the view bytes on top. B releases every view memo and frees
    /// the same data bytes as A: 3 entries for a shortfall of 2 entries and
    /// a byte.
    #[test]
    fn a_shortfall_above_the_views_frees_the_same_data_with_or_without_views() {
        let e = memo_entry_bytes();
        for cause in [Refusal::Ledger, Refusal::Gate] {
            let mut arms = Vec::new();
            for views in [false, true] {
                let (budget, cache, _memos) = pressure_fixture(8, views);
                let data = cache.resident_bytes();
                assert_eq!(data, 8 * e);
                let view_bytes = cache.view_resident_bytes();
                let n = refuse(&budget, cause, 2 * e + 1, view_bytes);
                budget
                    .try_reserve(n)
                    .unwrap_or_else(|err| panic!("{cause:?} views {views}: {err:?}"));
                arms.push((
                    cache.pressure_counters(),
                    cache.view_counters().releases,
                    cache.view_resident_bytes(),
                ));
            }
            let freed = PressureSnapshot {
                evictions: 3,
                freed_bytes: 3 * e,
            };
            assert_eq!(arms[0], (freed, 0, 0), "{cause:?}: A");
            assert_eq!(arms[1], (freed, 8, 0), "{cause:?}: B");
        }
    }

    /// ADR-3326, view memo amendment, T6, the counter falls with the release:
    /// each pass that releases a view memo whose entry stays resident lowers
    /// the view total and the budget by exactly its bytes, down to 0, and
    /// leaves `resident` alone.
    #[test]
    fn releasing_a_view_memo_lowers_the_view_total_and_the_budget_by_its_bytes() {
        let (budget, cache, memos) = pressure_fixture(2, true);
        let reserved = budget.reserved();
        let resident = cache.resident_bytes();
        assert_eq!(cache.view_resident_bytes(), 2 * VIEW_BYTES);
        for pass in 1..=2u64 {
            assert_eq!(
                cache.evict_for_pressure(VIEW_BYTES, 0, ExhaustionCause::Accounted),
                VIEW_BYTES,
                "pass {pass}"
            );
            assert_eq!(
                cache.view_resident_bytes(),
                (2 - pass) * VIEW_BYTES,
                "pass {pass}"
            );
            assert_eq!(
                budget.reserved(),
                reserved - pass * VIEW_BYTES,
                "pass {pass}"
            );
            assert_eq!(cache.resident_bytes(), resident, "pass {pass}");
            assert_eq!(cache.len(), 2, "pass {pass}");
            assert_eq!(cache.view_counters().releases, pass);
        }
        assert!(memos.iter().all(|m| extra(m) == 0));
        assert_eq!(cache.pressure_counters().evictions, 0);
    }

    /// ADR-3326, view memo amendment, T7: evicting an entry, by CLOCK or by
    /// pressure, drops its memo and its views with the column and takes its
    /// views off the view total; once the last batch drops the budget is back
    /// where it started; and with the cache empty a new fill is admitted on a
    /// view budget that holds one memo.
    #[test]
    fn removing_an_entry_drops_its_views_and_lowers_the_view_total() {
        let e = memo_entry_bytes();
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache =
            DecodedColumnCache::new(e, Some(Arc::clone(&budget))).with_view_max_bytes(VIEW_BYTES);
        let c = DecodedCacheCounters::default();

        // By CLOCK, a batch holding the views.
        let m0 = admit_memo(&cache, key(0, 1), &c);
        assert!(fill_at(&cache, key(0, 1), &m0, &c));
        let batch = m0.upgrade().and_then(|m| m.batch()).expect("views");
        assert_eq!(budget.reserved(), e + VIEW_BYTES);
        let m1 = admit_memo(&cache, key(1, 1), &c);
        assert_eq!(c.snapshot().evictions, 1);
        assert!(m0.upgrade().is_none(), "the memo went with its column");
        assert_eq!(cache.view_resident_bytes(), 0);
        drop(batch);
        assert_eq!(budget.reserved(), e, "only block 1's data stays reserved");

        // A fill on the view budget the evicted views left.
        assert!(fill_at(&cache, key(1, 1), &m1, &c), "admitted");
        assert_eq!(cache.view_resident_bytes(), VIEW_BYTES);

        // By pressure.
        assert_eq!(
            cache.evict_for_pressure(u64::MAX, 0, ExhaustionCause::Accounted),
            e + VIEW_BYTES
        );
        assert!(cache.is_empty());
        assert!(m1.upgrade().is_none(), "the memo went with its column");
        assert_eq!(cache.view_resident_bytes(), 0);
        assert_eq!(budget.reserved(), 0, "back to the start");

        let m2 = admit_memo(&cache, key(2, 1), &c);
        assert!(fill_at(&cache, key(2, 1), &m2, &c), "admitted");
        assert_eq!(cache.view_resident_bytes(), VIEW_BYTES);
        assert_eq!(cache.view_counters().fills, 3);
    }

    /// A fill for a key with no entry, with a cell other than its entry's, on
    /// a column whose memo slot is empty, or on a memo that already holds
    /// views, runs no `install`, charges nothing and counts no refusal.
    #[test]
    fn a_fill_for_no_entry_another_cell_or_a_filled_memo_is_not_charged() {
        let budget = Arc::new(MemoryBudget::new(4 << 30));
        let cache = DecodedColumnCache::new(1 << 20, Some(Arc::clone(&budget)))
            .with_view_max_bytes(1 << 20);
        let c = DecodedCacheCounters::default();
        let m0 = admit_memo(&cache, key(0, 1), &c);
        let bare = cache
            .offer(key(1, 1), &plain_rows(), AccessIntent::Interactive, &c)
            .expect("admitted");
        let ran = AtomicU64::new(0);
        let try_fill = |k: DecodedKey, cell: &Arc<ReservationCell>| {
            cache
                .reserve_view(&k, cell, VIEW_BYTES, &c, |_| {
                    ran.fetch_add(1, Ordering::Relaxed);
                })
                .is_some()
        };
        let fresh = Arc::new(ReservationCell::default());
        assert!(!try_fill(key(9, 1), bare.reservation_cell()), "no entry");
        assert!(
            !try_fill(key(0, 1), bare.reservation_cell()),
            "another entry's cell"
        );
        assert!(!try_fill(key(0, 1), &fresh), "a cell no entry holds");
        assert!(!try_fill(key(1, 1), bare.reservation_cell()), "no memo");
        let reserved = budget.reserved();
        assert!(fill_at(&cache, key(0, 1), &m0, &c));
        assert!(
            !try_fill(key(0, 1), &cell_of(&cache, key(0, 1))),
            "the memo already holds views"
        );
        assert_eq!(ran.load(Ordering::Relaxed), 0);
        assert_eq!(
            cache.view_counters(),
            ViewMemoSnapshot {
                bytes: VIEW_BYTES,
                fills: 1,
                refusals: 0,
                releases: 0,
            }
        );
        assert_eq!(budget.reserved(), reserved + VIEW_BYTES);
    }
}
