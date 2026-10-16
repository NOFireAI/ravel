//! Per-GET time to first byte and time to last byte, bucketed by the GET's
//! requested size and split by the read phase that issued it (issue #2976).
//!
//! Observability only, like the rest of [`crate::instrument`]: nothing here
//! changes a request, and no figure feeds a correctness decision.
//!
//! # What one observation is
//!
//! [`InstrumentedStore`](crate::InstrumentedStore) records one observation per
//! completed `get`, `get_pinned` or `get_with_pin` call into the
//! [`GetLatencyRecorder`] of its [`StoreMetrics`](crate::StoreMetrics). Both
//! durations start at the same clock reading, taken immediately before the
//! wrapped backend's call:
//!
//! - **Time to last byte** (`total`) ends when that call returns, with the
//!   whole body in hand.
//! - **Time to first byte** (`first_byte`) ends when the backend reports that
//!   the response's status line and headers arrived, before any of its body is
//!   read, through [`mark_first_byte`]. The S3 adapter and the external-store
//!   adapter mark it right after `object_store`'s request future resolves to a
//!   `GetResult` and before they read the body, for whole-object and ranged
//!   reads alike. A whole-object read the S3 adapter splits into several
//!   requests keeps the first mark. A backend that never marks (the in-process
//!   [`MemoryStore`](crate::memory::MemoryStore), whose answer is the complete
//!   body) records first byte equal to total.
//!
//! # Retries
//!
//! Each call is one request here. A caller that retries issues a second call,
//! which is recorded on its own under whatever phase is in scope for it: the
//! same phase when the retry runs inside the scope that issued the original.
//! A call that returns `Err` is counted in its phase's `failed` row with its
//! total latency and in no size bucket, since a failed whole-object GET has no
//! byte count to bucket by. A retry `object_store`'s S3 client makes inside one
//! call (its `RetryConfig` loop) is not a separate request here: it is part of
//! that call's latency, and the billed attempt is counted by
//! [`StoreMetrics::record_attempt`](crate::StoreMetrics::record_attempt).
//!
//! # Size buckets
//!
//! A GET is bucketed by its requested byte count: the range length for
//! [`GetRange::Range`](crate::GetRange::Range), `n` for
//! [`GetRange::Suffix`](crate::GetRange::Suffix), and the bytes returned for a
//! whole-object [`GetRange::Full`](crate::GetRange::Full). Bucket 0 is below
//! 4 KiB; buckets 1 to 14 are the power-of-two spans `[4 KiB, 8 KiB)` up to
//! `[32 MiB, 64 MiB]`, the last of which includes 64 MiB itself; bucket 15 is
//! everything above 64 MiB. See [`size_bucket`] and [`SIZE_BUCKET_LABELS`].
//!
//! # Latency resolution
//!
//! Each latency histogram counts whole microseconds in [`LATENCY_SLOT_COUNT`]
//! slots: one slot per value below 8 us, then eight equal slots per power of
//! two up to 2^32 us (about 71 minutes), the last slot also holding anything
//! longer. A reported quantile is the largest value of the slot holding that
//! rank, capped at the exact maximum, so it never understates the true
//! quantile and overstates it by at most one eighth (12.5%); a quantile that
//! falls in the last slot is reported as the maximum. `max` is exact to the
//! microsecond.
//!
//! # Phases
//!
//! The caller that knows the phase wraps the future that issues the GET with
//! [`InGetPhase::in_get_phase`]; the decorator reads it with
//! [`current_phase`]. The phase is a `tokio` task-local, so it reaches every
//! GET polled inside that future on the same task, through any number of
//! decorators, and an inner scope overrides an outer one. A GET issued with no
//! phase in scope is recorded under [`GetPhase::Unattributed`].
//!
//! # Cost
//!
//! The histograms are allocated once, on the first recorded GET; after that,
//! recording a GET is a few relaxed atomic operations and allocates nothing.

use std::cell::Cell;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::task::futures::TaskLocalFuture;

use crate::GetRange;
use crate::instrument::MonotonicClock;

/// The read phase a GET is attributed to. The first four mirror
/// `ravel_query::QueryPhase`; [`GetPhase::Unattributed`] is where a GET issued
/// with no phase in scope is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GetPhase {
    Resolve,
    Plan,
    Probe,
    Scan,
    Unattributed,
}

/// Number of [`GetPhase`] variants.
pub const GET_PHASE_COUNT: usize = 5;

impl GetPhase {
    /// Every phase, in [`index`](Self::index) order.
    pub const ALL: [GetPhase; GET_PHASE_COUNT] = [
        GetPhase::Resolve,
        GetPhase::Plan,
        GetPhase::Probe,
        GetPhase::Scan,
        GetPhase::Unattributed,
    ];

    /// Lower-case, stable name for a report key.
    pub fn name(self) -> &'static str {
        match self {
            GetPhase::Resolve => "resolve",
            GetPhase::Plan => "plan",
            GetPhase::Probe => "probe",
            GetPhase::Scan => "scan",
            GetPhase::Unattributed => "unattributed",
        }
    }

    /// Dense index, `< GET_PHASE_COUNT`.
    pub fn index(self) -> usize {
        match self {
            GetPhase::Resolve => 0,
            GetPhase::Plan => 1,
            GetPhase::Probe => 2,
            GetPhase::Scan => 3,
            GetPhase::Unattributed => 4,
        }
    }
}

/// Number of size buckets: below 4 KiB, fourteen power-of-two spans, overflow.
pub const SIZE_BUCKET_COUNT: usize = 16;

/// Report label of each size bucket, in [`size_bucket`] order.
pub const SIZE_BUCKET_LABELS: [&str; SIZE_BUCKET_COUNT] = [
    "lt_4KiB",
    "4KiB_8KiB",
    "8KiB_16KiB",
    "16KiB_32KiB",
    "32KiB_64KiB",
    "64KiB_128KiB",
    "128KiB_256KiB",
    "256KiB_512KiB",
    "512KiB_1MiB",
    "1MiB_2MiB",
    "2MiB_4MiB",
    "4MiB_8MiB",
    "8MiB_16MiB",
    "16MiB_32MiB",
    "32MiB_64MiB",
    "gt_64MiB",
];

const SMALLEST_BUCKETED: u64 = 4 * 1024;
const LARGEST_BUCKETED: u64 = 64 * 1024 * 1024;

/// The size bucket of a GET that requested `bytes` bytes. See the
/// [module docs](self) for the bounds.
pub fn size_bucket(bytes: u64) -> usize {
    if bytes < SMALLEST_BUCKETED {
        return 0;
    }
    if bytes > LARGEST_BUCKETED {
        return SIZE_BUCKET_COUNT - 1;
    }
    // 4 KiB is 2^12 and lands in bucket 1; 64 MiB is 2^26 and joins the
    // [32 MiB, 64 MiB) bucket, 14.
    let log2 = bytes.ilog2() as usize;
    log2.min(25) - 11
}

/// Inclusive byte bounds of size bucket `bucket`: `(lowest, highest)`, with
/// `None` as the highest for the overflow bucket.
pub fn size_bucket_bounds(bucket: usize) -> (u64, Option<u64>) {
    match bucket {
        0 => (0, Some(SMALLEST_BUCKETED - 1)),
        b if b >= SIZE_BUCKET_COUNT - 1 => (LARGEST_BUCKETED + 1, None),
        14 => (LARGEST_BUCKETED / 2, Some(LARGEST_BUCKETED)),
        b => (1 << (b + 11), Some((1 << (b + 12)) - 1)),
    }
}

/// The byte count a GET for `range` is bucketed by when it returned
/// `returned` bytes: the requested length for a bounded or suffix range, the
/// bytes returned for a whole-object read.
pub fn requested_bytes(range: GetRange, returned: u64) -> u64 {
    match range {
        GetRange::Full => returned,
        GetRange::Range(start, end) => end.saturating_sub(start),
        GetRange::Suffix(n) => n,
    }
}

/// Slots per latency histogram. See the [module docs](self).
pub const LATENCY_SLOT_COUNT: usize = 240;

/// Values below this many microseconds get one slot each.
const EXACT_MICROS: u64 = 8;
const SUB_BUCKET_BITS: u32 = 3;
const LARGEST_SLOTTED_MICROS: u64 = (1 << 32) - 1;

fn latency_slot(micros: u64) -> usize {
    if micros < EXACT_MICROS {
        return micros as usize;
    }
    let micros = micros.min(LARGEST_SLOTTED_MICROS);
    let octave = micros.ilog2();
    let sub = (micros >> (octave - SUB_BUCKET_BITS)) & (EXACT_MICROS - 1);
    EXACT_MICROS as usize + (octave - SUB_BUCKET_BITS) as usize * 8 + sub as usize
}

/// The largest microsecond value slot `slot` holds.
fn slot_highest_micros(slot: usize) -> u64 {
    if slot < EXACT_MICROS as usize {
        return slot as u64;
    }
    let shift = ((slot - EXACT_MICROS as usize) / 8) as u32;
    let sub = ((slot - EXACT_MICROS as usize) % 8) as u64;
    let lowest = (EXACT_MICROS + sub) << shift;
    lowest + (1 << shift) - 1
}

struct Histogram {
    slots: [AtomicU64; LATENCY_SLOT_COUNT],
    max_micros: AtomicU64,
}

impl Histogram {
    fn new() -> Self {
        Histogram {
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
            max_micros: AtomicU64::new(0),
        }
    }

    fn record(&self, nanos: u64) {
        let micros = nanos / 1_000;
        self.slots[latency_slot(micros)].fetch_add(1, Ordering::Relaxed);
        self.max_micros.fetch_max(micros, Ordering::Relaxed);
    }

    /// Observation count and quantiles, from one pass over the slots.
    fn summary(&self) -> (u64, LatencyQuantiles) {
        let counts: [u64; LATENCY_SLOT_COUNT] =
            std::array::from_fn(|slot| self.slots[slot].load(Ordering::Relaxed));
        let count: u64 = counts.iter().sum();
        let max_micros = self.max_micros.load(Ordering::Relaxed);
        let quantile = |q: f64| -> u64 {
            if count == 0 {
                return 0;
            }
            let rank = ((q * count as f64).ceil() as u64).clamp(1, count);
            let mut seen = 0u64;
            for (slot, n) in counts.iter().enumerate() {
                seen += n;
                if seen >= rank {
                    // The last slot also holds every longer value, so its top
                    // is no bound.
                    if slot == LATENCY_SLOT_COUNT - 1 {
                        return max_micros;
                    }
                    return slot_highest_micros(slot).min(max_micros);
                }
            }
            max_micros
        };
        (
            count,
            LatencyQuantiles {
                p50_micros: quantile(0.50),
                p90_micros: quantile(0.90),
                p99_micros: quantile(0.99),
                max_micros,
            },
        )
    }
}

struct SizeBucketCell {
    first_byte: Histogram,
    total: Histogram,
}

struct Histograms {
    /// `GET_PHASE_COUNT * SIZE_BUCKET_COUNT` cells, phase-major.
    buckets: Box<[SizeBucketCell]>,
    /// One per phase: total latency of the calls that returned `Err`.
    failed: Box<[Histogram]>,
}

impl Histograms {
    fn new() -> Self {
        Histograms {
            buckets: (0..GET_PHASE_COUNT * SIZE_BUCKET_COUNT)
                .map(|_| SizeBucketCell {
                    first_byte: Histogram::new(),
                    total: Histogram::new(),
                })
                .collect(),
            failed: (0..GET_PHASE_COUNT).map(|_| Histogram::new()).collect(),
        }
    }

    fn cell(&self, phase: GetPhase, bucket: usize) -> &SizeBucketCell {
        &self.buckets[phase.index() * SIZE_BUCKET_COUNT + bucket]
    }
}

/// Per-phase, per-size-bucket GET latency histograms. See the
/// [module docs](self).
#[derive(Default)]
pub struct GetLatencyRecorder {
    histograms: OnceLock<Histograms>,
}

impl std::fmt::Debug for GetLatencyRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetLatencyRecorder")
            .field("allocated", &self.histograms.get().is_some())
            .finish()
    }
}

impl GetLatencyRecorder {
    fn histograms(&self) -> &Histograms {
        self.histograms.get_or_init(Histograms::new)
    }

    /// One GET that returned its body: `requested_bytes` picks the size bucket
    /// ([`requested_bytes`]), and both latencies are measured from the same
    /// issue instant.
    pub fn record_ok(
        &self,
        phase: GetPhase,
        requested_bytes: u64,
        first_byte_nanos: u64,
        total_nanos: u64,
    ) {
        let cell = self.histograms().cell(phase, size_bucket(requested_bytes));
        cell.first_byte.record(first_byte_nanos);
        cell.total.record(total_nanos);
    }

    /// One GET that returned `Err` after `total_nanos`.
    pub fn record_failed(&self, phase: GetPhase, total_nanos: u64) {
        self.histograms().failed[phase.index()].record(total_nanos);
    }

    /// Point-in-time quantiles of every cell. Not a consistent cut: a GET
    /// recorded concurrently can land in one histogram of a cell and not yet
    /// the other.
    pub fn snapshot(&self) -> GetLatencySnapshot {
        let phases = GetPhase::ALL.map(|phase| match self.histograms.get() {
            None => PhaseLatencySnapshot::empty(phase),
            Some(histograms) => {
                let buckets = std::array::from_fn(|bucket| {
                    let cell = histograms.cell(phase, bucket);
                    let (count, total) = cell.total.summary();
                    let (_, first_byte) = cell.first_byte.summary();
                    SizeBucketSnapshot {
                        count,
                        first_byte,
                        total,
                    }
                });
                let (failed_count, failed_total) = histograms.failed[phase.index()].summary();
                PhaseLatencySnapshot {
                    phase,
                    buckets,
                    failed_count,
                    failed_total,
                }
            }
        });
        GetLatencySnapshot { phases }
    }
}

/// p50, p90, p99 and max of one latency histogram, in microseconds. All zero
/// for a histogram with no observations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatencyQuantiles {
    pub p50_micros: u64,
    pub p90_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
}

/// One phase's one size bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SizeBucketSnapshot {
    /// Successful GETs in this bucket.
    pub count: u64,
    pub first_byte: LatencyQuantiles,
    pub total: LatencyQuantiles,
}

/// One phase: its size buckets, in [`size_bucket`] order, and its failed GETs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseLatencySnapshot {
    pub phase: GetPhase,
    pub buckets: [SizeBucketSnapshot; SIZE_BUCKET_COUNT],
    /// GETs that returned `Err`, in no size bucket.
    pub failed_count: u64,
    pub failed_total: LatencyQuantiles,
}

impl PhaseLatencySnapshot {
    fn empty(phase: GetPhase) -> Self {
        PhaseLatencySnapshot {
            phase,
            buckets: [SizeBucketSnapshot::default(); SIZE_BUCKET_COUNT],
            failed_count: 0,
            failed_total: LatencyQuantiles::default(),
        }
    }

    /// Successful GETs across every size bucket.
    pub fn ok_count(&self) -> u64 {
        self.buckets.iter().map(|bucket| bucket.count).sum()
    }
}

/// Every phase, in [`GetPhase::ALL`] order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetLatencySnapshot {
    pub phases: [PhaseLatencySnapshot; GET_PHASE_COUNT],
}

impl GetLatencySnapshot {
    pub fn phase(&self, phase: GetPhase) -> &PhaseLatencySnapshot {
        &self.phases[phase.index()]
    }
}

tokio::task_local! {
    /// The phase GETs polled inside an [`InGetPhase::in_get_phase`] future are
    /// attributed to.
    static CURRENT_PHASE: GetPhase;

    /// Where [`mark_first_byte`] leaves the first-byte instant of the GET an
    /// [`InstrumentedStore`](crate::InstrumentedStore) is timing.
    static FIRST_BYTE: FirstByteSlot;
}

struct FirstByteSlot {
    clock: Arc<dyn MonotonicClock>,
    at_nanos: Cell<Option<u64>>,
}

/// Attribute every GET a future issues to a read phase.
pub trait InGetPhase: Future + Sized {
    /// Run `self` with `phase` as the [`current_phase`].
    fn in_get_phase(self, phase: GetPhase) -> TaskLocalFuture<GetPhase, Self> {
        CURRENT_PHASE.scope(phase, self)
    }
}

impl<F: Future> InGetPhase for F {}

/// The phase in scope on this task, or [`GetPhase::Unattributed`] outside
/// every [`InGetPhase::in_get_phase`].
pub fn current_phase() -> GetPhase {
    CURRENT_PHASE
        .try_with(|phase| *phase)
        .unwrap_or(GetPhase::Unattributed)
}

/// Report that the GET being timed has received its response headers. A
/// backend calls this once it holds the response and before it reads the body;
/// only the first call inside one timed GET counts, and a call outside one is a
/// no-op.
pub fn mark_first_byte() {
    let _ = FIRST_BYTE.try_with(|slot| {
        if slot.at_nanos.get().is_none() {
            slot.at_nanos.set(Some(slot.clock.now_nanos()));
        }
    });
}

/// Run `fut` with a fresh first-byte slot installed, returning its output and
/// the clock reading [`mark_first_byte`] took inside it, if any.
pub(crate) async fn observe_first_byte<F: Future>(
    clock: Arc<dyn MonotonicClock>,
    fut: F,
) -> (F::Output, Option<u64>) {
    let slot = FirstByteSlot {
        clock,
        at_nanos: Cell::new(None),
    };
    FIRST_BYTE
        .scope(slot, async move {
            let output = fut.await;
            let at = FIRST_BYTE.with(|slot| slot.at_nanos.get());
            (output, at)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_buckets_split_at_the_documented_bounds() {
        assert_eq!(size_bucket(0), 0);
        assert_eq!(size_bucket(4095), 0);
        assert_eq!(size_bucket(4096), 1);
        assert_eq!(size_bucket(8191), 1);
        assert_eq!(size_bucket(8192), 2);
        assert_eq!(size_bucket(32 * 1024 * 1024 - 1), 13);
        assert_eq!(size_bucket(32 * 1024 * 1024), 14);
        assert_eq!(size_bucket(64 * 1024 * 1024), 14);
        assert_eq!(size_bucket(64 * 1024 * 1024 + 1), 15);
        assert_eq!(size_bucket(u64::MAX), 15);
    }

    /// Every byte count falls inside the bounds of the bucket it is assigned,
    /// and the bounds tile the range with no gap.
    #[test]
    fn size_bucket_bounds_agree_with_size_bucket() {
        let mut next_lowest = 0u64;
        for bucket in 0..SIZE_BUCKET_COUNT {
            let (lowest, highest) = size_bucket_bounds(bucket);
            assert_eq!(
                lowest, next_lowest,
                "bucket {bucket} starts where the last ended"
            );
            assert_eq!(size_bucket(lowest), bucket, "{lowest}");
            match highest {
                Some(highest) => {
                    assert_eq!(size_bucket(highest), bucket, "{highest}");
                    next_lowest = highest + 1;
                }
                None => assert_eq!(bucket, SIZE_BUCKET_COUNT - 1),
            }
        }
    }

    #[test]
    fn latency_slots_are_exact_below_eight_and_monotonic_above() {
        for micros in 0..EXACT_MICROS {
            assert_eq!(slot_highest_micros(latency_slot(micros)), micros);
        }
        let mut last = 0usize;
        for micros in [8u64, 9, 15, 16, 17, 1_000, 70_000, 1 << 31, (1 << 32) - 1] {
            let slot = latency_slot(micros);
            assert!(slot >= last, "{micros}");
            last = slot;
            let highest = slot_highest_micros(slot);
            assert!(highest >= micros, "{micros} above its slot's top {highest}");
            // The slot's top overstates by at most an eighth of the value.
            assert!(highest - micros <= micros / 8, "{micros} -> {highest}");
        }
        assert_eq!(latency_slot(u64::MAX), LATENCY_SLOT_COUNT - 1);
    }

    #[test]
    fn quantiles_take_the_slot_top_capped_at_the_exact_max() {
        let histogram = Histogram::new();
        // 98 observations of 1 ms and two of 100 ms.
        for _ in 0..98 {
            histogram.record(1_000_000);
        }
        histogram.record(100_000_000);
        histogram.record(100_000_000);
        let (count, q) = histogram.summary();
        assert_eq!(count, 100);
        // 1000 us lies in the slot [960, 1023].
        assert_eq!(q.p50_micros, 1_023);
        assert_eq!(q.p90_micros, 1_023);
        assert_eq!(q.p99_micros, 100_000);
        assert_eq!(q.max_micros, 100_000);
    }

    /// A value past the last slot's top is not understated: a quantile in the
    /// last slot reads as the exact max.
    #[test]
    fn a_quantile_in_the_last_slot_is_the_max() {
        let histogram = Histogram::new();
        let two_hours_micros = 2 * 3_600 * 1_000_000u64;
        histogram.record(two_hours_micros * 1_000);
        let (count, q) = histogram.summary();
        assert_eq!(count, 1);
        assert_eq!(q.p50_micros, two_hours_micros);
        assert_eq!(q.max_micros, two_hours_micros);
    }

    #[test]
    fn an_empty_recorder_snapshots_to_zero_without_allocating() {
        let recorder = GetLatencyRecorder::default();
        let snap = recorder.snapshot();
        assert!(recorder.histograms.get().is_none());
        for phase in GetPhase::ALL {
            assert_eq!(snap.phase(phase).ok_count(), 0);
            assert_eq!(snap.phase(phase).failed_count, 0);
        }
    }

    #[test]
    fn requested_bytes_is_the_range_length_not_the_object_size() {
        assert_eq!(requested_bytes(GetRange::Range(100, 4196), 4096), 4096);
        assert_eq!(requested_bytes(GetRange::Suffix(512), 512), 512);
        assert_eq!(requested_bytes(GetRange::Full, 77), 77);
    }

    /// A clock that reads 10, 20, 30, ... one step per reading.
    #[derive(Default)]
    struct SteppingClock(AtomicU64);

    impl MonotonicClock for SteppingClock {
        fn now_nanos(&self) -> u64 {
            self.0.fetch_add(10, Ordering::Relaxed) + 10
        }
    }

    /// Only the first mark inside one timed GET counts (a split S3 read marks
    /// once per request), and a mark outside every timed GET does nothing.
    #[tokio::test]
    async fn the_first_mark_wins_and_a_mark_outside_a_timed_get_is_ignored() {
        let clock = Arc::new(SteppingClock::default());
        mark_first_byte();
        let ((), at) = observe_first_byte(clock.clone(), async {
            mark_first_byte();
            mark_first_byte();
        })
        .await;
        assert_eq!(at, Some(10));
        assert_eq!(
            clock.0.load(Ordering::Relaxed),
            10,
            "the second mark took no reading"
        );
    }

    #[tokio::test]
    async fn an_inner_phase_scope_overrides_an_outer_one() {
        assert_eq!(current_phase(), GetPhase::Unattributed);
        let seen = async {
            let outer = current_phase();
            let inner = async { current_phase() }.in_get_phase(GetPhase::Scan).await;
            (outer, inner, current_phase())
        }
        .in_get_phase(GetPhase::Probe)
        .await;
        assert_eq!(seen, (GetPhase::Probe, GetPhase::Scan, GetPhase::Probe));
    }
}
