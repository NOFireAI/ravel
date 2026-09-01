//! Per-phase split of [`QueryAccounting`] (issue #796, split out of ADR-0044's
//! pooled handle to unblock #835's diagnosis: one statement issued 18,937 GETs
//! against a 3,469-object tenant and the pooled counter could not say which
//! phase issued them).
//!
//! [`PhaseAccounting`] wraps four independent, unmodified
//! [`QueryAccounting`] handles, one per [`QueryPhase`], rather than adding
//! per-phase fields to `QueryAccounting` itself: that type lives in
//! `ravel-types`, outside this crate's assigned scope for issue #796, and its
//! existing funnels (`ravel-catalog`'s `guarded_get`/`guarded_list_all`, this
//! crate's `fetcher`/`log_fetcher`) already decide, at each call site, which
//! `QueryAccounting` instance a GET is recorded against. Handing that call
//! site the phase's own handle achieves the same per-phase split without an
//! in-place change to a type another crate also owns.
//!
//! # Phase boundaries
//!
//! Matches `docs/query-engine.md`'s own vocabulary:
//!
//! - [`QueryPhase::Resolve`]: the catalog snapshot resolve's commit-record
//!   GETs (`Catalog::resolve_pruned_with_generations` and friends).
//! - [`QueryPhase::Plan`]: footer/skip-index probing to build a fetch plan
//!   (`SegmentFetcher::open_segment`, `LogSegmentFetcher::plan_segment`).
//! - [`QueryPhase::Probe`]: segment catalog fetch -- whole-object, sparse
//!   catalog-probe, or whole-object-fallback (`SegmentFetcher::decode_selected`).
//! - [`QueryPhase::Scan`]: the actual block/page/chunk data reads
//!   (`SegmentFetcher::fetch_scalar_pages`/`fetch_histogram_pages`,
//!   `LogSegmentFetcher::scan_accounted_with_tenant`).
//!
//! Decode (turning fetched bytes into typed samples/rows) is deliberately not
//! a fifth phase here: it issues no store request, so it has no request or
//! wire-byte counters to attribute. Its own byte figures
//! (`decompressed_bytes`, `page_bytes_fetched`/`page_bytes_decoded`) stay on
//! whichever handle a decode call site is passed -- by convention the
//! [`QueryPhase::Scan`] handle, since decode always follows a scan in this
//! crate's fetch pipelines -- and are never summed into a phase's request or
//! wire-byte totals.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ravel_types::accounting::{QueryAccounting, QueryAccountingSnapshot};

/// The phases a query's object-store cost is split across (issue #796). See
/// the [module docs](self) for what each phase covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryPhase {
    /// Catalog snapshot resolve.
    Resolve,
    /// Footer/skip-index probing to build a fetch plan.
    Plan,
    /// Segment catalog fetch (whole-object, sparse probe, or fallback).
    Probe,
    /// Block/page/chunk data reads.
    Scan,
}

impl QueryPhase {
    /// Every phase, in the fixed order [`PhaseAccountingSnapshot`]'s fields
    /// and [`PhaseAccounting::snapshot`] agree on.
    pub const ALL: [QueryPhase; 4] = [
        QueryPhase::Resolve,
        QueryPhase::Plan,
        QueryPhase::Probe,
        QueryPhase::Scan,
    ];

    /// Lower-case, stable name for a report table column or a JSON key.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            QueryPhase::Resolve => "resolve",
            QueryPhase::Plan => "plan",
            QueryPhase::Probe => "probe",
            QueryPhase::Scan => "scan",
        }
    }

    /// This phase's position in [`Self::ALL`], which is also its slot in
    /// [`PhaseWireByteCounter`]'s array.
    #[must_use]
    pub fn index(self) -> usize {
        match self {
            QueryPhase::Resolve => 0,
            QueryPhase::Plan => 1,
            QueryPhase::Probe => 2,
            QueryPhase::Scan => 3,
        }
    }
}

/// Accumulating per-[`QueryPhase`] count of WIRE bytes -- bytes a store GET
/// actually transferred -- across every read one fetcher served (issue #913).
///
/// # Why this is not a field on `QueryAccounting`
///
/// `QueryAccounting` already totals wire bytes per operation kind, but it is
/// one pooled handle per query: `ravel-sql`'s executor creates a single
/// `QueryAccounting` per attempt and hands the same handle to the catalog
/// resolve, the plan reads, and the scan reads alike, so the pooled total
/// cannot say which phase moved the bytes. [`PhaseAccounting`] solves that by
/// handing each phase its own handle, but only the callers that already thread
/// one can use it, and the SQL read path threads the pooled handle instead.
/// This counter is the channel that reaches a caller which cannot change that
/// threading: it is owned by the fetcher, shared by every clone, and read the
/// same way `ravel_query::ProbeMissCounter` is -- [`snapshot`](Self::snapshot)
/// before and after one execution, then
/// [`PhaseWireByteCounts::saturating_sub`].
///
/// # What lands where
///
/// The phase a byte is charged to is the phase of the request that moved it,
/// decided at the GET call site. For the RLOG read path
/// (`ravel_query::ReadPhases`):
///
/// - [`QueryPhase::Plan`]: every GET a planning read issues -- the footer probe,
///   the footer chase, SKIP_IDX/PAGE_DIR/FIELD_DIR, and the whole-object plan
///   fallback. A planning read fetches no block data, so all of its bytes are
///   plan bytes.
/// - [`QueryPhase::Probe`]: the metadata GETs of a DATA read -- its suffix
///   probe, footer chase, SKIP_IDX, PAGE_DIR, the front directories, BLOOM and
///   POSTINGS, and the object's trailing bytes.
/// - [`QueryPhase::Scan`]: the BLOCKS-section data ranges of a data read, and a
///   whole-object GET a data read issued to obtain block data.
/// - [`QueryPhase::Resolve`]: never written by the RLOG read path. The catalog
///   snapshot resolve's commit-record GETs are issued by `ravel-catalog`, which
///   has no handle on this counter.
///
/// Cache hits move no bytes over the wire and are recorded here as nothing at
/// all; they stay on `QueryAccounting::cache_bytes`, which is a different
/// quantity from every figure in this type.
#[derive(Debug, Clone, Default)]
pub struct PhaseWireByteCounter(Arc<PhaseWireByteInner>);

#[derive(Debug, Default)]
struct PhaseWireByteInner {
    /// One slot per [`QueryPhase`], indexed by [`QueryPhase::index`].
    phases: [AtomicU64; 4],
    /// GET REQUEST count per phase, same indexing. Recorded beside the byte
    /// charge at every fetch site, so the two figures describe the same GETs
    /// (issue #857: a per-phase byte split whose request counts stay pooled
    /// cannot say whether a phase is few large reads or many small ones).
    requests: [AtomicU64; 4],
}

impl PhaseWireByteCounter {
    /// New counter with every phase at zero.
    #[must_use]
    pub fn new() -> Self {
        PhaseWireByteCounter::default()
    }

    /// Add `bytes` wire bytes to `phase`. Called from the same place the same
    /// bytes are recorded onto a `QueryAccounting` handle, so the two channels
    /// cannot drift: every byte this counter holds is also in that handle's
    /// GET total, and the per-phase figures sum back to it.
    pub fn record(&self, phase: QueryPhase, bytes: u64) {
        self.0.phases[phase.index()].fetch_add(bytes, Ordering::Relaxed);
        // One call here is one store GET (the fetch sites call once per
        // request), so the request count rides the same call and cannot
        // drift from the byte charge.
        self.0.requests[phase.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Point-in-time copy of every phase's total.
    #[must_use]
    pub fn snapshot(&self) -> PhaseWireByteCounts {
        let mut out = PhaseWireByteCounts::default();
        for phase in QueryPhase::ALL {
            out.phases[phase.index()] = self.0.phases[phase.index()].load(Ordering::Relaxed);
            out.requests[phase.index()] = self.0.requests[phase.index()].load(Ordering::Relaxed);
        }
        out
    }
}

/// Point-in-time copy of a [`PhaseWireByteCounter`]'s per-phase totals
/// (issue #913). Every figure is WIRE bytes: what a store GET transferred,
/// including a coalesced run's unwanted bytes and any retry. Never stored
/// bytes, never decompressed bytes, and never cache-served bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PhaseWireByteCounts {
    /// One slot per [`QueryPhase`], indexed by [`QueryPhase::index`].
    phases: [u64; 4],
    /// GET request count per phase, same indexing (issue #857).
    requests: [u64; 4],
}

impl PhaseWireByteCounts {
    /// Wire bytes charged to `phase`.
    #[must_use]
    pub fn phase(&self, phase: QueryPhase) -> u64 {
        self.phases[phase.index()]
    }

    /// GET requests charged to `phase` (issue #857): the request count the
    /// byte figure above was accumulated over, recorded by the same call.
    #[must_use]
    pub fn phase_requests(&self, phase: QueryPhase) -> u64 {
        self.requests[phase.index()]
    }

    /// Field-wise difference `self - earlier`: the wire bytes one measured
    /// execution added to a counter that keeps accumulating. Saturating, so a
    /// snapshot pair read out of order reports zero rather than wrapping.
    #[must_use]
    pub fn saturating_sub(&self, earlier: &PhaseWireByteCounts) -> PhaseWireByteCounts {
        let mut out = PhaseWireByteCounts::default();
        for phase in QueryPhase::ALL {
            out.phases[phase.index()] =
                self.phases[phase.index()].saturating_sub(earlier.phases[phase.index()]);
            out.requests[phase.index()] =
                self.requests[phase.index()].saturating_sub(earlier.requests[phase.index()]);
        }
        out
    }

    /// Wire bytes across every phase.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.phases.iter().fold(0u64, |a, b| a.saturating_add(*b))
    }

    /// Sum of every phase's GET request count (issue #857).
    #[must_use]
    pub fn total_requests(&self) -> u64 {
        self.requests.iter().fold(0u64, |a, b| a.saturating_add(*b))
    }
}

/// Four independent [`QueryAccounting`] handles, one per [`QueryPhase`]
/// (issue #796). Cheap to clone: each phase handle is itself an `Arc`-backed
/// clone, exactly like `QueryAccounting`, so cloning the whole struct clones
/// four `Arc`s and nothing else. Created once per query attempt, alongside
/// the query's other fresh per-attempt state (see `engine.rs`'s
/// `resolve_snapshot_with_retry`), and passed explicitly into every component
/// that touches the object store on that phase's behalf -- never held in a
/// task-local, for the same reason `QueryAccounting` itself is not (see
/// `ravel_types::accounting`'s module docs).
#[derive(Debug, Clone, Default)]
pub struct PhaseAccounting {
    resolve: QueryAccounting,
    plan: QueryAccounting,
    probe: QueryAccounting,
    scan: QueryAccounting,
}

impl PhaseAccounting {
    /// New handle with every phase's every counter at zero, for one query
    /// attempt.
    #[must_use]
    pub fn new() -> Self {
        PhaseAccounting::default()
    }

    /// The resolve phase's handle.
    #[must_use]
    pub fn resolve(&self) -> &QueryAccounting {
        &self.resolve
    }

    /// The plan phase's handle.
    #[must_use]
    pub fn plan(&self) -> &QueryAccounting {
        &self.plan
    }

    /// The probe phase's handle.
    #[must_use]
    pub fn probe(&self) -> &QueryAccounting {
        &self.probe
    }

    /// The scan phase's handle. Decode call sites downstream of a scan (see
    /// the [module docs](self)) are also passed this handle, for their
    /// decode-only byte counters.
    #[must_use]
    pub fn scan(&self) -> &QueryAccounting {
        &self.scan
    }

    /// The handle for a given phase. Used where a call site's phase is
    /// chosen dynamically rather than named directly.
    #[must_use]
    pub fn phase(&self, phase: QueryPhase) -> &QueryAccounting {
        match phase {
            QueryPhase::Resolve => &self.resolve,
            QueryPhase::Plan => &self.plan,
            QueryPhase::Probe => &self.probe,
            QueryPhase::Scan => &self.scan,
        }
    }

    /// Point-in-time copy of every phase's counters.
    #[must_use]
    pub fn snapshot(&self) -> PhaseAccountingSnapshot {
        PhaseAccountingSnapshot {
            resolve: self.resolve.snapshot(),
            plan: self.plan.snapshot(),
            probe: self.probe.snapshot(),
            scan: self.scan.snapshot(),
        }
    }
}

/// Point-in-time copy of a [`PhaseAccounting`]'s four phase snapshots (issue
/// #796). Plain `QueryAccountingSnapshot` fields, so this is `Copy` and needs
/// no allocation, mirroring `QueryAccountingSnapshot` itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PhaseAccountingSnapshot {
    pub resolve: QueryAccountingSnapshot,
    pub plan: QueryAccountingSnapshot,
    pub probe: QueryAccountingSnapshot,
    pub scan: QueryAccountingSnapshot,
}

impl PhaseAccountingSnapshot {
    /// This snapshot's phase, named directly rather than through
    /// [`QueryPhase::ALL`]'s iteration order.
    #[must_use]
    pub fn phase(&self, phase: QueryPhase) -> &QueryAccountingSnapshot {
        match phase {
            QueryPhase::Resolve => &self.resolve,
            QueryPhase::Plan => &self.plan,
            QueryPhase::Probe => &self.probe,
            QueryPhase::Scan => &self.scan,
        }
    }

    /// Field-wise saturating sum of every phase, exactly the number a caller
    /// would have gotten from one pooled `QueryAccounting` handle before
    /// issue #796 split it by phase. Every existing caller of the old pooled
    /// `accounting: QueryAccountingSnapshot` field keeps reading this value
    /// (`QueryStats::accounting` is computed as `phase_accounting.pooled()`),
    /// so splitting the counters changes no existing number.
    #[must_use]
    pub fn pooled(&self) -> QueryAccountingSnapshot {
        self.resolve
            .saturating_add(&self.plan)
            .saturating_add(&self.probe)
            .saturating_add(&self.scan)
    }
}

#[cfg(test)]
mod tests {
    use ravel_types::accounting::AccountedOp;

    use super::*;

    #[test]
    fn fresh_handle_is_all_zero() {
        let phase = PhaseAccounting::new();
        let snap = phase.snapshot();
        assert_eq!(snap, PhaseAccountingSnapshot::default());
        assert_eq!(snap.pooled(), QueryAccountingSnapshot::default());
    }

    #[test]
    fn each_phase_handle_is_independent() {
        let phase = PhaseAccounting::new();
        phase.resolve().record_s3_request(AccountedOp::Get);
        phase.plan().record_s3_request(AccountedOp::Get);
        phase.plan().record_s3_request(AccountedOp::Get);
        phase.probe().add_s3_bytes(AccountedOp::Get, 100);
        phase.scan().add_s3_bytes(AccountedOp::Get, 7);
        phase.scan().record_s3_request(AccountedOp::Get);

        let snap = phase.snapshot();
        assert_eq!(snap.resolve.s3_requests(AccountedOp::Get), 1);
        assert_eq!(snap.plan.s3_requests(AccountedOp::Get), 2);
        assert_eq!(snap.probe.s3_bytes(AccountedOp::Get), 100);
        assert_eq!(snap.scan.s3_requests(AccountedOp::Get), 1);
        assert_eq!(snap.scan.s3_bytes(AccountedOp::Get), 7);

        let pooled = snap.pooled();
        assert_eq!(pooled.s3_requests(AccountedOp::Get), 4);
        assert_eq!(pooled.s3_bytes(AccountedOp::Get), 107);
    }

    #[test]
    fn pooled_uses_max_not_sum_for_the_non_additive_peak() {
        // Every other pooled test uses GET requests and GET bytes, which are
        // additive, so none of them can tell a field-wise sum from correct
        // pooled semantics. `peak_intermediate_bytes` is a high-water mark:
        // two phases each peaking at 100 never held 200 at once. This fails
        // if `saturating_add` is ever changed to add that field.
        let phase = PhaseAccounting::new();
        phase.plan().observe_intermediate_bytes(100);
        phase.scan().observe_intermediate_bytes(60);

        let pooled = phase.snapshot().pooled();
        assert_eq!(
            pooled.peak_intermediate_bytes, 100,
            "pooled peak must be the max across phases, not their sum"
        );
        assert_ne!(
            pooled.peak_intermediate_bytes, 160,
            "a field-wise sum of the peaks would report memory never held"
        );
    }

    #[test]
    fn wire_byte_counter_keeps_each_phase_separate_and_sums_to_the_total() {
        let wire = PhaseWireByteCounter::new();
        wire.record(QueryPhase::Probe, 7_000);
        wire.record(QueryPhase::Scan, 30);
        wire.record(QueryPhase::Scan, 12);

        let snap = wire.snapshot();
        assert_eq!(snap.phase(QueryPhase::Resolve), 0);
        assert_eq!(snap.phase(QueryPhase::Plan), 0);
        assert_eq!(snap.phase(QueryPhase::Probe), 7_000);
        assert_eq!(snap.phase(QueryPhase::Scan), 42);
        assert_eq!(snap.total(), 7_042);
    }

    #[test]
    fn wire_byte_delta_is_one_executions_share_of_an_accumulator() {
        let wire = PhaseWireByteCounter::new();
        wire.record(QueryPhase::Scan, 100);
        let before = wire.snapshot();
        wire.record(QueryPhase::Scan, 30);
        wire.record(QueryPhase::Probe, 7_000);

        let delta = wire.snapshot().saturating_sub(&before);
        assert_eq!(delta.phase(QueryPhase::Scan), 30, "the second read only");
        assert_eq!(delta.phase(QueryPhase::Probe), 7_000);
        assert_eq!(delta.total(), 7_030);

        // Read out of order: zero rather than a wrapped enormous figure that
        // would present as a catastrophic amplification.
        assert_eq!(before.saturating_sub(&wire.snapshot()).total(), 0);
    }

    #[test]
    fn every_phase_has_its_own_wire_byte_slot() {
        // A `QueryPhase::index` collision would silently pool two phases; this
        // fails on the pooled figure rather than on a name.
        let wire = PhaseWireByteCounter::new();
        for (i, phase) in QueryPhase::ALL.iter().enumerate() {
            wire.record(*phase, 1 << i);
        }
        let snap = wire.snapshot();
        for (i, phase) in QueryPhase::ALL.iter().enumerate() {
            assert_eq!(snap.phase(*phase), 1 << i, "{} slot", phase.name());
        }
        assert_eq!(snap.total(), 15);
    }

    #[test]
    fn phase_accessor_matches_named_accessor() {
        let phase = PhaseAccounting::new();
        phase.probe().record_s3_request(AccountedOp::Get);
        let snap = phase.snapshot();
        for p in QueryPhase::ALL {
            assert_eq!(
                snap.phase(p),
                match p {
                    QueryPhase::Resolve => &snap.resolve,
                    QueryPhase::Plan => &snap.plan,
                    QueryPhase::Probe => &snap.probe,
                    QueryPhase::Scan => &snap.scan,
                }
            );
        }
    }
}
