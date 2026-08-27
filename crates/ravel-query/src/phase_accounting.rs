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
    fn phase_accessor_matches_named_accessor() {
        let phase = PhaseAccounting::new();
        phase.probe().record_s3_request(AccountedOp::Get);
        let snap = phase.snapshot();
        for p in QueryPhase::ALL {
            assert_eq!(snap.phase(p), match p {
                QueryPhase::Resolve => &snap.resolve,
                QueryPhase::Plan => &snap.plan,
                QueryPhase::Probe => &snap.probe,
                QueryPhase::Scan => &snap.scan,
            });
        }
    }
}
