//! Per-phase object-store accounting (issue #796).
//!
//! [`QueryAccountingSnapshot`] (ravel-types) is a single pooled counter set:
//! every GET a query issues, from catalog resolution down to page decode,
//! lands in the same `s3_requests`/`s3_bytes` arrays. That answers "how much
//! did this query cost" but not "which part of the query path spent it",
//! which is what a plan-shape regression (a scan reading catalog-sized
//! bytes, a probe issuing scan-sized ranges) needs to localize.
//!
//! [`PhaseAccounting`] does not change what `QueryAccounting` records or how;
//! it is four independent handles, one per [`Phase`], and the fetch paths
//! that already call `QueryAccounting::record_s3_request`/`add_s3_bytes` at
//! their existing funnels (`fetcher.rs`'s `store_get`) now do so against
//! whichever phase's handle their caller selected. `ravel-types`,
//! `ravel-catalog`, and `ravel-sql` are unmodified: every accounted call site
//! this crate does not own keeps taking a plain `&QueryAccounting` exactly as
//! before, and a plain `QueryAccounting` still satisfies
//! [`PhaseAccountingSource`] (see below), so it costs nothing extra to route
//! a resolve-phase call into `ravel-catalog::Catalog::resolve_pruned_*` --
//! that function's signature never changes, only which handle instance the
//! caller passes it.
//!
//! The pooled total is never recorded separately: [`PhaseAccountingSnapshot::pooled`]
//! derives it as the field-wise sum of the four phase snapshots
//! (`QueryAccountingSnapshot::saturating_add`), so "pooled equals the sum of
//! the phases" holds by construction, not by two independent recordings
//! staying in sync.

use ravel_types::accounting::{QueryAccounting, QueryAccountingSnapshot};

/// The four phases a metrics (RSEG) query's object-store traffic is split
/// into. Decode (typed-sample materialization from already-fetched bytes) is
/// deliberately not a phase here: it issues no GET, so folding it in would
/// make a phase's byte count answer "how many bytes did this step touch"
/// instead of "how many bytes crossed the network for this step" -- see
/// [`QueryAccountingSnapshot::decompressed_bytes`]/`page_bytes_decoded` for
/// the decode-side figures, which stay on the pooled snapshot only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    /// Catalog snapshot resolution: the commit-record and generation-history
    /// reads `ravel-catalog::Catalog::resolve_pruned_with_generations` issues
    /// to turn a query window into a pruned segment list, before any segment
    /// is opened.
    Resolve,
    /// Catalog-section reads inside an already-open segment that determine
    /// which series/pages exist and match the query's selectors, before any
    /// sample-page GET: `fetcher.rs`'s `decode_selected`/`decode_sparse_catalog`
    /// (LABEL_DICT, SERIES_IDS, SERIES_META or the sparse SERIES_IDX +
    /// SERIES_META_CHUNKS pair), the RSEG counterpart of the log path's
    /// FIELD_DIR read.
    Plan,
    /// The suffix GET (and footer `NeedRange` chase, if the suffix did not
    /// carry the whole footer) that opens a segment and establishes its etag:
    /// `fetcher.rs`'s `open_segment`. Runs before `Plan`, on every segment a
    /// query opens, whether or not any series in it ultimately matches.
    Probe,
    /// Coalesced sample-page range GETs for the series `Plan` selected:
    /// `fetcher.rs`'s `fetch_scalar_pages`/`fetch_histogram_pages`. The only
    /// phase whose GET volume scales with matched series and time range
    /// rather than with segment count.
    Scan,
}

/// A source of per-call accounting handles: either a single flat
/// [`QueryAccounting`] (every phase routes to the same handle, matching
/// today's pooled-only behavior exactly) or a real [`PhaseAccounting`] split.
/// `fetcher.rs`'s phase-aware entry points take `&dyn PhaseAccountingSource`
/// so the shared internals (`open_segment`, `decode_selected`,
/// `fetch_scalar_pages`, ...) need no changes at all: they still each take a
/// plain `&QueryAccounting`, obtained here via [`Self::for_phase`].
pub trait PhaseAccountingSource: Send + Sync {
    fn for_phase(&self, phase: Phase) -> &QueryAccounting;
}

/// A plain `QueryAccounting` is a valid (degenerate) phase source: every
/// phase resolves to the same handle. This is what lets `fetch_series_impl`
/// serve both the existing unphased public entry points (`fetch_series_accounted`,
/// unchanged behavior) and the new phase-aware ones from one body.
impl PhaseAccountingSource for QueryAccounting {
    fn for_phase(&self, _phase: Phase) -> &QueryAccounting {
        self
    }
}

/// Four independent [`QueryAccounting`] handles, one per [`Phase`], created
/// once per query attempt alongside (not instead of) the pooled handle
/// callers outside this crate still expect.
#[derive(Debug, Clone)]
pub struct PhaseAccounting {
    resolve: QueryAccounting,
    plan: QueryAccounting,
    probe: QueryAccounting,
    scan: QueryAccounting,
}

impl Default for PhaseAccounting {
    fn default() -> Self {
        Self::new()
    }
}

impl PhaseAccounting {
    pub fn new() -> Self {
        PhaseAccounting {
            resolve: QueryAccounting::new(),
            plan: QueryAccounting::new(),
            probe: QueryAccounting::new(),
            scan: QueryAccounting::new(),
        }
    }

    /// The live handle for one phase, for a call site that already knows
    /// which phase it is (most callers: `open_segment` is always `Probe`,
    /// `decode_selected` is always `Plan`, regardless of who calls them).
    pub fn handle(&self, phase: Phase) -> &QueryAccounting {
        match phase {
            Phase::Resolve => &self.resolve,
            Phase::Plan => &self.plan,
            Phase::Probe => &self.probe,
            Phase::Scan => &self.scan,
        }
    }

    /// Scrapes all four handles into a plain-value snapshot.
    pub fn snapshot(&self) -> PhaseAccountingSnapshot {
        PhaseAccountingSnapshot {
            resolve: self.resolve.snapshot(),
            plan: self.plan.snapshot(),
            probe: self.probe.snapshot(),
            scan: self.scan.snapshot(),
        }
    }
}

impl PhaseAccountingSource for PhaseAccounting {
    fn for_phase(&self, phase: Phase) -> &QueryAccounting {
        self.handle(phase)
    }
}

/// Point-in-time copy of a [`PhaseAccounting`]'s four handles. Each field's
/// `s3_requests`/`s3_bytes` carries the exact same semantics as
/// [`QueryAccountingSnapshot::s3_bytes`]: wire bytes of GETs that actually
/// completed against the store (a failed GET is never recorded, matching
/// `store_get`'s doc comment), no separate accounting for a retried attempt
/// (only the attempt that returns `Ok` records), and a cache hit is not
/// counted here at all -- it lands in that phase's own `cache_hits`/
/// `cache_bytes` instead, since no store round trip happened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PhaseAccountingSnapshot {
    /// See [`Phase::Resolve`].
    pub resolve: QueryAccountingSnapshot,
    /// See [`Phase::Plan`].
    pub plan: QueryAccountingSnapshot,
    /// See [`Phase::Probe`].
    pub probe: QueryAccountingSnapshot,
    /// See [`Phase::Scan`].
    pub scan: QueryAccountingSnapshot,
}

impl PhaseAccountingSnapshot {
    /// The pooled snapshot today's callers already read
    /// (`QueryStats.accounting`, `SqlOutcome.accounting`): the field-wise sum
    /// of all four phases, via `QueryAccountingSnapshot::saturating_add`.
    /// Derived, never recorded separately, so it equals the phase sum by
    /// construction rather than by two counters staying in sync.
    pub fn pooled(&self) -> QueryAccountingSnapshot {
        self.resolve
            .saturating_add(&self.plan)
            .saturating_add(&self.probe)
            .saturating_add(&self.scan)
    }
}
