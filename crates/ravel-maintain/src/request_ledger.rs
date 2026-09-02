//! The compaction read ledger (ADR-0996 task 996-8): a per-run count of the
//! store requests a compaction run issues, and the wire bytes they move, split
//! by the phase that issued them.
//!
//! # Observation only
//!
//! Nothing in this crate ever reads a ledger figure back to make a decision.
//! No fetch is routed, no range is coalesced, no budget is consulted, and no
//! branch anywhere tests a request count or a byte count recorded here. The
//! only condition any call site evaluates is whether a ledger is installed at
//! all ([`CompactorConfig::request_ledger`] being `Some`), exactly as the
//! [`MergeMemoryTracker`] hooks do. ADR-0996 defers every compaction
//! fetch-policy question behind epic #979; this task is counters only.
//!
//! [`CompactorConfig::request_ledger`]: crate::config::CompactorConfig::request_ledger
//! [`MergeMemoryTracker`]: crate::config::MergeMemoryTracker
//!
//! # Attribution is by call site, never inferred
//!
//! Every increment happens at the store call the run already owns, naming its
//! phase there. Nothing is derived afterwards from a pooled total, and no
//! instrumented-store wrapper does the counting in production: a decorator sees
//! `get`/`put`/`head`/`list` and cannot say which phase issued one, which is the
//! whole point of a per-phase split (the repo's measurement rule: a slowdown has
//! to be attributable in one query, not one afternoon). Tests do wrap the store,
//! but only as an ORACLE for the totals this ledger claims.
//!
//! # Byte kinds
//!
//! Each phase reports a request count plus two byte figures that are DIFFERENT
//! KINDS and are never summed with each other:
//!
//! - [`PhaseRequests::wire_bytes_received`]: response payload bytes as
//!   transferred, i.e. the stored (still compressed) form a GET hands back,
//!   including the gap bytes a coalesced range carries.
//! - [`PhaseRequests::wire_bytes_sent`]: request payload bytes as offered to a
//!   PUT, counted whether the store accepted the write or answered
//!   `AlreadyExists`.
//!
//! Neither is a decoded-heap figure: [`crate::config::MergePhasePeaks`] owns
//! decoded residency, and folding the two together would produce a number of no
//! kind at all. LIST and HEAD report requests only; their response bodies are
//! not visible at the [`ObjectStoreBackend`] seam, so both byte fields stay zero
//! for a phase that only issues those.
//!
//! [`ObjectStoreBackend`]: ravel_object_store::ObjectStoreBackend
//!
//! # Requests are calls, not billed attempts
//!
//! A figure here counts logical store calls the compaction path made. ADR-0996
//! decision 3's headline BILLING figure is `StoreMetrics.attempts`, which only
//! the S3 adapter's counting connector can fill in; that seam is below this one
//! and is not duplicated here (the ADR's rejected alternative 4). On a backend
//! with an attempts source, `calls <= attempts`, and the difference is retry
//! overhead this ledger does not see.
//!
//! # Run scope
//!
//! One ledger per concurrent run, like the memory tracker. The run's outermost
//! driver opens the scope with [`RequestLedger::reset_for_run`] before its first
//! store call (the bucket LIST happens before the rewrite primitive is entered,
//! so the rewrite cannot be the resetting point), and
//! [`RequestLedger::reset_for_run_unless_open`] lets a directly driven
//! [`crate::rewrite::rewrite_and_publish`] still get a scope of its own without
//! wiping the LIST an enclosing driver already counted.
//!
//! The instrumented paths are the shared compaction pipeline: the bucket
//! listing, input commit-record reads, per-input catalog reads, the merge's
//! block reads, part PUTs, and the publish protocol. The erasure rewrite
//! (`crate::erasure_rewrite`) reuses several of those helpers, so a ledger
//! installed while it runs collects the shared parts of its traffic and not its
//! own listing or publish; install one per compaction run.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The phase a store request is attributed to. Attribution is by the call site
/// that issued the request, never by operation kind: a HEAD in the publish
/// protocol and a HEAD anywhere else are different phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestPhase {
    /// The bucket listing that discovers the input set (`crate::read::list_bucket`).
    /// One request per listing PAGE, so a paginated drain counts every page.
    List,
    /// The L0 commit-record GETs that decode the input set
    /// (`crate::read::load_inputs`).
    RecordRead,
    /// Per-input catalog reads: the footer suffix probe and any follow-up tail
    /// range, the directory/catalog sections fetched by range, the whole-object
    /// GET the RSEG sparse-catalog decode needs, and the optional
    /// exemplar/postings sections.
    CatalogRead,
    /// Page and block reads during the merge: the RSEG coalesced page-range
    /// GETs and the RLOG/RSPAN cursor pipeline's per-block ranged GETs.
    BlockRead,
    /// L1 part PUT attempts, including one that answers `AlreadyExists` (a
    /// converging rerun still spent the request), and the convergence-repair
    /// re-PUT of a winner's missing part.
    PartPut,
    /// The publish protocol: the compaction record's `CreateIfAbsent` PUT, the
    /// post-publish HEAD verification of `AlreadyExists` parts, and, on the
    /// converging path, the winner-record GET and its per-part HEADs.
    Publish,
}

impl RequestPhase {
    /// Every phase, in report order.
    pub const ALL: [RequestPhase; 6] = [
        RequestPhase::List,
        RequestPhase::RecordRead,
        RequestPhase::CatalogRead,
        RequestPhase::BlockRead,
        RequestPhase::PartPut,
        RequestPhase::Publish,
    ];

    /// Stable snake_case name, used as the tracing field prefix.
    pub fn name(self) -> &'static str {
        match self {
            RequestPhase::List => "list",
            RequestPhase::RecordRead => "record_read",
            RequestPhase::CatalogRead => "catalog_read",
            RequestPhase::BlockRead => "block_read",
            RequestPhase::PartPut => "part_put",
            RequestPhase::Publish => "publish",
        }
    }

    fn index(self) -> usize {
        match self {
            RequestPhase::List => 0,
            RequestPhase::RecordRead => 1,
            RequestPhase::CatalogRead => 2,
            RequestPhase::BlockRead => 3,
            RequestPhase::PartPut => 4,
            RequestPhase::Publish => 5,
        }
    }
}

/// One phase's figures. The two byte fields are different kinds and are never
/// summed with each other, nor with any decoded-heap figure
/// ([`crate::config::MergePhasePeaks`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PhaseRequests {
    /// Store calls this phase issued. A ranged GET is one request, a listing
    /// page is one request, and a PUT that answered `AlreadyExists` is one
    /// request: what is counted is what was spent, not what succeeded.
    pub requests: u64,
    /// Response payload bytes as transferred, summed over this phase's GETs:
    /// the stored (compressed) form the store handed back, including the gap
    /// bytes a coalesced range carries. Zero for a phase that only issues LIST
    /// or HEAD, whose response bodies this seam cannot see. Never a decoded
    /// figure.
    pub wire_bytes_received: u64,
    /// Request payload bytes as offered, summed over this phase's PUTs,
    /// counted whether the store accepted the write or answered
    /// `AlreadyExists`. Encoded object bytes, the same kind a part's
    /// `object_size` records.
    pub wire_bytes_sent: u64,
}

/// One compaction run's request/byte figures split by phase (ADR-0996
/// decision 3's per-phase attribution rule). Read from a [`RequestLedger`] via
/// [`RequestLedger::report`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunRequestReport {
    /// Bucket listing ([`RequestPhase::List`]): requests only.
    pub list: PhaseRequests,
    /// Input commit-record reads ([`RequestPhase::RecordRead`]).
    pub record_read: PhaseRequests,
    /// Per-input catalog reads ([`RequestPhase::CatalogRead`]).
    pub catalog_read: PhaseRequests,
    /// Merge page/block reads ([`RequestPhase::BlockRead`]).
    pub block_read: PhaseRequests,
    /// L1 part PUT attempts ([`RequestPhase::PartPut`]).
    pub part_put: PhaseRequests,
    /// The publish protocol ([`RequestPhase::Publish`]).
    pub publish: PhaseRequests,
}

impl RunRequestReport {
    /// One phase's figures.
    pub fn phase(&self, phase: RequestPhase) -> PhaseRequests {
        match phase {
            RequestPhase::List => self.list,
            RequestPhase::RecordRead => self.record_read,
            RequestPhase::CatalogRead => self.catalog_read,
            RequestPhase::BlockRead => self.block_read,
            RequestPhase::PartPut => self.part_put,
            RequestPhase::Publish => self.publish,
        }
    }

    /// Total store calls across every phase. Requests are one kind, so this sum
    /// is meaningful where a byte sum across phases would not be.
    pub fn total_requests(&self) -> u64 {
        RequestPhase::ALL
            .iter()
            .fold(0u64, |acc, p| acc.saturating_add(self.phase(*p).requests))
    }

    /// Total response payload bytes received across every phase. One kind
    /// (bytes as transferred, GET responses only), so the sum is well defined.
    pub fn total_wire_bytes_received(&self) -> u64 {
        RequestPhase::ALL.iter().fold(0u64, |acc, p| {
            acc.saturating_add(self.phase(*p).wire_bytes_received)
        })
    }

    /// Total request payload bytes sent across every phase. One kind (bytes as
    /// offered, PUT bodies only); deliberately NOT added to
    /// [`Self::total_wire_bytes_received`].
    pub fn total_wire_bytes_sent(&self) -> u64 {
        RequestPhase::ALL.iter().fold(0u64, |acc, p| {
            acc.saturating_add(self.phase(*p).wire_bytes_sent)
        })
    }
}

#[derive(Debug, Default)]
struct PhaseCounters {
    requests: AtomicU64,
    wire_bytes_received: AtomicU64,
    wire_bytes_sent: AtomicU64,
}

#[derive(Debug, Default)]
struct RequestLedgerInner {
    phases: [PhaseCounters; RequestPhase::ALL.len()],
    /// Whether a run's scope is currently open. Read only by
    /// [`RequestLedger::reset_for_run_unless_open`] to decide whether an
    /// enclosing driver already opened one; it never reaches any store or
    /// fetch decision.
    run_open: AtomicBool,
}

/// A test-injectable, cheap-atomics ledger of a compaction run's store requests
/// and wire bytes, split by phase. See the [module docs](self): observation
/// only, attributed at the call site, one ledger per concurrent run.
///
/// Production never installs one ([`crate::config::CompactorConfig::request_ledger`]
/// is `None`), so every hook compiles to a single `Option` check. Installing one
/// is the one-line change that surfaces [`Self::report`] to an operator through
/// the `tracing::info!` event [`crate::rewrite::rewrite_and_publish`] emits.
#[derive(Clone, Debug, Default)]
pub struct RequestLedger {
    inner: Arc<RequestLedgerInner>,
}

impl RequestLedger {
    /// A fresh ledger with every counter at zero and no run open.
    pub fn new() -> Self {
        Self::default()
    }

    /// Account one GET issued by `phase`, with the response payload length it
    /// returned (bytes as transferred).
    pub fn record_get(&self, phase: RequestPhase, wire_bytes_received: u64) {
        let counters = &self.inner.phases[phase.index()];
        counters.requests.fetch_add(1, Ordering::Relaxed);
        counters
            .wire_bytes_received
            .fetch_add(wire_bytes_received, Ordering::Relaxed);
    }

    /// Account one PUT issued by `phase`, with the request payload length it
    /// offered (bytes as transferred). Called whether the PUT was accepted or
    /// answered `AlreadyExists`: both spent the request and both sent the body.
    pub fn record_put(&self, phase: RequestPhase, wire_bytes_sent: u64) {
        let counters = &self.inner.phases[phase.index()];
        counters.requests.fetch_add(1, Ordering::Relaxed);
        counters
            .wire_bytes_sent
            .fetch_add(wire_bytes_sent, Ordering::Relaxed);
    }

    /// Account one metadata request issued by `phase`: a HEAD, or one page of a
    /// LIST drain. Neither payload is visible at the store seam, so only the
    /// request count moves.
    pub fn record_metadata(&self, phase: RequestPhase) {
        self.inner.phases[phase.index()]
            .requests
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Open a run's scope: clear every phase counter and mark a run in
    /// progress. Called by the run's outermost driver
    /// ([`crate::compact::compact_bucket`],
    /// [`crate::rewrite::migrate_bucket_format`]) BEFORE its first store call,
    /// so the bucket LIST that opens a run lands in that run's report instead
    /// of being wiped by the rewrite primitive it precedes.
    ///
    /// Wrong to call mid-run. CONCURRENT runs sharing one ledger still produce
    /// combined figures, and that contract is the installer's to keep, exactly
    /// as for [`crate::config::MergeMemoryTracker::reset_for_run`].
    pub fn reset_for_run(&self) {
        for counters in &self.inner.phases {
            counters.requests.store(0, Ordering::Relaxed);
            counters.wire_bytes_received.store(0, Ordering::Relaxed);
            counters.wire_bytes_sent.store(0, Ordering::Relaxed);
        }
        self.inner.run_open.store(true, Ordering::Relaxed);
    }

    /// Open a run's scope only if no enclosing driver already opened one. This
    /// is what [`crate::rewrite::rewrite_and_publish`] calls: driven under
    /// `compact_bucket` it must keep that run's already-counted LIST and
    /// record reads, and driven directly it must still start from zero.
    pub fn reset_for_run_unless_open(&self) {
        if !self.inner.run_open.load(Ordering::Relaxed) {
            self.reset_for_run();
        }
    }

    /// Close the run's scope. The counters are left intact (a caller reads the
    /// report after the run returns, including after an error); only the next
    /// [`Self::reset_for_run_unless_open`] is affected.
    pub fn end_run(&self) {
        self.inner.run_open.store(false, Ordering::Relaxed);
    }

    /// Whether a run's scope is currently open.
    pub fn run_is_open(&self) -> bool {
        self.inner.run_open.load(Ordering::Relaxed)
    }

    /// The full per-phase split. See [`RunRequestReport`]; do not sum the two
    /// byte kinds.
    pub fn report(&self) -> RunRequestReport {
        let read = |phase: RequestPhase| {
            let counters = &self.inner.phases[phase.index()];
            PhaseRequests {
                requests: counters.requests.load(Ordering::Relaxed),
                wire_bytes_received: counters.wire_bytes_received.load(Ordering::Relaxed),
                wire_bytes_sent: counters.wire_bytes_sent.load(Ordering::Relaxed),
            }
        };
        RunRequestReport {
            list: read(RequestPhase::List),
            record_read: read(RequestPhase::RecordRead),
            catalog_read: read(RequestPhase::CatalogRead),
            block_read: read(RequestPhase::BlockRead),
            part_put: read(RequestPhase::PartPut),
            publish: read(RequestPhase::Publish),
        }
    }
}

/// Record one GET into `ledger` if one is installed, whether or not it
/// succeeded: a failed GET still spent the request, and carried no payload
/// back. Takes the result by reference so a call site keeps its `?`.
pub(crate) fn note_get(
    ledger: Option<&RequestLedger>,
    phase: RequestPhase,
    result: &std::result::Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError>,
) {
    if let Some(l) = ledger {
        l.record_get(phase, result.as_ref().map_or(0, |g| g.data.len() as u64));
    }
}

/// Record one PUT of `payload_len` bytes into `ledger` if one is installed.
/// Called on every outcome: a rejected write (`AlreadyExists`, a failed
/// precondition) still sent the body.
pub(crate) fn note_put(ledger: Option<&RequestLedger>, phase: RequestPhase, payload_len: u64) {
    if let Some(l) = ledger {
        l.record_put(phase, payload_len);
    }
}

/// Record one HEAD, or one listing page, into `ledger` if one is installed.
pub(crate) fn note_metadata(ledger: Option<&RequestLedger>, phase: RequestPhase) {
    if let Some(l) = ledger {
        l.record_metadata(phase);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn phase_indices_are_dense_and_in_report_order() {
        for (expected, phase) in RequestPhase::ALL.iter().enumerate() {
            assert_eq!(phase.index(), expected, "{} index moved", phase.name());
        }
    }

    /// Totals sum within a kind and never across kinds: a report carrying both
    /// received and sent bytes reports each separately, and the request total is
    /// the sum of the per-phase counts.
    #[test]
    fn totals_stay_within_one_byte_kind() {
        let ledger = RequestLedger::new();
        ledger.reset_for_run();
        ledger.record_metadata(RequestPhase::List);
        ledger.record_get(RequestPhase::RecordRead, 100);
        ledger.record_get(RequestPhase::CatalogRead, 250);
        ledger.record_put(RequestPhase::PartPut, 4_000);
        ledger.record_put(RequestPhase::Publish, 70);
        ledger.record_metadata(RequestPhase::Publish);

        let report = ledger.report();
        assert_eq!(report.total_requests(), 6);
        assert_eq!(report.total_wire_bytes_received(), 350);
        assert_eq!(report.total_wire_bytes_sent(), 4_070);
        assert_eq!(report.list.requests, 1);
        assert_eq!(report.list.wire_bytes_received, 0);
        assert_eq!(report.list.wire_bytes_sent, 0);
        assert_eq!(report.publish.requests, 2);
        assert_eq!(report.publish.wire_bytes_sent, 70);
    }

    /// `reset_for_run` clears; `reset_for_run_unless_open` does not clear a
    /// scope an enclosing driver already opened, and does clear once it closed.
    #[test]
    fn run_scope_protects_an_enclosing_drivers_figures() {
        let ledger = RequestLedger::new();
        ledger.reset_for_run();
        ledger.record_metadata(RequestPhase::List);
        ledger.reset_for_run_unless_open();
        assert_eq!(
            ledger.report().list.requests,
            1,
            "a nested reset must not wipe the driver's LIST"
        );

        ledger.end_run();
        ledger.reset_for_run_unless_open();
        assert_eq!(
            ledger.report().list.requests,
            0,
            "once the scope closed, the next entry starts from zero"
        );
        assert!(ledger.run_is_open());
    }
}
