//! Per-phase split of ADR-0107 decision 4's page-byte pair (epic #913 task T1).
//!
//! [`QueryAccounting`](ravel_types::accounting::QueryAccounting) accumulates
//! `page_bytes_fetched`/`page_bytes_decoded` into one process-wide pair per
//! query, so a byte cannot be attributed to the phase that fetched it. This
//! carries the same two quantities per [`ProbePhase`], the axis this crate
//! already splits probe misses along, and is written from the same call sites
//! that fold the pooled pair, so the phases always sum to the pooled total.
//!
//! # These are stored bytes, not wire bytes
//!
//! `fetched` is the stored size of the pages present in the blocks a statement
//! fetched; `decoded` is the stored size of the pages it actually decoded after
//! column projection. Neither is a transfer figure: the bytes moved over the
//! wire are counted by
//! [`add_s3_bytes`](ravel_types::accounting::QueryAccounting::add_s3_bytes) and
//! reported as `object_store_bytes`. The two kinds must never be summed with or
//! compared against each other.
//!
//! Like [`ProbeMissCounter`](crate::log_fetcher::ProbeMissCounter) this is an
//! accumulator, not a per-query handle: the counters only ever grow, so a caller
//! measuring one execution takes a [`snapshot`](PageByteCounter::snapshot)
//! before and after and reads [`PageByteCounts::saturating_sub`] of the two.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::log_fetcher::ProbePhase;

/// Per-phase stored page bytes accumulated across every scan one fetcher served
/// (epic #913 task T1). Cheap to clone (one `Arc`); every clone of the owning
/// fetcher shares the same counters, exactly as its probe-miss counter does.
#[derive(Debug, Clone, Default)]
pub struct PageByteCounter(Arc<PageByteInner>);

#[derive(Debug, Default)]
struct PageByteInner {
    plan_fetched: AtomicU64,
    plan_decoded: AtomicU64,
    scan_fetched: AtomicU64,
    scan_decoded: AtomicU64,
}

impl PageByteCounter {
    /// New counter with every phase at zero.
    #[must_use]
    pub fn new() -> Self {
        PageByteCounter::default()
    }

    /// Charge one finished scan's stored page bytes to `phase`. Both figures are
    /// taken in one call, from the call site that folds the same pair into the
    /// query's pooled handle, so the two channels cannot drift into disagreeing
    /// accounting. Zero is recorded like any other value: a scan that decoded
    /// nothing must not be skipped, or the counter would only be written on the
    /// paths that already moved bytes.
    pub(crate) fn record(&self, phase: ProbePhase, fetched: u64, decoded: u64) {
        let (f, d) = match phase {
            ProbePhase::Plan => (&self.0.plan_fetched, &self.0.plan_decoded),
            ProbePhase::Scan => (&self.0.scan_fetched, &self.0.scan_decoded),
        };
        f.fetch_add(fetched, Ordering::Relaxed);
        d.fetch_add(decoded, Ordering::Relaxed);
    }

    /// Point-in-time copy of every phase's totals.
    #[must_use]
    pub fn snapshot(&self) -> PageByteCounts {
        PageByteCounts {
            fetched_plan: self.0.plan_fetched.load(Ordering::Relaxed),
            decoded_plan: self.0.plan_decoded.load(Ordering::Relaxed),
            fetched_scan: self.0.scan_fetched.load(Ordering::Relaxed),
            decoded_scan: self.0.scan_decoded.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of a [`PageByteCounter`]'s per-phase totals. Stored page
/// bytes throughout, never wire bytes; see the [module docs](self).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageByteCounts {
    /// Stored bytes of pages present in the blocks [`ProbePhase::Plan`] fetched.
    pub fetched_plan: u64,
    /// Stored bytes of the pages [`ProbePhase::Plan`] decoded after projection.
    pub decoded_plan: u64,
    /// Stored bytes of pages present in the blocks [`ProbePhase::Scan`] fetched.
    pub fetched_scan: u64,
    /// Stored bytes of the pages [`ProbePhase::Scan`] decoded after projection.
    pub decoded_scan: u64,
}

impl PageByteCounts {
    /// Field-wise difference `self - earlier`, the bytes one measured execution
    /// added to a counter that keeps accumulating across executions. Saturating:
    /// a snapshot pair read out of order reports zero rather than wrapping to a
    /// huge figure that would read as a catastrophic decode.
    #[must_use]
    pub fn saturating_sub(&self, earlier: &PageByteCounts) -> PageByteCounts {
        PageByteCounts {
            fetched_plan: self.fetched_plan.saturating_sub(earlier.fetched_plan),
            decoded_plan: self.decoded_plan.saturating_sub(earlier.decoded_plan),
            fetched_scan: self.fetched_scan.saturating_sub(earlier.fetched_scan),
            decoded_scan: self.decoded_scan.saturating_sub(earlier.decoded_scan),
        }
    }

    /// Field-wise saturating sum, for a caller holding one counter per fetcher
    /// (the logs and spans fetchers are separate) that reports one statement's
    /// figures.
    #[must_use]
    pub fn saturating_add(&self, other: &PageByteCounts) -> PageByteCounts {
        PageByteCounts {
            fetched_plan: self.fetched_plan.saturating_add(other.fetched_plan),
            decoded_plan: self.decoded_plan.saturating_add(other.decoded_plan),
            fetched_scan: self.fetched_scan.saturating_add(other.fetched_scan),
            decoded_scan: self.decoded_scan.saturating_add(other.decoded_scan),
        }
    }

    /// Stored fetched page bytes across both phases, which is the quantity the
    /// pooled `page_bytes_fetched` counter holds for the same work.
    #[must_use]
    pub fn total_fetched(&self) -> u64 {
        self.fetched_plan.saturating_add(self.fetched_scan)
    }

    /// Stored decoded page bytes across both phases, which is the quantity the
    /// pooled `page_bytes_decoded` counter holds for the same work.
    #[must_use]
    pub fn total_decoded(&self) -> u64 {
        self.decoded_plan.saturating_add(self.decoded_scan)
    }

    /// One phase's fetched total, named through [`ProbePhase`] rather than by
    /// field.
    #[must_use]
    pub fn fetched(&self, phase: ProbePhase) -> u64 {
        match phase {
            ProbePhase::Plan => self.fetched_plan,
            ProbePhase::Scan => self.fetched_scan,
        }
    }

    /// One phase's decoded total, named through [`ProbePhase`] rather than by
    /// field.
    #[must_use]
    pub fn decoded(&self, phase: ProbePhase) -> u64 {
        match phase {
            ProbePhase::Plan => self.decoded_plan,
            ProbePhase::Scan => self.decoded_scan,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn phases_do_not_contaminate_each_other_and_sum_to_the_pooled_totals() {
        let counter = PageByteCounter::new();
        counter.record(ProbePhase::Plan, 300, 120);
        counter.record(ProbePhase::Scan, 1_000, 400);
        counter.record(ProbePhase::Scan, 7, 7);

        let snap = counter.snapshot();
        assert_eq!(snap.fetched_plan, 300);
        assert_eq!(snap.decoded_plan, 120);
        assert_eq!(snap.fetched_scan, 1_007);
        assert_eq!(snap.decoded_scan, 407);
        assert_eq!(snap.total_fetched(), 1_307);
        assert_eq!(snap.total_decoded(), 527);
        assert_eq!(snap.fetched(ProbePhase::Plan), snap.fetched_plan);
        assert_eq!(snap.decoded(ProbePhase::Scan), snap.decoded_scan);
    }

    #[test]
    fn a_zero_record_still_writes_the_phase() {
        // A scan that decoded nothing is a measured zero, not an absent one:
        // skipping it would leave the counter written only where bytes moved.
        let counter = PageByteCounter::new();
        counter.record(ProbePhase::Scan, 0, 0);
        assert_eq!(counter.snapshot(), PageByteCounts::default());
    }

    #[test]
    fn saturating_sub_is_the_per_execution_share_and_never_wraps() {
        let counter = PageByteCounter::new();
        counter.record(ProbePhase::Scan, 500, 200);
        let before = counter.snapshot();
        counter.record(ProbePhase::Scan, 40, 10);
        counter.record(ProbePhase::Plan, 5, 5);
        let run = counter.snapshot().saturating_sub(&before);
        assert_eq!(run.fetched_scan, 40);
        assert_eq!(run.decoded_scan, 10);
        assert_eq!(run.fetched_plan, 5);

        // Reversed order reports zero rather than wrapping.
        let reversed = before.saturating_sub(&counter.snapshot());
        assert_eq!(reversed, PageByteCounts::default());
    }

    #[test]
    fn saturating_add_combines_two_fetchers_counters() {
        let logs = PageByteCounter::new();
        logs.record(ProbePhase::Scan, 100, 60);
        let spans = PageByteCounter::new();
        spans.record(ProbePhase::Scan, 20, 5);
        spans.record(ProbePhase::Plan, 3, 1);

        let combined = logs.snapshot().saturating_add(&spans.snapshot());
        assert_eq!(combined.fetched_scan, 120);
        assert_eq!(combined.decoded_scan, 65);
        assert_eq!(combined.fetched_plan, 3);
        assert_eq!(combined.decoded_plan, 1);
    }

    #[test]
    fn totals_saturate_instead_of_overflowing() {
        let counts = PageByteCounts {
            fetched_plan: u64::MAX,
            fetched_scan: 10,
            decoded_plan: u64::MAX,
            decoded_scan: 10,
        };
        assert_eq!(counts.total_fetched(), u64::MAX);
        assert_eq!(counts.total_decoded(), u64::MAX);
    }
}
