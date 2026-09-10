//! Per-tenant counting of the normalization layer's admission decisions
//! (ADR-0051 section 3, layer 3).
//!
//! Layer 3 enforces structural and event-time bounds per point, record, and
//! span. Until now its decisions were visible only inside an OTLP
//! partial-success response: an operator could see a tenant's data being
//! dropped by a client-side log line, and nothing at all on `/metrics`. This
//! collector records them under the `skew` and `structural` reasons ADR-0051
//! section 6 already reserved on `ravel_admission_rejected_total`.
//!
//! It is a `ravel-server`-local counter, separate from the `ravel-ingest`
//! `AdmissionController`'s per-tenant usage, for the same reason
//! [`crate::ingest_byte_metrics`] is: the controller enforces layers 2 and 4
//! and its usage snapshot has no row for a decision normalization made.
//!
//! Counting happens where the rejection is observed, at the ingest surface
//! that builds the partial-success response, not inside `ravel-otlp`: the
//! normalizer is a pure function that knows no tenant, and both the OTLP and
//! the OTAP surface hand it the same request. That is also what keeps the two
//! transports counting identically, so switching a fleet from OTLP to OTAP
//! does not move this signal.
//!
//! Body conversions are counted here too, and deliberately not as a
//! rejection: a structured log body that was converted to canonical JSON was
//! stored, so an operator alerting on rejection reasons must see nothing from
//! it. It gets its own family.

use std::collections::HashMap;

use parking_lot::Mutex;
use ravel_otlp::NormalizeRejectCounts;
use ravel_types::{Signal, TenantHash, TenantId};

/// Accumulated normalize-layer decisions per `(tenant, signal)`. Keyed by the
/// tenant hash (not the full id), matching how the admission `/metrics` family
/// folds tenants, so the rendered cardinality is bounded the same way.
#[derive(Default)]
pub struct NormalizeRejectMetrics {
    rows: Mutex<HashMap<(TenantHash, Signal), Counts>>,
}

#[derive(Default, Clone, Copy)]
struct Counts {
    skew: u64,
    structural: u64,
    body_conversions: u64,
}

/// One `(tenant, signal)` row of accumulated normalize-layer decisions, for
/// the `/metrics` renderer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantNormalizeRejects {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    /// Points, records, or spans rejected for an event timestamp outside the
    /// admissible window.
    pub skew_total: u64,
    /// Points, records, or spans rejected for a structural bound: an
    /// unsupported type or temporality, a name or attribute over its limit, a
    /// malformed identifier.
    pub structural_total: u64,
    /// Log records admitted after converting a structured body to its
    /// canonical JSON form. Never a rejection: every one of these was stored.
    pub body_conversions_total: u64,
}

impl NormalizeRejectMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one request's normalize-layer rejections to `(tenant, signal)`.
    ///
    /// `counts` is what the rejections in the normalizer's output classify to,
    /// already multiplied out: a rejection that drops a whole scope counts
    /// every point it dropped, matching the `rejected_data_points` the same
    /// request reports back to the sender.
    pub fn record(&self, tenant: &TenantId, signal: Signal, counts: NormalizeRejectCounts) {
        if counts.is_empty() {
            return;
        }
        let mut rows = self.rows.lock();
        let row = rows.entry((tenant.hash(), signal)).or_default();
        row.skew = row.skew.saturating_add(counts.skew as u64);
        row.structural = row.structural.saturating_add(counts.structural as u64);
    }

    /// Adds `count` admitted-after-conversion log records to `(tenant,
    /// signal)`. Separate from [`Self::record`] because a conversion is not a
    /// rejection and must not move a rejection reason.
    pub fn record_body_conversions(&self, tenant: &TenantId, signal: Signal, count: usize) {
        if count == 0 {
            return;
        }
        let mut rows = self.rows.lock();
        let row = rows.entry((tenant.hash(), signal)).or_default();
        row.body_conversions = row.body_conversions.saturating_add(count as u64);
    }

    /// A point-in-time copy of every accumulated row, for `/metrics` rendering.
    pub fn snapshot(&self) -> Vec<TenantNormalizeRejects> {
        self.rows
            .lock()
            .iter()
            .map(|(&(tenant_hash, signal), counts)| TenantNormalizeRejects {
                tenant_hash,
                signal,
                skew_total: counts.skew,
                structural_total: counts.structural,
                body_conversions_total: counts.body_conversions,
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn tenant(name: &str) -> TenantId {
        TenantId::new(name)
    }

    #[test]
    fn empty_counts_create_no_row() {
        let metrics = NormalizeRejectMetrics::new();
        metrics.record(
            &tenant("acme"),
            Signal::Metrics,
            NormalizeRejectCounts::default(),
        );
        metrics.record_body_conversions(&tenant("acme"), Signal::Logs, 0);
        assert!(metrics.snapshot().is_empty());
    }

    #[test]
    fn counts_accumulate_per_tenant_and_signal() {
        let metrics = NormalizeRejectMetrics::new();
        metrics.record(
            &tenant("acme"),
            Signal::Metrics,
            NormalizeRejectCounts {
                skew: 2,
                structural: 3,
            },
        );
        metrics.record(
            &tenant("acme"),
            Signal::Metrics,
            NormalizeRejectCounts {
                skew: 1,
                structural: 0,
            },
        );
        metrics.record(
            &tenant("acme"),
            Signal::Logs,
            NormalizeRejectCounts {
                skew: 0,
                structural: 7,
            },
        );
        metrics.record_body_conversions(&tenant("acme"), Signal::Logs, 4);
        metrics.record(
            &tenant("other"),
            Signal::Metrics,
            NormalizeRejectCounts {
                skew: 5,
                structural: 0,
            },
        );

        let mut rows = metrics.snapshot();
        rows.sort_by_key(|r| (r.tenant_hash, r.signal as u8));
        assert_eq!(rows.len(), 3);

        let acme_metrics = rows
            .iter()
            .find(|r| r.tenant_hash == tenant("acme").hash() && r.signal == Signal::Metrics)
            .expect("acme metrics row");
        assert_eq!(acme_metrics.skew_total, 3);
        assert_eq!(acme_metrics.structural_total, 3);
        assert_eq!(acme_metrics.body_conversions_total, 0);

        let acme_logs = rows
            .iter()
            .find(|r| r.tenant_hash == tenant("acme").hash() && r.signal == Signal::Logs)
            .expect("acme logs row");
        assert_eq!(acme_logs.skew_total, 0);
        assert_eq!(acme_logs.structural_total, 7);
        assert_eq!(acme_logs.body_conversions_total, 4);

        let other = rows
            .iter()
            .find(|r| r.tenant_hash == tenant("other").hash())
            .expect("other tenant row");
        assert_eq!(other.skew_total, 5);
        assert_eq!(other.structural_total, 0);
    }
}
