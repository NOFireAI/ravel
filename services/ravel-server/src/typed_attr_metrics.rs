//! Process-global stale-fallback counter for declared typed attribute columns
//! (ADR-0090 decision 2).
//!
//! [`crate::declared_columns::TenantConfigDeclaredColumns`] resolves each
//! tenant's declared `logs` columns cache-aside and never fails a query on an
//! unreadable `TenantConfig`: it serves the last known good declaration, or the
//! CLI-derived base. That fallback is a real, query-visible degradation (a
//! declaration an operator wrote is not in effect yet, and two replicas can
//! disagree for a horizon), so ADR-0090 requires it to be observable rather
//! than silent. This counter is that observability: every resolution served
//! from a stale entry, a backoff-suppressed read, a failed read, or a
//! malformed durable declaration increments it, and `/metrics` renders it as
//! `ravel_typed_attr_columns_stale_fallback_total`.
//!
//! Process-global with a single source and no per-query state, mirroring
//! [`crate::query_postings_metrics`] (the nearest neighbour: the other
//! cumulative counter family the logs query path publishes) and, before it,
//! [`crate::provisioning::shard_count_mismatch_count`]. The counter only ever
//! increases, so a scrape reads a monotone series. Unconditional, not behind
//! the `sql` feature, so the `/metrics` family exists whether or not this
//! build serves SQL: a scraper's dashboard should not change shape with a
//! compile-time feature. In a build without `sql` nothing increments it and it
//! renders zero, which is the truth for that process.

use std::sync::atomic::{AtomicU64, Ordering};

static STALE_FALLBACKS: AtomicU64 = AtomicU64::new(0);

/// Count one resolution served from a stale cache entry or a failed-read
/// fallback.
pub fn record_stale_fallback() {
    STALE_FALLBACKS.fetch_add(1, Ordering::Relaxed);
}

/// The cumulative stale-fallback count for the `/metrics` renderer.
pub fn stale_fallbacks() -> u64 {
    STALE_FALLBACKS.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The counter is monotone and observes its own increments. Asserted as a
    /// delta, not an absolute: this is process-global state other tests in the
    /// same binary also move.
    #[test]
    fn recording_a_fallback_advances_the_counter() {
        let before = stale_fallbacks();
        record_stale_fallback();
        assert!(stale_fallbacks() > before);
    }
}
