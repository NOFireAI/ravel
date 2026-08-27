//! The tenant-delegating memory pool bridge.
//!
//! DataFusion memory pools are per-`RuntimeEnv`, not hierarchical. Ravel needs
//! two nested budgets: a per-query byte ceiling and a per-tenant ceiling that
//! outlives any single query and is shared across a tenant's concurrent
//! queries. [`TenantDelegatingPool`] is the bridge: it is the `MemoryPool`
//! installed on the query's `RuntimeEnv`, it enforces the per-query ceiling
//! locally, and it forwards every `grow`/`try_grow`/`shrink` to the
//! [`TenantMemoryAccountant`] so tenant usage is accounted across queries.
//!
//! The forwarding of `shrink` is load-bearing for cancellation:
//! DataFusion frees a `MemoryReservation` on `Drop`, which calls
//! `MemoryPool::shrink`. A cancelled, timed-out, or client-disconnected query
//! drops its streams, so every reservation shrinks to zero and the tenant's
//! reserved bytes return to zero without any explicit cleanup path. A pool that
//! did not forward `shrink` would leak tenant budget on every cancellation.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use ravel_types::accounting::QueryAccounting;

/// A one-shot per-query abort flag for the best-effort memory ceiling.
///
/// It is set once [`TenantDelegatingPool::grow`]'s unconditional path has
/// already pushed a budget over its ceiling. Because `grow` cannot decline or
/// clamp a reservation (see the note on that method: `MemoryReservation` does
/// `pool.grow` then an unconditional local increment, so a pool that grows a
/// different amount than asked desyncs the reservation's own accounting), this
/// flag cannot prevent the overshoot. It only records that the overshoot
/// happened so the query's stream can notice at its next poll and abort,
/// rather than run to completion over budget. This is a detect-after-the-fact
/// signal, not a guard.
///
/// The message is set on the first trip only (whichever ceiling was breached
/// first wins); later trips are no-ops, because the query is already aborting.
#[derive(Debug, Default)]
pub struct CeilingBreach {
    message: OnceLock<String>,
}

impl CeilingBreach {
    /// A fresh, untripped breach flag.
    pub fn new() -> Arc<Self> {
        Arc::new(CeilingBreach::default())
    }

    /// Record `message` as the breach reason, but only on the first call;
    /// subsequent trips are ignored (the query is already aborting, and the
    /// first ceiling to overshoot is the one worth reporting).
    fn trip(&self, message: String) {
        // `set` returns Err when already set; a second breach is expected and
        // deliberately dropped.
        let _ = self.message.set(message);
    }

    /// The breach reason, once tripped; `None` while no ceiling has been
    /// overshot.
    pub fn message(&self) -> Option<&str> {
        self.message.get().map(String::as_str)
    }
}

/// Per-tenant memory accountant: a byte counter with a ceiling, shared (via
/// `Arc`) across every query a tenant runs concurrently. Independent of
/// DataFusion; the query's [`TenantDelegatingPool`] forwards into it.
///
/// This is a ravel-sql-local stand-in for Ravel's tenant accountant: nothing
/// tenant-wide exists to delegate to yet (there is no cross-crate accountant
/// type), so this crate defines the shape here. When a process-wide accountant lands,
/// this becomes a thin adapter over it; the pool bridge above does not change.
#[derive(Debug)]
pub struct TenantMemoryAccountant {
    limit: usize,
    used: AtomicUsize,
}

impl TenantMemoryAccountant {
    /// A tenant accountant capped at `limit` bytes.
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(TenantMemoryAccountant {
            limit,
            used: AtomicUsize::new(0),
        })
    }

    /// Bytes currently reserved across all of this tenant's queries.
    pub fn reserved(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    /// The tenant ceiling.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Reserve `additional` bytes if doing so stays at or under the ceiling.
    /// Returns `Err(used)` (reserving nothing) when it would overflow the
    /// tenant budget, where `used` is the bytes reserved at the moment of
    /// refusal so the caller can name the pool's occupancy, not just the
    /// rejected delta. CAS loop so concurrent queries account correctly.
    fn try_grow(&self, additional: usize) -> Result<(), usize> {
        let mut cur = self.used.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_add(additional);
            if next > self.limit {
                return Err(cur);
            }
            match self
                .used
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(observed) => cur = observed,
            }
        }
    }

    /// Reserve `additional` bytes unconditionally (mirrors the infallible
    /// `MemoryPool::grow`, which the trait requires never fail). Returns the
    /// new total so the caller can compare it against the ceiling and trip a
    /// [`CeilingBreach`] without a second racy load.
    fn grow(&self, additional: usize) -> usize {
        self.used
            .fetch_add(additional, Ordering::AcqRel)
            .saturating_add(additional)
    }

    /// Release `amount` bytes, saturating at zero so a double-shrink or an
    /// accounting drift can never underflow.
    fn shrink(&self, amount: usize) {
        let mut cur = self.used.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_sub(amount);
            match self
                .used
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }
}

/// The query's DataFusion `MemoryPool`: enforces a per-query byte ceiling and
/// forwards all accounting to a shared [`TenantMemoryAccountant`].
///
/// `try_grow` fails if either budget is exhausted, and it checks the query
/// budget first so a high-cardinality query trips its own pool before it can
/// threaten the tenant budget (the ordering the sizing test asserts).
/// A failed `try_grow` reserves nothing on either budget.
pub struct TenantDelegatingPool {
    query_limit: usize,
    query_used: AtomicUsize,
    tenant: Arc<TenantMemoryAccountant>,
    /// Tripped when `grow`'s unconditional path pushes either budget over its
    /// ceiling. Shared with the query's stream, which reads it each poll and
    /// aborts once it is set.
    breach: Arc<CeilingBreach>,
    /// The query this pool was built for (ADR-0044 "1. A per-request
    /// accounting handle"). Every successful grow reports the query's new
    /// reserved high-water mark to `observe_intermediate_bytes`, so
    /// `peak_intermediate_bytes` reflects this pool's own reservations
    /// rather than staying zero, as it does on the PromQL path today.
    accounting: QueryAccounting,
}

impl fmt::Debug for TenantDelegatingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantDelegatingPool")
            .field("query_limit", &self.query_limit)
            .field("query_used", &self.query_used.load(Ordering::Relaxed))
            .field("tenant_reserved", &self.tenant.reserved())
            .field("tenant_limit", &self.tenant.limit())
            .finish()
    }
}

impl fmt::Display for TenantDelegatingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TenantDelegatingPool(query_limit={}, tenant_limit={})",
            self.query_limit,
            self.tenant.limit()
        )
    }
}

impl TenantDelegatingPool {
    /// A pool capped at `query_limit` bytes that delegates to `tenant` and
    /// trips `breach` if `grow`'s unconditional path overshoots either ceiling.
    /// `accounting` receives this query's reserved-bytes high-water mark on
    /// every successful grow.
    pub fn new(
        query_limit: usize,
        tenant: Arc<TenantMemoryAccountant>,
        breach: Arc<CeilingBreach>,
        accounting: QueryAccounting,
    ) -> Self {
        TenantDelegatingPool {
            query_limit,
            query_used: AtomicUsize::new(0),
            tenant,
            breach,
            accounting,
        }
    }

    /// The tenant accountant this pool delegates to (for tests and the
    /// endpoint that owns the tenant budget).
    pub fn tenant(&self) -> &Arc<TenantMemoryAccountant> {
        &self.tenant
    }

    /// Reserve `additional` against the query budget only. CAS loop; returns
    /// the new query total on success, and `Err(used)` reserving nothing on
    /// overflow, where `used` is the bytes reserved at the moment of refusal.
    fn query_try_grow(&self, additional: usize) -> Result<usize, usize> {
        let mut cur = self.query_used.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_add(additional);
            if next > self.query_limit {
                return Err(cur);
            }
            match self.query_used.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(next),
                Err(observed) => cur = observed,
            }
        }
    }

    fn query_shrink(&self, amount: usize) {
        let mut cur = self.query_used.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_sub(amount);
            match self.query_used.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }
}

impl MemoryPool for TenantDelegatingPool {
    fn name(&self) -> &str {
        "TenantDelegatingPool"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        // The trait requires this to be infallible, and both budgets grow
        // unconditionally -- this is not only reachable after a validated
        // try_grow. datafusion 54.1.0's MemoryReservation::resize() and at
        // least the nested-loop and sort-merge join operators call grow
        // directly with a delta that was never checked against either
        // ceiling (confirmed against the pinned datafusion source; matches
        // upstream GreedyMemoryPool::grow, which has the same shape). Do
        // not "fix" this by clamping or declining here: MemoryReservation
        // itself does `pool.grow` then an unconditional local size
        // increment, so a pool that grows a different amount than it was
        // asked desyncs the reservation's own accounting, and the eventual
        // `free()` on drop would then over-release into shrink's
        // saturating floor, undercounting the tenant. Both budgets are
        // therefore best-effort ceilings once joins reach this pool (B3+),
        // not hard caps against every DataFusion-internal growth path.
        // Accepted and documented: ADR-0013 amendment (2026-07-28).
        //
        // What the ceiling check adds: after the (still unconditional) increments,
        // note whether this grow pushed a budget over its ceiling and, if so,
        // trip the shared CeilingBreach. This does not prevent the overshoot
        // -- it cannot, per the paragraph above -- it lets the query's stream
        // notice at its next poll and abort rather than run over budget. Only
        // one of the two checks trips per call (whichever ceiling was actually
        // breached); the query check is first so a query overrunning its own
        // budget is reported against the per-query ceiling, not the tenant's.
        let query_total = self
            .query_used
            .fetch_add(additional, Ordering::AcqRel)
            .saturating_add(additional);
        self.accounting
            .observe_intermediate_bytes(query_total as u64);
        let tenant_total = self.tenant.grow(additional);
        if query_total > self.query_limit {
            self.breach.trip(format!(
                "query memory ceiling breached: {query_total} bytes reserved exceeds \
                 per-query limit {}",
                self.query_limit
            ));
        } else if tenant_total > self.tenant.limit() {
            self.breach.trip(format!(
                "tenant memory ceiling breached: {tenant_total} bytes reserved exceeds \
                 tenant limit {}",
                self.tenant.limit()
            ));
        }
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        // Forwarded to the tenant accountant, including the shrink DataFusion
        // issues when a MemoryReservation is dropped: this is what
        // makes a cancelled or dropped stream return its tenant reservation to
        // zero.
        self.query_shrink(shrink);
        self.tenant.shrink(shrink);
    }

    fn try_grow(&self, _reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        // Query budget first: a query must trip its own pool before it can
        // threaten the tenant budget.
        let query_total = self.query_try_grow(additional).map_err(|used| {
            DataFusionError::ResourcesExhausted(format!(
                "query memory pool exhausted: {additional} more bytes on top of {used} \
                 already reserved exceeds per-query limit {}",
                self.query_limit
            ))
        })?;
        if let Err(used) = self.tenant.try_grow(additional) {
            // Roll the query reservation back so a tenant-budget failure
            // leaves nothing reserved on either budget.
            self.query_shrink(additional);
            return Err(DataFusionError::ResourcesExhausted(format!(
                "tenant memory budget exhausted: {additional} more bytes on top of {used} \
                 already reserved exceeds tenant limit {}",
                self.tenant.limit()
            )));
        }
        // Observed only once BOTH budgets have granted the growth. Recording it
        // before the tenant check leaves a peak for bytes that no successful
        // reservation ever held, because the query reservation above is rolled
        // back on a tenant refusal: the peak would outlive the allocation.
        self.accounting
            .observe_intermediate_bytes(query_total as u64);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.query_used.load(Ordering::Acquire)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.query_limit)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use datafusion::execution::memory_pool::MemoryConsumer;
    use ravel_types::accounting::QueryAccounting;

    use super::*;

    /// Mirrors `sql3_f01`'s construction (tests/audit_sql3_exec.rs): a `grow`
    /// that pushes the query total past the per-query ceiling trips the
    /// breach, and the message names the query ceiling and the two numbers.
    #[test]
    fn grow_past_the_query_ceiling_trips_the_breach_naming_the_query() {
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let breach = CeilingBreach::new();
        let pool: Arc<dyn MemoryPool> = Arc::new(TenantDelegatingPool::new(
            1024,
            Arc::clone(&tenant),
            Arc::clone(&breach),
            QueryAccounting::new(),
        ));
        let res = MemoryConsumer::new("ceiling-query").register(&pool);

        // grow is infallible and cannot decline; the overshoot happens, and
        // the breach records it.
        res.grow(4096);
        let message = breach
            .message()
            .expect("query ceiling overshoot must trip the breach");
        assert!(
            message.contains("query") && message.contains("per-query"),
            "message must name the query ceiling: {message}"
        );
        assert!(
            message.contains("4096") && message.contains("1024"),
            "message must name the reserved bytes and the limit: {message}"
        );
        // The tenant ceiling was not breached, so the tenant half of the
        // message never appears.
        assert!(
            !message.contains("tenant"),
            "only the query ceiling was breached: {message}"
        );
    }

    /// A `grow` that stays within the query limit but pushes the tenant total
    /// past the tenant limit trips the breach against the tenant ceiling.
    #[test]
    fn grow_past_only_the_tenant_ceiling_names_the_tenant() {
        let tenant = TenantMemoryAccountant::new(1024);
        let breach = CeilingBreach::new();
        // A generous per-query limit so the same grow clears it but overruns
        // the small tenant ceiling.
        let pool: Arc<dyn MemoryPool> = Arc::new(TenantDelegatingPool::new(
            1 << 30,
            Arc::clone(&tenant),
            Arc::clone(&breach),
            QueryAccounting::new(),
        ));
        let res = MemoryConsumer::new("ceiling-tenant").register(&pool);

        res.grow(4096);
        let message = breach
            .message()
            .expect("tenant ceiling overshoot must trip the breach");
        assert!(
            message.contains("tenant"),
            "message must name the tenant ceiling: {message}"
        );
        assert!(
            message.contains("4096") && message.contains("1024"),
            "message must name the reserved bytes and the limit: {message}"
        );
    }

    /// A `grow` that stays under both ceilings leaves the breach untripped.
    #[test]
    fn grow_within_both_ceilings_leaves_the_breach_untripped() {
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let breach = CeilingBreach::new();
        let pool: Arc<dyn MemoryPool> = Arc::new(TenantDelegatingPool::new(
            1 << 30,
            Arc::clone(&tenant),
            Arc::clone(&breach),
            QueryAccounting::new(),
        ));
        let res = MemoryConsumer::new("ceiling-none").register(&pool);

        res.grow(4096);
        assert!(
            breach.message().is_none(),
            "a grow under both ceilings must not trip the breach"
        );
    }

    /// `grow` reports the query's reserved high-water mark to the accounting
    /// handle, and a later shrink does not pull the recorded peak back down
    /// (`observe_intermediate_bytes` is a maximum, never a sum or a gauge).
    #[test]
    fn grow_then_shrink_leaves_peak_intermediate_bytes_at_the_high_water_mark() {
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let breach = CeilingBreach::new();
        let accounting = QueryAccounting::new();
        let pool: Arc<dyn MemoryPool> = Arc::new(TenantDelegatingPool::new(
            1 << 30,
            Arc::clone(&tenant),
            Arc::clone(&breach),
            accounting.clone(),
        ));
        let res = MemoryConsumer::new("peak-bytes").register(&pool);

        res.try_grow(4096).expect("within both ceilings");
        res.grow(2048);
        assert_eq!(accounting.snapshot().peak_intermediate_bytes, 6144);

        res.shrink(5000);
        assert_eq!(
            accounting.snapshot().peak_intermediate_bytes,
            6144,
            "a shrink must not lower the recorded peak"
        );
    }
}
