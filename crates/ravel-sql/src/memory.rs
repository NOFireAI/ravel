//! The tenant-delegating memory pool bridge (docs/arrow-datafusion-plan.md
//! section 2 "Per-tenant accounting", review F13).
//!
//! DataFusion memory pools are per-`RuntimeEnv`, not hierarchical. Ravel needs
//! two nested budgets: a per-query byte ceiling and a per-tenant ceiling that
//! outlives any single query and is shared across a tenant's concurrent
//! queries. [`TenantDelegatingPool`] is the bridge: it is the `MemoryPool`
//! installed on the query's `RuntimeEnv`, it enforces the per-query ceiling
//! locally, and it forwards every `grow`/`try_grow`/`shrink` to the
//! [`TenantMemoryAccountant`] so tenant usage is accounted across queries.
//!
//! The forwarding of `shrink` is load-bearing for cancellation (review F13):
//! DataFusion frees a `MemoryReservation` on `Drop`, which calls
//! `MemoryPool::shrink`. A cancelled, timed-out, or client-disconnected query
//! drops its streams, so every reservation shrinks to zero and the tenant's
//! reserved bytes return to zero without any explicit cleanup path. A pool that
//! did not forward `shrink` would leak tenant budget on every cancellation.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};

/// Per-tenant memory accountant: a byte counter with a ceiling, shared (via
/// `Arc`) across every query a tenant runs concurrently. Independent of
/// DataFusion; the query's [`TenantDelegatingPool`] forwards into it.
///
/// This is a ravel-sql-local stand-in for Ravel's tenant accountant: nothing
/// tenant-wide exists to delegate to yet (there is no cross-crate accountant
/// type), so B2 defines the shape here. When a process-wide accountant lands,
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
    /// Returns `Err` (reserving nothing) when it would overflow the tenant
    /// budget. CAS loop so concurrent queries account correctly.
    fn try_grow(&self, additional: usize) -> Result<(), ()> {
        let mut cur = self.used.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_add(additional);
            if next > self.limit {
                return Err(());
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
    /// `MemoryPool::grow`, which the trait requires never fail).
    fn grow(&self, additional: usize) {
        self.used.fetch_add(additional, Ordering::AcqRel);
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
/// threaten the tenant budget (the ordering review F10's sizing test asserts).
/// A failed `try_grow` reserves nothing on either budget.
pub struct TenantDelegatingPool {
    query_limit: usize,
    query_used: AtomicUsize,
    tenant: Arc<TenantMemoryAccountant>,
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
    /// A pool capped at `query_limit` bytes that delegates to `tenant`.
    pub fn new(query_limit: usize, tenant: Arc<TenantMemoryAccountant>) -> Self {
        TenantDelegatingPool {
            query_limit,
            query_used: AtomicUsize::new(0),
            tenant,
        }
    }

    /// The tenant accountant this pool delegates to (for tests and the
    /// endpoint that owns the tenant budget).
    pub fn tenant(&self) -> &Arc<TenantMemoryAccountant> {
        &self.tenant
    }

    /// Reserve `additional` against the query budget only. CAS loop; returns
    /// `Err` reserving nothing on overflow.
    fn query_try_grow(&self, additional: usize) -> Result<(), ()> {
        let mut cur = self.query_used.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_add(additional);
            if next > self.query_limit {
                return Err(());
            }
            match self.query_used.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
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
        // The trait requires this to be infallible; both budgets grow
        // unconditionally. DataFusion uses this only after a successful
        // reservation, so it never crosses a ceiling in practice.
        self.query_used.fetch_add(additional, Ordering::AcqRel);
        self.tenant.grow(additional);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        // Forwarded to the tenant accountant, including the shrink DataFusion
        // issues when a MemoryReservation is dropped (review F13): this is what
        // makes a cancelled or dropped stream return its tenant reservation to
        // zero.
        self.query_shrink(shrink);
        self.tenant.shrink(shrink);
    }

    fn try_grow(&self, _reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        // Query budget first: a query must trip its own pool before it can
        // threaten the tenant budget (review F10).
        self.query_try_grow(additional).map_err(|()| {
            DataFusionError::ResourcesExhausted(format!(
                "query memory pool exhausted: {additional} more bytes exceeds per-query limit {}",
                self.query_limit
            ))
        })?;
        if self.tenant.try_grow(additional).is_err() {
            // Roll the query reservation back so a tenant-budget failure
            // leaves nothing reserved on either budget.
            self.query_shrink(additional);
            return Err(DataFusionError::ResourcesExhausted(format!(
                "tenant memory budget exhausted: {additional} more bytes exceeds tenant limit {}",
                self.tenant.limit()
            )));
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.query_used.load(Ordering::Acquire)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.query_limit)
    }
}
