//! The tenant-delegating memory pool bridge.
//!
//! DataFusion memory pools are per-`RuntimeEnv`, not hierarchical. Ravel needs
//! three nested budgets: a per-query byte ceiling, a per-tenant ceiling that
//! outlives any single query and is shared across a tenant's concurrent
//! queries, and one process-wide ceiling every tenant draws from (ADR-1170
//! decision 1). [`TenantDelegatingPool`] is the bridge: it is the `MemoryPool`
//! installed on the query's `RuntimeEnv`, it enforces the per-query ceiling
//! locally, and it forwards every `grow`/`try_grow`/`shrink` to the
//! [`TenantMemoryAccountant`] so tenant usage is accounted across queries; the
//! accountant forwards the same delta to the process-wide
//! [`ravel_memory::MemoryBudget`] it adapts.
//!
//! The forwarding of `shrink` is load-bearing for cancellation:
//! DataFusion frees a `MemoryReservation` on `Drop`, which calls
//! `MemoryPool::shrink`. A cancelled, timed-out, or client-disconnected query
//! drops its streams, so every reservation shrinks to zero and the tenant's
//! reserved bytes return to zero without any explicit cleanup path. A pool that
//! did not forward `shrink` would leak tenant budget on every cancellation.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use ravel_memory::{MemoryBudget, MemoryExhausted};
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

/// Which ledger refused a forwarded [`TenantMemoryAccountant::try_grow`], with
/// the figures that explain the refusal. A refusal leaves both ledgers exactly
/// as it found them: the tenant charge is rolled back before a process refusal
/// is returned, and the process reserved nothing on its own refusal.
#[derive(Debug)]
enum GrowRefused {
    /// The tenant ceiling refused. `used` is the tenant bytes reserved at the
    /// moment of refusal.
    Tenant { used: usize },
    /// The process budget refused, after the tenant charge was rolled back.
    /// `tenant_held` is the tenant total read BEFORE that rollback, so the
    /// message reports what this tenant held when it was refused rather than
    /// what it holds after the refused reservation was returned.
    Process {
        exhausted: MemoryExhausted,
        tenant_held: usize,
    },
}

/// The totals both ledgers reached after an infallible
/// [`TenantMemoryAccountant::grow`], so the caller can compare each against its
/// own ceiling without a second racy load.
#[derive(Debug, Clone, Copy)]
struct GrowTotals {
    tenant: usize,
    process: u64,
}

/// Per-tenant memory accountant: a byte counter with a ceiling, shared (via
/// `Arc`) across every query a tenant runs concurrently. Independent of
/// DataFusion; the query's [`TenantDelegatingPool`] forwards into it.
///
/// This is the adapter over the process-wide [`MemoryBudget`] (ADR-1170
/// decision 1): every charge the pool bridge above makes is applied to the
/// tenant counter and forwarded to the process counter with the same delta,
/// tenant then process on the way up and process then tenant on the way down,
/// so the per-tenant ceilings are a fairness bound nested inside one process
/// ceiling rather than N ceilings that multiply with the number of active
/// tenants. The tenant ceiling keeps its own meaning and its own refusal.
///
/// A budget built with [`MemoryBudget::unlimited`] counts every charge and
/// refuses none, which is the default until a caller installs a bounded one.
#[derive(Debug)]
pub struct TenantMemoryAccountant {
    limit: usize,
    used: AtomicUsize,
    budget: Arc<MemoryBudget>,
    /// Bytes THIS accountant currently holds on `budget`. The process counter
    /// is shared, and `MemoryBudget::release` saturates rather than reporting
    /// an over-release, so a shrink larger than this accountant's own charge
    /// would hand another tenant's bytes back to the budget and make the
    /// process figure under-report. Releasing `min(amount, outstanding)`
    /// bounds the damage to this tenant's own ledger. A correct DataFusion
    /// cannot over-shrink (its `MemoryReservation::shrink` panics on an
    /// over-free before it reaches the pool), so this is containment, not a
    /// live bug fix: a cross-tenant safety property should not rest on a
    /// dependency's internal invariant.
    process_outstanding: AtomicU64,
}

impl TenantMemoryAccountant {
    /// A tenant accountant capped at `limit` bytes whose charges are counted
    /// against a private unlimited process budget, so it refuses on the tenant
    /// ceiling alone.
    ///
    /// [`Self::with_process_budget`] is the constructor that shares one budget
    /// across a process's tenants; this one exists for callers (tests in this
    /// and other crates) that only exercise the tenant ceiling.
    pub fn new(limit: usize) -> Arc<Self> {
        TenantMemoryAccountant::with_process_budget(limit, Arc::new(MemoryBudget::unlimited()))
    }

    /// A tenant accountant capped at `limit` bytes that also charges `budget`,
    /// the process-wide accountant shared by every tenant in this process.
    pub fn with_process_budget(limit: usize, budget: Arc<MemoryBudget>) -> Arc<Self> {
        Arc::new(TenantMemoryAccountant {
            limit,
            used: AtomicUsize::new(0),
            budget,
            process_outstanding: AtomicU64::new(0),
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

    /// Bytes currently reserved on the process-wide budget, across every
    /// tenant sharing it (and every non-SQL component charging it).
    pub fn process_reserved(&self) -> u64 {
        self.budget.reserved()
    }

    /// The process-wide ceiling. `u64::MAX` under the unlimited default.
    pub fn process_limit(&self) -> u64 {
        self.budget.limit()
    }

    /// Reserve `additional` bytes on the tenant counter if doing so stays at or
    /// under the tenant ceiling, then on the process budget. Reserves nothing
    /// on either ledger when it returns `Err`: a tenant refusal never reaches
    /// the process budget, and a process refusal rolls the tenant charge back
    /// before returning (the process reserved nothing to release, per
    /// `MemoryBudget::try_reserve`'s all-or-nothing rule).
    fn try_grow(&self, additional: usize) -> Result<(), GrowRefused> {
        self.tenant_try_grow(additional)
            .map_err(|used| GrowRefused::Tenant { used })?;
        if let Err(exhausted) = self.budget.try_reserve(additional as u64) {
            // Read the tenant total BEFORE the rollback: the refusal message
            // reports how much this tenant held when it was refused, and after
            // the rollback that figure excludes the very reservation being
            // refused (a single-query tenant would report zero).
            let tenant_held = self.reserved();
            self.tenant_shrink(additional);
            return Err(GrowRefused::Process {
                exhausted,
                tenant_held,
            });
        }
        self.process_outstanding
            .fetch_add(additional as u64, Ordering::AcqRel);
        Ok(())
    }

    /// The tenant half of [`Self::try_grow`]: reserve `additional` bytes if
    /// doing so stays at or under the ceiling. Returns `Err(used)` (reserving
    /// nothing) when it would overflow the tenant budget, where `used` is the
    /// bytes reserved at the moment of refusal so the caller can name the
    /// pool's occupancy, not just the rejected delta. CAS loop so concurrent
    /// queries account correctly.
    fn tenant_try_grow(&self, additional: usize) -> Result<(), usize> {
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

    /// Reserve `additional` bytes unconditionally on both ledgers (mirrors the
    /// infallible `MemoryPool::grow`, which the trait requires never fail).
    /// Returns both new totals so the caller can compare each against its
    /// ceiling and trip a [`CeilingBreach`] without a second racy load.
    fn grow(&self, additional: usize) -> GrowTotals {
        let tenant = self
            .used
            .fetch_add(additional, Ordering::AcqRel)
            .saturating_add(additional);
        let process = self.budget.reserve_unchecked(additional as u64);
        self.process_outstanding
            .fetch_add(additional as u64, Ordering::AcqRel);
        GrowTotals { tenant, process }
    }

    /// Release `amount` bytes from the process budget and then the tenant
    /// counter, the reverse of the order [`Self::try_grow`] charges them in.
    fn shrink(&self, amount: usize) {
        self.release_process_at_most(amount as u64);
        self.tenant_shrink(amount);
    }

    /// Release at most this accountant's own outstanding process charge, so an
    /// oversized shrink cannot reach bytes another tenant reserved. CAS loop
    /// because `process_outstanding` and the release must move by the same
    /// amount even when two of this tenant's queries shrink concurrently.
    fn release_process_at_most(&self, amount: u64) {
        let mut cur = self.process_outstanding.load(Ordering::Acquire);
        loop {
            let release = amount.min(cur);
            if release == 0 {
                return;
            }
            match self.process_outstanding.compare_exchange_weak(
                cur,
                cur - release,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.budget.release(release);
                    return;
                }
                Err(observed) => cur = observed,
            }
        }
    }

    /// Bytes this accountant currently holds on the process budget. Equal to
    /// [`Self::reserved`] in the absence of accounting drift; they are tracked
    /// separately so an oversized shrink is contained to this tenant.
    pub fn process_outstanding(&self) -> u64 {
        self.process_outstanding.load(Ordering::Acquire)
    }

    /// The tenant half of [`Self::shrink`], saturating at zero so a
    /// double-shrink or an accounting drift can never underflow. Also the
    /// rollback for a process refusal, which must leave the process counter
    /// alone because nothing was reserved there.
    fn tenant_shrink(&self, amount: usize) {
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
/// `try_grow` fails if any of the three budgets is exhausted (query, tenant,
/// then the process-wide [`MemoryBudget`] the accountant adapts), and it checks
/// the query budget first so a high-cardinality query trips its own pool before
/// it can threaten the tenant budget (the ordering the sizing test asserts).
/// A failed `try_grow` reserves nothing on any of them.
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
            .field("process_reserved", &self.tenant.process_reserved())
            .field("process_limit", &self.tenant.process_limit())
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
        // one of the three checks trips per call (whichever ceiling was
        // actually breached); the query check is first so a query overrunning
        // its own budget is reported against the per-query ceiling, not the
        // tenant's, and the process check is last because it is the widest
        // ceiling: a query or tenant overshoot names the narrower budget that
        // it also crossed.
        let query_total = self
            .query_used
            .fetch_add(additional, Ordering::AcqRel)
            .saturating_add(additional);
        self.accounting
            .observe_intermediate_bytes(query_total as u64);
        let totals = self.tenant.grow(additional);
        if query_total > self.query_limit {
            self.breach.trip(format!(
                "query memory ceiling breached: {query_total} bytes reserved exceeds \
                 per-query limit {}",
                self.query_limit
            ));
        } else if totals.tenant > self.tenant.limit() {
            self.breach.trip(format!(
                "tenant memory ceiling breached: {} bytes reserved exceeds \
                 tenant limit {}",
                totals.tenant,
                self.tenant.limit()
            ));
        } else if totals.process > self.tenant.process_limit() {
            self.breach.trip(format!(
                "process memory ceiling breached: {} bytes reserved exceeds \
                 process limit {}",
                totals.process,
                self.tenant.process_limit()
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
        if let Err(refused) = self.tenant.try_grow(additional) {
            // Roll the query reservation back so a tenant- or process-budget
            // failure leaves nothing reserved on any of the three budgets. The
            // accountant has already rolled its own tenant charge back on a
            // process refusal, and the process reserved nothing to release.
            self.query_shrink(additional);
            let message = match refused {
                GrowRefused::Tenant { used } => format!(
                    "tenant memory budget exhausted: {additional} more bytes on top of {used} \
                     already reserved exceeds tenant limit {}",
                    self.tenant.limit()
                ),
                // No tenant identifier here, only figures: the process budget
                // is shared, so this message is read by an operator looking at
                // process-wide pressure, and the tenant figures are what say
                // how much of it this tenant held.
                GrowRefused::Process {
                    exhausted,
                    tenant_held,
                } => format!(
                    "process memory budget exhausted: {additional} more bytes on top of {} \
                     already reserved exceeds process limit {} (tenant reserved {} of \
                     tenant limit {})",
                    exhausted.reserved,
                    exhausted.limit,
                    tenant_held,
                    self.tenant.limit()
                ),
            };
            return Err(DataFusionError::ResourcesExhausted(message));
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

    /// A pool over `tenant` with ceilings wide enough that only the process
    /// budget can refuse or breach.
    fn pool_over(
        tenant: Arc<TenantMemoryAccountant>,
        breach: Arc<CeilingBreach>,
    ) -> Arc<dyn MemoryPool> {
        Arc::new(TenantDelegatingPool::new(
            1 << 30,
            tenant,
            breach,
            QueryAccounting::new(),
        ))
    }

    /// ADR-1170 decision 1: two tenants over ONE process budget. B's 50 bytes
    /// fit B's own 60-byte tenant ceiling, and are refused anyway because A
    /// already holds 60 of the process budget's 100. The refusal names the
    /// process figures, and it reserves nothing anywhere: B's tenant counter is
    /// back at zero, B's query counter is rolled back, and the process still
    /// holds exactly A's 60 bytes (a process refusal reserves nothing, so there
    /// is nothing to release from it).
    #[test]
    fn a_process_refusal_names_the_process_figures_and_rolls_the_tenant_back() {
        let budget = Arc::new(MemoryBudget::new(100));
        let tenant_a = TenantMemoryAccountant::with_process_budget(60, Arc::clone(&budget));
        let tenant_b = TenantMemoryAccountant::with_process_budget(60, Arc::clone(&budget));
        let pool_a = pool_over(Arc::clone(&tenant_a), CeilingBreach::new());
        let pool_b = pool_over(Arc::clone(&tenant_b), CeilingBreach::new());
        let res_a = MemoryConsumer::new("tenant-a").register(&pool_a);
        let res_b = MemoryConsumer::new("tenant-b").register(&pool_b);

        res_a
            .try_grow(60)
            .expect("60 fits A's tenant ceiling and the process budget");
        assert_eq!(budget.reserved(), 60);

        let err = res_b
            .try_grow(50)
            .expect_err("50 fits B's tenant ceiling but not the process budget's remaining 40");
        let DataFusionError::ResourcesExhausted(message) = &err else {
            panic!("a process refusal must stay a ResourcesExhausted; got {err:?}");
        };
        assert_eq!(
            message,
            "process memory budget exhausted: 50 more bytes on top of 60 already reserved \
             exceeds process limit 100 (tenant reserved 50 of tenant limit 60)",
            "the tenant figure is read before the rollback, so it names what B held \
             when it was refused, not what it holds after the refused charge came back"
        );

        assert_eq!(
            tenant_b.reserved(),
            0,
            "the refused tenant charge is rolled back"
        );
        assert_eq!(
            pool_b.reserved(),
            0,
            "the refused query charge is rolled back"
        );
        assert_eq!(
            budget.reserved(),
            60,
            "a process refusal reserves nothing, so only A's bytes are held"
        );
        assert_eq!(tenant_a.reserved(), 60, "A's charge is untouched");
    }

    /// An oversized shrink releases only this accountant's own outstanding
    /// process charge, so it cannot hand another tenant's bytes back to the
    /// shared budget.
    ///
    /// FLIP: in `shrink`, call `self.budget.release(amount as u64)` directly
    /// instead of `release_process_at_most`; the process counter then reads 0
    /// and B's 40 bytes are gone from the budget while B still holds them.
    #[test]
    fn an_oversized_shrink_cannot_release_another_tenants_process_bytes() {
        let budget = Arc::new(MemoryBudget::new(100));
        let tenant_a = TenantMemoryAccountant::with_process_budget(60, Arc::clone(&budget));
        let tenant_b = TenantMemoryAccountant::with_process_budget(60, Arc::clone(&budget));

        tenant_a.try_grow(40).expect("40 fits both ledgers");
        tenant_b.try_grow(40).expect("40 fits both ledgers");
        assert_eq!(budget.reserved(), 80);

        // Larger than A's own charge: DataFusion cannot produce this, the
        // containment exists so a future caller that does cannot corrupt the
        // shared counter.
        tenant_a.shrink(100);

        assert_eq!(
            budget.reserved(),
            40,
            "only A's own 40 bytes leave the process budget"
        );
        assert_eq!(
            tenant_b.reserved(),
            40,
            "B's tenant counter is untouched by A's shrink"
        );
        assert_eq!(
            tenant_a.process_outstanding(),
            0,
            "A now holds nothing on the process budget"
        );
    }

    /// The tracked outstanding charge returns to zero across a matching
    /// grow/shrink pair, and a shrink with nothing outstanding releases
    /// nothing at all.
    ///
    /// FLIP: drop the `if release == 0 { return; }` guard in
    /// `release_process_at_most` and the CAS spins on an unchanged value; drop
    /// the `fetch_add` in `try_grow` and the pair leaves the budget at 40.
    #[test]
    fn a_matching_grow_and_shrink_leave_nothing_outstanding() {
        let budget = Arc::new(MemoryBudget::new(100));
        let tenant = TenantMemoryAccountant::with_process_budget(60, Arc::clone(&budget));

        tenant.try_grow(40).expect("40 fits both ledgers");
        assert_eq!(tenant.process_outstanding(), 40);
        tenant.shrink(40);
        assert_eq!(tenant.process_outstanding(), 0);
        assert_eq!(budget.reserved(), 0);

        // Nothing outstanding: the shrink is a no-op on both ledgers rather
        // than a saturating release of someone else's bytes.
        let other = TenantMemoryAccountant::with_process_budget(60, Arc::clone(&budget));
        other.try_grow(25).expect("25 fits both ledgers");
        tenant.shrink(25);
        assert_eq!(
            budget.reserved(),
            25,
            "a shrink with nothing outstanding releases nothing"
        );
        assert_eq!(other.process_outstanding(), 25);
    }

    /// Shrink order: process then tenant, the reverse of the way up. A's
    /// release returns its bytes to the shared process budget and to A's own
    /// counter, and touches B's counter not at all.
    #[test]
    fn a_shrink_returns_the_bytes_to_the_process_and_only_the_shrinking_tenant() {
        let budget = Arc::new(MemoryBudget::new(1000));
        let tenant_a = TenantMemoryAccountant::with_process_budget(100, Arc::clone(&budget));
        let tenant_b = TenantMemoryAccountant::with_process_budget(100, Arc::clone(&budget));
        let pool_a = pool_over(Arc::clone(&tenant_a), CeilingBreach::new());
        let pool_b = pool_over(Arc::clone(&tenant_b), CeilingBreach::new());
        let res_a = MemoryConsumer::new("tenant-a").register(&pool_a);
        let res_b = MemoryConsumer::new("tenant-b").register(&pool_b);

        res_a.try_grow(40).expect("40 fits every ceiling");
        res_b.try_grow(40).expect("40 fits every ceiling");
        assert_eq!(budget.reserved(), 80);

        res_a.shrink(40);
        assert_eq!(budget.reserved(), 40);
        assert_eq!(tenant_a.reserved(), 0);
        assert_eq!(tenant_b.reserved(), 40);
    }

    /// The infallible path over the process budget: `grow` cannot decline, so
    /// the overshoot is counted past the process limit and the breach records
    /// it with the process figures, exactly as a tenant overshoot does.
    #[test]
    fn grow_past_only_the_process_ceiling_names_the_process() {
        let budget = Arc::new(MemoryBudget::new(1024));
        let tenant = TenantMemoryAccountant::with_process_budget(1 << 30, Arc::clone(&budget));
        let breach = CeilingBreach::new();
        let pool = pool_over(Arc::clone(&tenant), Arc::clone(&breach));
        let res = MemoryConsumer::new("process-ceiling").register(&pool);

        res.grow(4096);
        assert_eq!(
            budget.reserved(),
            4096,
            "the infallible path reserves the bytes it was asked for, over the limit"
        );
        let message = breach
            .message()
            .expect("a process ceiling overshoot must trip the breach");
        assert_eq!(
            message,
            "process memory ceiling breached: 4096 bytes reserved exceeds process limit 1024"
        );
    }

    /// The default an executor starts with: an unlimited budget refuses
    /// nothing, and still COUNTS every SQL reservation, which is what makes the
    /// gauge a later server task reads meaningful before any limit is set.
    #[test]
    fn the_unlimited_default_counts_the_reservation_and_refuses_nothing() {
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let breach = CeilingBreach::new();
        let pool = pool_over(Arc::clone(&tenant), Arc::clone(&breach));
        let res = MemoryConsumer::new("unlimited-default").register(&pool);

        assert_eq!(tenant.process_limit(), u64::MAX);
        res.try_grow(4096)
            .expect("an unlimited budget refuses nothing");
        assert_eq!(tenant.process_reserved(), 4096);
        assert_eq!(tenant.reserved(), 4096);
        assert!(breach.message().is_none());

        res.shrink(4096);
        assert_eq!(tenant.process_reserved(), 0);
    }
}
