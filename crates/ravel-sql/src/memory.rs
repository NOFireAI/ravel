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
//!
//! Two exceptions hold released bytes on all three budgets instead of
//! releasing them, both for grouped hash aggregate streams whose consumer
//! cannot spill, because DataFusion 55.2 shrinks those reservations while the
//! emitted output still holds the bytes:
//!
//! - The legacy `GroupedHashAggregateStream`, which 55.2 runs only for the
//!   shapes `AggregateExec::execute_typed` has not migrated (for example a
//!   single-stage aggregate with a limit or over ordered input), shrinks its
//!   reservation at emit. Every shrink of that consumer is held.
//! - The migrated `FinalHashAggregateStream` and `SingleHashAggregateStream`
//!   keep their reservation at the output buffers' size while they hand out
//!   slices, but resize it to zero as they return the last slice, which still
//!   points into those buffers. Only that shrink to zero is held; a shrink
//!   that leaves the reservation above zero is released.
//!
//! A consumer that can spill is never held: it is a partial aggregate that
//! answers pressure by emitting into the exchange that reserves what it
//! buffers, or a final aggregate on a runtime with a disk, and holding either
//! would charge those bytes twice. The choice is made per stream, from the
//! consumer itself. A hold is released when that consumer unregisters, which
//! DataFusion does when the stream's last reservation drops, so cancellation
//! still returns every byte. The legacy hold can be removed once Ravel runs a
//! DataFusion release whose `execute_typed` no longer falls back to the legacy
//! stream (apache/datafusion#22710, #25902), and the last-release hold once
//! the migrated streams keep their final output reserved.
//!
//! The migrated partial, final and single-stage hash aggregate streams are
//! also charged ahead of what they ask for ([`AggregateReserveAhead`], issue
//! #3181), because DataFusion 55.2 grows their reservations only after
//! `aggregate_batch` has already grown the group table and the accumulator
//! `Vec`s, and counts less than those allocations hold:
//!
//! - Every grow and shrink of such a consumer is charged at
//!   `undercount_factor` times the bytes asked, on the query, tenant and
//!   process ledgers alike.
//! - While the consumer builds (until its first shrink), it carries an
//!   envelope for the next step it can make: `envelope_size_factor x S`
//!   while its asked size `S` is under 8 MiB, else the smaller of that and
//!   its next step. Only the `P` largest envelopes in the process are
//!   reserved, on the process [`MemoryBudget`] alone, where `P` is the
//!   number of aggregate streams that can run `aggregate_batch` at once. A
//!   grow whose envelope change the budget refuses is itself refused before
//!   anything is charged.
//!
//! The next step comes from the query's [`AggregateProfile`], which the
//! executor derives from the plan ([`plan_aggregate_profile`]) and sets on
//! the pool before the plan executes. A known [`AggregateLayout`] gives each
//! stream two ladders of predicted grows: group-table steps of
//! `7/8 x 2^k x entry_size` and `Vec` steps of `2^k x W`, `W` the per-group
//! bytes of every `Vec` that grows with the group count. Each grow is
//! matched on its own size to the nearest predicted step of each ladder: the
//! largest grow within 3% of a table step is `J_t` and of a `Vec` step `J_v`
//! (a grow can count for both), and one off both is per-batch growth that
//! moves neither. Largest, not last: the groups only grow while a stream
//! builds, so a per-batch grow that lands on a low rung is not its next step.
//! The next step is then the larger of
//! `table_multiplier x J_t` (5.0 for 16-byte entries, 4.9 for 32-byte
//! entries) and `vec_multiplier x J_v x (1 + w_max / W)` (2.1), `w_max` the
//! widest `Vec`'s per-group bytes. Under [`AggregateProfile::Unknown`], with
//! no profile, or before the stream has shown a step of either ladder, it is
//! `unknown_layout_multiplier x J_max` (5.0), `J_max` its largest grow.
//!
//! A layout whose group table and `Vec`s are all reported by capacity
//! ([`AggregateLayout::capacity_accounted`]) also bounds the next step by the
//! share of `S` each can hold: every one holds between one and two times its
//! live groups, so the next table block is at most
//! `16/7 x (s + 1) x (2S / (W + 2s) + 12) + 16` and the next `Vec` transient
//! at most `(S + 12s) x 2(W + w_max) / (2W + s)`, `s` the entry size. The
//! envelope is the smallest of the size bound, the next step and that share
//! bound. When the two ladders coincide (16-byte entries and a `W` of 56:
//! `14 x 2^k` and `56 x 2^j`), every grow counts for both and the next step
//! reads `5 x J_max`; the share bound reads `1.125 x (S + 192)`. q33 is that
//! case. A `W` of 65 keeps its ladders apart: its next step is
//! `max(5 x J_t, 2.1 x J_v x 81 / 65)` and its share bound
//! `81/73 x (S + 192)`.
//!
//! [`AggregateReserveAhead::DEFAULT`] charges the factor and carries the
//! envelope; [`AggregateReserveAhead::OFF`] restores one-to-one charging and
//! no envelope.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use datafusion::arrow::datatypes::DataType;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use ravel_memory::{ExhaustionCause, MemoryBudget, MemoryExhausted};
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

/// The name prefix DataFusion 55.2 gives the legacy
/// `GroupedHashAggregateStream`'s memory consumer:
/// `"GroupedHashAggregateStream[{partition}] ({agg_fn_names})"` in
/// `datafusion-physical-plan`'s `aggregates/grouped_hash_stream.rs`. The
/// migrated aggregate streams register under other names
/// (`FinalHashAggregateStream[..]`, `SingleHashAggregateStream[..]` and so on),
/// so this prefix selects only the shapes DataFusion has not migrated.
pub const HASH_AGGREGATE_CONSUMER_PREFIX: &str = "GroupedHashAggregateStream[";

/// The name prefix DataFusion 55.2 gives the migrated final hash aggregate
/// stream's memory consumer: `"FinalHashAggregateStream[{partition}]"` in
/// `datafusion-physical-plan`'s `aggregates/hash_stream.rs`.
pub const FINAL_HASH_AGGREGATE_CONSUMER_PREFIX: &str = "FinalHashAggregateStream[";

/// The name prefix DataFusion 55.2 gives the migrated single-stage hash
/// aggregate stream's memory consumer: `"SingleHashAggregateStream[{partition}]"`
/// in `datafusion-physical-plan`'s `aggregates/single_stream.rs`.
pub const SINGLE_HASH_AGGREGATE_CONSUMER_PREFIX: &str = "SingleHashAggregateStream[";

/// Bytes currently held across every [`TenantDelegatingPool`] in the process:
/// the process-wide figure behind [`sql_memory_held_bytes`].
static HELD_BYTES: AtomicU64 = AtomicU64::new(0);

/// Bytes every live [`TenantDelegatingPool`] in this process currently holds
/// for hash aggregate streams that cannot spill. No metric renders it yet.
/// These bytes are also counted in every query, tenant and process reserved
/// figure.
pub fn sql_memory_held_bytes() -> u64 {
    HELD_BYTES.load(Ordering::Acquire)
}

/// The name prefix DataFusion 55.2 gives the migrated partial hash aggregate
/// stream's memory consumer: `"PartialHashAggregateStream[{partition}]"` in
/// `datafusion-physical-plan`'s `aggregates/hash_stream.rs`.
pub const PARTIAL_HASH_AGGREGATE_CONSUMER_PREFIX: &str = "PartialHashAggregateStream[";

/// The asked size under which a migrated aggregate's envelope is
/// `envelope_size_factor` times its size alone: below it the first steps are
/// too few to have shown the largest one.
const ENVELOPE_STEP_BOUND_FROM: usize = 8 << 20;

/// How far a grow may sit from a predicted table or `Vec` step and still be
/// classified as that step, as a fraction of the step.
const STEP_TOLERANCE: f64 = 0.03;

/// How far ahead of what they ask for the migrated hash aggregate streams are
/// charged (issue #3181). See the module documentation for the two parts.
///
/// [`Self::OFF`] (`1.0` and every envelope term `0`) charges every consumer
/// one to one and reserves no envelope.
#[derive(Debug, Clone, Copy)]
pub struct AggregateReserveAhead {
    /// Multiplier on every grow and shrink a migrated aggregate asks for,
    /// applied on the query, tenant and process ledgers.
    pub undercount_factor: f64,
    /// Multiplier on the largest group-table step of a stream whose table has
    /// 16-byte entries: its next table step, once it has asked for 8 MiB.
    pub table_multiplier_16_byte_entries: f64,
    /// The same for a stream whose table has 32-byte entries.
    pub table_multiplier_32_byte_entries: f64,
    /// Multiplier on the largest `Vec` step, before the `1 + w_max / W` term for
    /// the widest `Vec` moving last: its next `Vec` step.
    pub vec_multiplier: f64,
    /// Multiplier on a building consumer's asked size: its envelope while it
    /// is under 8 MiB, and the cap on its envelope after.
    pub envelope_size_factor: f64,
    /// Multiplier on the largest single grow of a stream whose layout is
    /// [`AggregateProfile::Unknown`], or that has no profile, or that has
    /// shown no table or `Vec` step yet.
    pub unknown_layout_multiplier: f64,
}

impl AggregateReserveAhead {
    /// One-to-one charging and no envelope.
    pub const OFF: AggregateReserveAhead = AggregateReserveAhead {
        undercount_factor: 1.0,
        table_multiplier_16_byte_entries: 0.0,
        table_multiplier_32_byte_entries: 0.0,
        vec_multiplier: 0.0,
        envelope_size_factor: 0.0,
        unknown_layout_multiplier: 0.0,
    };

    /// The default: a 1.25 undercount factor and the envelope sized from the
    /// stream's layout, capped at 2.43 times its asked size.
    pub const DEFAULT: AggregateReserveAhead = AggregateReserveAhead {
        undercount_factor: 1.25,
        table_multiplier_16_byte_entries: 5.0,
        table_multiplier_32_byte_entries: 4.9,
        vec_multiplier: 2.1,
        envelope_size_factor: 2.43,
        unknown_layout_multiplier: 5.0,
    };

    fn values(&self) -> [(&'static str, f64); 6] {
        [
            ("undercount_factor", self.undercount_factor),
            (
                "table_multiplier_16_byte_entries",
                self.table_multiplier_16_byte_entries,
            ),
            (
                "table_multiplier_32_byte_entries",
                self.table_multiplier_32_byte_entries,
            ),
            ("vec_multiplier", self.vec_multiplier),
            ("envelope_size_factor", self.envelope_size_factor),
            ("unknown_layout_multiplier", self.unknown_layout_multiplier),
        ]
    }

    /// Every value finite, the factor at least 1 and every multiplier at
    /// least 0.
    pub fn validate(&self) -> Result<(), AggregateReserveAheadError> {
        let values = self.values();
        if let Some((name, value)) = values.iter().find(|(_, value)| !value.is_finite()) {
            return Err(AggregateReserveAheadError::NotFinite {
                name,
                value: *value,
            });
        }
        if self.undercount_factor < 1.0 {
            return Err(AggregateReserveAheadError::FactorBelowOne {
                value: self.undercount_factor,
            });
        }
        if let Some((name, value)) = values[1..].iter().find(|(_, value)| *value < 0.0) {
            return Err(AggregateReserveAheadError::Negative {
                name,
                value: *value,
            });
        }
        Ok(())
    }

    /// The bytes charged for a migrated aggregate that has asked for `asked`.
    /// Exactly `asked` under a factor of 1.
    fn charge(&self, asked: usize) -> usize {
        if self.undercount_factor.to_bits() == 1.0f64.to_bits() {
            asked
        } else {
            scale(asked, self.undercount_factor)
        }
    }

    /// The envelope of a building consumer in state `state`, whose layout is
    /// `profile`.
    fn envelope(&self, state: &AggregateState, profile: Option<AggregateProfile>) -> usize {
        let by_size = scale(state.asked, self.envelope_size_factor);
        if state.asked < ENVELOPE_STEP_BOUND_FROM {
            return by_size;
        }
        let next = match profile {
            Some(AggregateProfile::Layout(layout))
                if state.table_step > 0 || state.vec_step > 0 =>
            {
                let table_multiplier = if layout.entry_size >= 32 {
                    self.table_multiplier_32_byte_entries
                } else {
                    self.table_multiplier_16_byte_entries
                };
                let next_table = scale(state.table_step, table_multiplier);
                let next_vec = scale(
                    state.vec_step,
                    self.vec_multiplier * (layout.vec_width_total + layout.vec_width_max) as f64
                        / layout.vec_width_total as f64,
                );
                next_table.max(next_vec)
            }
            _ => scale(state.largest_step, self.unknown_layout_multiplier),
        };
        let share = match profile {
            Some(AggregateProfile::Layout(layout)) => layout.share_bound(state.asked),
            _ => None,
        };
        next.min(by_size).min(share.unwrap_or(usize::MAX))
    }
}

impl Default for AggregateReserveAhead {
    fn default() -> Self {
        AggregateReserveAhead::DEFAULT
    }
}

impl PartialEq for AggregateReserveAhead {
    fn eq(&self, other: &Self) -> bool {
        self.values()
            .iter()
            .zip(other.values().iter())
            .all(|((_, a), (_, b))| a.to_bits() == b.to_bits())
    }
}

impl Eq for AggregateReserveAhead {}

/// The memory layout of a query's migrated hash aggregates, derived from its
/// physical plan by [`plan_aggregate_profile`] and set on the query's pool with
/// [`TenantDelegatingPool::set_aggregate_profile`] before it executes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateProfile {
    /// Every grouped aggregate's group values and accumulators were
    /// classified; the layout the envelope is sized from.
    Layout(AggregateLayout),
    /// Some grouped aggregate's layout is not one this module knows how the
    /// streams grow; its envelope falls back to
    /// [`AggregateReserveAhead::unknown_layout_multiplier`].
    Unknown,
}

/// What one grouped aggregate's state grows by, per group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateLayout {
    /// Bytes per group-table entry: 16 for DataFusion's primitive and
    /// multi-column group values, 32 for its single string-view key map.
    pub entry_size: usize,
    /// Bytes per group summed over every `Vec` that grows with the group
    /// count: the group values' own `Vec`s and every accumulator's.
    pub vec_width_total: usize,
    /// Bytes per group of the widest of those `Vec`s.
    pub vec_width_max: usize,
    /// Whether the group table and every `Vec` in `vec_width_total` are
    /// reported to the reservation by capacity, which bounds each one's share
    /// of the asked size (see [`AggregateLayout::share_bound`]). DataFusion's
    /// single string-view key map reports its views by length, so it is not.
    pub capacity_accounted: bool,
}

impl AggregateLayout {
    /// The bytes a table step of `2^k` buckets asks for: seven eighths of
    /// the buckets, times the entry size.
    fn table_step(&self, k: i32) -> f64 {
        0.875 * 2f64.powi(k) * self.entry_size as f64
    }

    /// The bytes a `Vec` step to `2^k` groups asks for.
    fn vec_step(&self, k: i32) -> f64 {
        2f64.powi(k) * self.vec_width_total as f64
    }

    /// An upper bound on the next block of a capacity-accounted stream that
    /// has asked for `asked` bytes, from the share of `asked` its table and
    /// its `Vec`s can each hold; `None` when the layout is not capacity
    /// accounted.
    ///
    /// With `g` groups, entry size `s`, table capacity `C_t` and table bytes
    /// `A_t`: `g <= C_t <= 2g` and `s x (C_t - 12) <= A_t <= s x C_t`, and
    /// every `Vec` has `g <= C_v <= 2g`, so its family's bytes `A_v` lie in
    /// `[W x g, 2W x g]`. With `A_t + A_v <= asked` that gives
    /// `A_t <= asked x 2s / (W + 2s)` and
    /// `A_v <= (asked + 12s) x 2W / (2W + s)`. The next table block is twice
    /// the table's `8/7 x C_t` buckets at `s + 1` bytes each plus a control
    /// tail, at most `16/7 x (s + 1) x (A_t / s + 12) + 16`. The next `Vec`
    /// transient is at most `A_v x (1 + w_max / W)`, which takes the `Vec`
    /// that moves to hold no more than `A_v / W` groups: true while one batch
    /// adds under `asked x s / ((2W + s) x (W + s))` groups.
    fn share_bound(&self, asked: usize) -> Option<usize> {
        if !self.capacity_accounted || self.entry_size == 0 || self.vec_width_total == 0 {
            return None;
        }
        let s = self.entry_size as f64;
        let w_total = self.vec_width_total as f64;
        let w_max = self.vec_width_max as f64;
        let asked = asked as f64;
        let table_entries = asked * 2.0 / (w_total + 2.0 * s) + TABLE_UNACCOUNTED_ENTRIES;
        let next_table = 16.0 / 7.0 * (s + 1.0) * table_entries + TABLE_CONTROL_TAIL_BYTES;
        let next_vec =
            (asked + TABLE_UNACCOUNTED_ENTRIES * s) * 2.0 * (w_total + w_max) / (2.0 * w_total + s);
        // A float-to-integer `as` saturates.
        Some(next_table.max(next_vec).ceil() as usize)
    }
}

/// How many entries of a group table's capacity its accounted bytes can miss:
/// `insert_accounted` (datafusion-common 55.2 `utils/proxy.rs`) bumps by 16
/// entries into a table whose capacity becomes 28, then by the capacity, so
/// the bumps sum to the capacity less 12.
const TABLE_UNACCOUNTED_ENTRIES: f64 = 12.0;

/// The control bytes a hashbrown table allocates past one per bucket: one
/// group width, at most 16.
const TABLE_CONTROL_TAIL_BYTES: f64 = 16.0;

/// Set the aggregate layout of `plan` on `pool`. A plan with no grouped
/// aggregate sets nothing, and neither does a pool that is not a
/// [`TenantDelegatingPool`].
pub(crate) fn install_aggregate_profile(plan: &Arc<dyn ExecutionPlan>, pool: &Arc<dyn MemoryPool>) {
    if let Some(pool) = pool.downcast_ref::<TenantDelegatingPool>()
        && let Some(profile) = plan_aggregate_profile(plan)
    {
        pool.set_aggregate_profile(profile);
    }
}

/// The name [`AggregateProfileRule`] registers under.
pub(crate) const AGGREGATE_PROFILE_RULE: &str = "ravel_aggregate_profile";

/// The last physical optimizer rule of every session
/// [`build_session_with_tables`](crate::session::build_session_with_tables)
/// builds. It leaves the plan as it is and sets the final plan's
/// [`AggregateProfile`] on the session's pool, so every path that turns a
/// logical plan into a physical one in such a session (a stream, a
/// `DataFrame::collect`, an explain) has set the profile before anything can
/// execute, with no call of its own to forget.
#[derive(Debug)]
pub(crate) struct AggregateProfileRule {
    pool: Arc<dyn MemoryPool>,
}

impl AggregateProfileRule {
    /// The rule for a session whose memory pool is `pool`.
    pub(crate) fn new(pool: Arc<dyn MemoryPool>) -> Self {
        AggregateProfileRule { pool }
    }
}

impl PhysicalOptimizerRule for AggregateProfileRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        install_aggregate_profile(&plan, &self.pool);
        Ok(plan)
    }

    fn name(&self) -> &str {
        AGGREGATE_PROFILE_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// The aggregate layout of `plan`: the [`AggregateLayout`] of its grouped
/// `AggregateExec` nodes, [`AggregateProfile::Unknown`] when any of them is
/// unclassified, and `None` when it has none. Of several known layouts the
/// one whose widest `Vec` is the largest share of its total wins, since its
/// next `Vec` step is the largest multiple of the last one; a tie keeps the
/// earlier node.
pub fn plan_aggregate_profile(plan: &Arc<dyn ExecutionPlan>) -> Option<AggregateProfile> {
    let mut profile = None;
    collect_aggregate_profile(plan, &mut profile);
    profile
}

fn collect_aggregate_profile(
    plan: &Arc<dyn ExecutionPlan>,
    profile: &mut Option<AggregateProfile>,
) {
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        && !aggregate.group_expr().expr().is_empty()
    {
        let next = match aggregate_layout(aggregate) {
            Some(layout) => AggregateProfile::Layout(layout),
            None => AggregateProfile::Unknown,
        };
        *profile = Some(match (*profile, next) {
            (None, next) => next,
            (Some(AggregateProfile::Unknown), _) | (_, AggregateProfile::Unknown) => {
                AggregateProfile::Unknown
            }
            (Some(AggregateProfile::Layout(held)), AggregateProfile::Layout(layout)) => {
                // w_max / W compared by cross-multiplication.
                if layout.vec_width_max * held.vec_width_total
                    > held.vec_width_max * layout.vec_width_total
                {
                    AggregateProfile::Layout(layout)
                } else {
                    AggregateProfile::Layout(held)
                }
            }
        });
    }
    for child in plan.children() {
        collect_aggregate_profile(child, profile);
    }
}

/// The layout of one grouped `AggregateExec`, following DataFusion 55.2's
/// `new_group_values` and the accumulators Ravel's session runs. `None` for
/// anything this does not classify.
fn aggregate_layout(aggregate: &AggregateExec) -> Option<AggregateLayout> {
    let group_by = aggregate.group_expr();
    if !group_by.is_single() {
        return None;
    }
    let schema = aggregate.schema();
    let keys = group_by.expr().len();
    if schema.fields().len() < keys {
        return None;
    }
    let key_types: Vec<&DataType> = schema.fields()[..keys]
        .iter()
        .map(|field| field.data_type())
        .collect();
    let (entry_size, capacity_accounted, mut widths) = match key_types.as_slice() {
        // `GroupValuesPrimitive`: a `(usize, u64)` map and one values `Vec`,
        // both sized by capacity.
        [key] if key.primitive_width().is_some() => (16, true, vec![key.primitive_width()?]),
        // `GroupValuesBytesView`: `ArrowBytesViewMap`'s 32-byte entries and
        // its 16-byte views `Vec`, whose size counts the views by length.
        [DataType::Utf8View | DataType::BinaryView] => (32, false, vec![16]),
        [_] => return None,
        // `GroupValuesColumn`: a `(u64, GroupIndexView)` map counted by
        // `insert_accounted` and one `Vec` per column, primitive values and
        // byte views alike sized by capacity.
        _ => (
            16,
            true,
            key_types
                .iter()
                .map(|key| match key {
                    DataType::Utf8View | DataType::BinaryView => Some(16),
                    key => key.primitive_width(),
                })
                .collect::<Option<Vec<usize>>>()?,
        ),
    };
    for expr in aggregate.aggr_expr() {
        widths.extend(accumulator_widths(expr)?);
    }
    let vec_width_total = widths.iter().sum::<usize>();
    let vec_width_max = widths.iter().copied().max()?;
    (vec_width_total > 0).then_some(AggregateLayout {
        entry_size,
        vec_width_total,
        vec_width_max,
        capacity_accounted,
    })
}

/// The per-group widths of the `Vec`s `expr`'s groups accumulator grows. Every
/// accumulator classified here reports its `Vec`s by capacity.
fn accumulator_widths(expr: &AggregateFunctionExpr) -> Option<Vec<usize>> {
    if expr.is_distinct() || !expr.groups_accumulator_supported() {
        return None;
    }
    let return_type = expr.field().data_type().clone();
    match expr.fun().name() {
        // `CountGroupsAccumulator`: one `i64` per group.
        "count" => Some(vec![8]),
        // Ravel's checked integer `SUM`: an `i64` or `u64` sum per group.
        // Its validity bitmap and its table of groups promoted past the
        // result type are not `Vec`s of a group width, as the built-in's
        // bitmap is not.
        "sum"
            if expr
                .state_fields()
                .ok()?
                .iter()
                .any(|field| field.name().ends_with(crate::sum::EXACT_SUM_STATE_SUFFIX)) =>
        {
            Some(crate::sum::EXACT_SUM_GROUP_WIDTHS.to_vec())
        }
        // The shared sum and count of an exact `SUM(x + c)`: an `i128` sum and
        // an `i64` count per group.
        crate::sum_plus::EXACT_SUM_COUNT => {
            Some(crate::sum_plus::EXACT_SUM_COUNT_GROUP_WIDTHS.to_vec())
        }
        // `PrimitiveGroupsAccumulator` over the return type.
        "sum" | "min" | "max" => Some(vec![return_type.primitive_width()?]),
        "avg" => {
            let state = expr.state_fields().ok()?;
            let names: Vec<&str> = state.iter().map(|field| field.name().as_str()).collect();
            match names.as_slice() {
                // Ravel's exact `AVG`: a 16-byte sum (`Option<f64>` or
                // `i128`) and an `i64` count per group.
                [sum, count] if sum.ends_with("[avg_sum]") && count.ends_with("[avg_count]") => {
                    Some(vec![16, 8])
                }
                // DataFusion's float `AVG`: a `u64` count and an `f64` sum.
                [count, sum]
                    if count.ends_with("[count]")
                        && sum.ends_with("[sum]")
                        && state[1].data_type() == &DataType::Float64 =>
                {
                    Some(vec![8, 8])
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Whether `delta` is within [`STEP_TOLERANCE`] of the `step(k)` nearest it.
fn matches_ladder(delta: usize, unit: f64, step: impl Fn(i32) -> f64) -> bool {
    if delta == 0 || unit <= 0.0 {
        return false;
    }
    let delta = delta as f64;
    let k = (delta / unit).log2().round() as i32;
    [k - 1, k, k + 1].into_iter().filter(|k| *k >= 0).any(|k| {
        let predicted = step(k);
        (delta - predicted).abs() <= STEP_TOLERANCE * predicted
    })
}

/// Why an [`AggregateReserveAhead`] was rejected.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum AggregateReserveAheadError {
    #[error("aggregate reserve-ahead {name} must be finite, got {value}")]
    NotFinite { name: &'static str, value: f64 },
    #[error("aggregate reserve-ahead undercount_factor must be at least 1.0, got {value}")]
    FactorBelowOne { value: f64 },
    #[error("aggregate reserve-ahead {name} must be at least 0, got {value}")]
    Negative { name: &'static str, value: f64 },
}

/// `bytes` times `factor`, rounded up and saturating at `usize::MAX`.
fn scale(bytes: usize, factor: f64) -> usize {
    // A float-to-integer `as` saturates, and `factor` is finite and
    // non-negative once validated.
    (bytes as f64 * factor).ceil() as usize
}

/// How many migrated aggregate streams can be inside `aggregate_batch` at
/// once in this process: the host's parallelism, or `partitions` when fewer.
/// The host's parallelism is read once per process; a budget's ledger keeps
/// the width of the first pool that reserves an envelope on it.
pub fn process_envelope_width(partitions: usize) -> NonZeroUsize {
    static PARALLELISM: OnceLock<usize> = OnceLock::new();
    let parallelism = *PARALLELISM
        .get_or_init(|| std::thread::available_parallelism().map_or(1, NonZeroUsize::get));
    NonZeroUsize::new(parallelism.min(partitions)).unwrap_or(NonZeroUsize::MIN)
}

/// Envelope bytes currently reserved across every process budget: the figure
/// behind [`sql_memory_aggregate_envelope_bytes`].
static ENVELOPE_BYTES: AtomicU64 = AtomicU64::new(0);

/// Bytes the migrated hash aggregate streams' envelopes currently reserve on
/// the process budget, summed over every budget in the process. These bytes
/// are in the process reserved figure and in no query or tenant figure.
pub fn sql_memory_aggregate_envelope_bytes() -> u64 {
    ENVELOPE_BYTES.load(Ordering::Acquire)
}

/// The live envelopes charged to one process [`MemoryBudget`], and how many
/// bytes their `width` largest hold on it.
struct EnvelopeGroup {
    budget: Arc<MemoryBudget>,
    width: usize,
    envelopes: BTreeMap<(u64, usize), usize>,
    on_budget: u64,
}

impl EnvelopeGroup {
    fn top_width_sum(&self) -> u64 {
        let mut sizes: Vec<usize> = self.envelopes.values().copied().collect();
        sizes.sort_unstable_by(|a, b| b.cmp(a));
        sizes
            .iter()
            .take(self.width)
            .fold(0u64, |sum, size| sum.saturating_add(*size as u64))
    }
}

/// Every live envelope in the process, grouped by the budget it is charged
/// to (keyed by the budget's address, which the group's `Arc` keeps unique).
/// A group's width is the one its first envelope's pool was built with.
fn envelope_ledger() -> MutexGuard<'static, BTreeMap<usize, EnvelopeGroup>> {
    static LEDGER: OnceLock<Mutex<BTreeMap<usize, EnvelopeGroup>>> = OnceLock::new();
    LEDGER
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

fn budget_key(budget: &Arc<MemoryBudget>) -> usize {
    Arc::as_ptr(budget) as usize
}

/// Whether an envelope change that raises the budget's charge may be refused.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EnvelopeCheck {
    Checked,
    Unchecked,
}

/// An envelope change the process budget refused.
struct EnvelopeRefused {
    delta: u64,
    exhausted: MemoryExhausted,
}

/// Set the envelope of consumer `key` on `budget` to `envelope` bytes (zero
/// removes it), and move the budget by the change in the group's top-`width`
/// sum. A checked change the budget refuses leaves the ledger and the budget
/// as they were.
fn set_process_envelope(
    budget: &Arc<MemoryBudget>,
    width: usize,
    key: (u64, usize),
    envelope: usize,
    check: EnvelopeCheck,
) -> Result<(), EnvelopeRefused> {
    let mut ledger = envelope_ledger();
    let budget_id = budget_key(budget);
    let group = ledger.entry(budget_id).or_insert_with(|| EnvelopeGroup {
        budget: Arc::clone(budget),
        width,
        envelopes: BTreeMap::new(),
        on_budget: 0,
    });
    let previous = if envelope == 0 {
        group.envelopes.remove(&key)
    } else {
        group.envelopes.insert(key, envelope)
    };
    let next = group.top_width_sum();
    if next > group.on_budget {
        let delta = next - group.on_budget;
        if check == EnvelopeCheck::Checked {
            if let Err(exhausted) = group.budget.try_reserve(delta) {
                match previous {
                    Some(previous) => group.envelopes.insert(key, previous),
                    None => group.envelopes.remove(&key),
                };
                if group.envelopes.is_empty() && group.on_budget == 0 {
                    ledger.remove(&budget_id);
                }
                return Err(EnvelopeRefused { delta, exhausted });
            }
        } else {
            group.budget.reserve_unchecked(delta);
        }
        ENVELOPE_BYTES.fetch_add(delta, Ordering::AcqRel);
    } else if next < group.on_budget {
        let delta = group.on_budget - next;
        group.budget.release(delta);
        ENVELOPE_BYTES.fetch_sub(delta, Ordering::AcqRel);
    }
    group.on_budget = next;
    if group.envelopes.is_empty() && group.on_budget == 0 {
        ledger.remove(&budget_id);
    }
    Ok(())
}

/// Remove every envelope pool `pool_id` still has on `budget`.
fn clear_pool_envelopes(budget: &Arc<MemoryBudget>, pool_id: u64) {
    let mut ledger = envelope_ledger();
    let budget_id = budget_key(budget);
    let Some(group) = ledger.get_mut(&budget_id) else {
        return;
    };
    group.envelopes.retain(|(pool, _), _| *pool != pool_id);
    // Removing envelopes cannot raise the top-width sum.
    let next = group.top_width_sum();
    let delta = group.on_budget.saturating_sub(next);
    if delta > 0 {
        group.budget.release(delta);
        ENVELOPE_BYTES.fetch_sub(delta, Ordering::AcqRel);
    }
    group.on_budget = next;
    if group.envelopes.is_empty() && group.on_budget == 0 {
        ledger.remove(&budget_id);
    }
}

/// Envelope bytes currently on `budget`: the figure a refusal names, and the
/// one tests read since they share the process ledger with every other test
/// in the binary.
fn envelope_on_budget(budget: &Arc<MemoryBudget>) -> u64 {
    envelope_ledger()
        .get(&budget_key(budget))
        .map_or(0, |group| group.on_budget)
}

/// What a pool tracks for one migrated aggregate consumer.
#[derive(Debug, Clone, Copy, Default)]
struct AggregateState {
    /// Bytes the consumer's reservations currently ask for.
    asked: usize,
    /// The largest single grow it asked for before its first shrink.
    largest_step: usize,
    /// Its largest grow classified as a group-table step (`J_t`).
    table_step: usize,
    /// Its largest grow classified as a `Vec` step (`J_v`).
    vec_step: usize,
    /// Its envelope on the process ledger.
    envelope: usize,
    /// Whether it has shrunk; from then on it carries no envelope.
    shrunk: bool,
}

impl AggregateState {
    /// The state after a grow of `additional` bytes. Under a known layout the
    /// grow is classified against both predicted ladders on its own size: a
    /// grow within [`STEP_TOLERANCE`] of a table step becomes `J_t`, of a
    /// `Vec` step `J_v`, of both both, unless the step it would replace is
    /// larger; one of neither is per-batch growth that moves neither.
    fn grown(
        self,
        additional: usize,
        reserve_ahead: &AggregateReserveAhead,
        profile: Option<AggregateProfile>,
    ) -> Self {
        let asked = self.asked.saturating_add(additional);
        if self.shrunk {
            return AggregateState { asked, ..self };
        }
        let mut next = AggregateState {
            asked,
            largest_step: self.largest_step.max(additional),
            ..self
        };
        if let Some(AggregateProfile::Layout(layout)) = profile {
            if matches_ladder(additional, layout.table_step(0), |k| layout.table_step(k)) {
                next.table_step = next.table_step.max(additional);
            }
            if matches_ladder(additional, layout.vec_step(0), |k| layout.vec_step(k)) {
                next.vec_step = next.vec_step.max(additional);
            }
        }
        next.envelope = reserve_ahead.envelope(&next, profile);
        next
    }
}

#[cfg(test)]
thread_local! {
    /// Every profile [`TenantDelegatingPool::set_aggregate_profile`] set on
    /// this thread, for tests whose pool the code under test builds itself.
    static PROFILES_SET_ON_THIS_THREAD: std::cell::RefCell<Vec<AggregateProfile>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// The profiles set on any pool on this thread since the last call, oldest
/// first.
#[cfg(test)]
pub(crate) fn take_profiles_set_on_this_thread() -> Vec<AggregateProfile> {
    PROFILES_SET_ON_THIS_THREAD.with(|set| std::mem::take(&mut *set.borrow_mut()))
}

/// Source of [`TenantDelegatingPool`] ids, which key a pool's envelopes on
/// the process ledger.
static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(0);

/// The floor of the spill-mode headroom `H`: a spill-mode pool
/// ([`TenantDelegatingPool::with_spill_headroom`]) keeps at least this many
/// bytes of its per-query limit `L` for consumers that cannot spill.
///
/// `H = max(SPILL_MODE_HEADROOM_MIN_BYTES, SPILL_MODE_HEADROOM_PERCENT% of L)`
/// is a hypothesis, not a measurement (issue #3899): it is to be re-derived
/// from the peak bytes non-spillable consumers were seen to hold
/// ([`TenantDelegatingPool::peak_unspillable_bytes`]).
pub(crate) const SPILL_MODE_HEADROOM_MIN_BYTES: usize = 64 << 20;

/// The share of the per-query limit `L`, in percent, the spill-mode headroom
/// `H` is at least. The same hypothesis as [`SPILL_MODE_HEADROOM_MIN_BYTES`].
pub(crate) const SPILL_MODE_HEADROOM_PERCENT: usize = 10;

/// The spill-mode headroom `H` for a per-query limit of `query_limit` bytes:
/// the larger of [`SPILL_MODE_HEADROOM_MIN_BYTES`] and
/// [`SPILL_MODE_HEADROOM_PERCENT`] percent of the limit, rounded down.
pub(crate) fn spill_mode_headroom(query_limit: usize) -> usize {
    let share = (query_limit as u128 * SPILL_MODE_HEADROOM_PERCENT as u128 / 100) as usize;
    share.max(SPILL_MODE_HEADROOM_MIN_BYTES)
}

/// Which limit refused a checked grow on a [`TenantDelegatingPool`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryRefusal {
    /// The per-query limit.
    Query,
    /// The tenant budget.
    Tenant,
    /// The process budget's accounted bytes.
    Process,
    /// The host memory gate.
    HostGate,
    /// The process aggregate envelope of a reserve-ahead grow.
    ReserveAhead,
}

impl MemoryRefusal {
    /// The name the query logs use for this refusal.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            MemoryRefusal::Query => "query_pool",
            MemoryRefusal::Tenant => "tenant_budget",
            MemoryRefusal::Process => "process_budget",
            MemoryRefusal::HostGate => "host_memory_gate",
            MemoryRefusal::ReserveAhead => "aggregate_reserve_ahead",
        }
    }

    fn of_cause(cause: &ExhaustionCause, accounted: MemoryRefusal) -> MemoryRefusal {
        match cause {
            ExhaustionCause::Accounted => accounted,
            ExhaustionCause::HostMemory { .. } => MemoryRefusal::HostGate,
        }
    }
}

/// What a pool recorded about its first memory refusal, read by the spill
/// retry's trigger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RefusalRecord {
    /// Which limit refused.
    pub(crate) kind: MemoryRefusal,
    /// For a tenant or process budget refusal, the budget's figures at the
    /// moment of refusal; `None` for every other kind.
    pub(crate) share: Option<BudgetShare>,
}

/// A shared budget's reserved bytes at a refusal, and how many of them were
/// the refused pool's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BudgetShare {
    /// The budget's reserved bytes, before the refused grow.
    pub(crate) budget_reserved: u64,
    /// The refused pool's bytes on that budget, before the refused grow: its
    /// charged bytes, plus its aggregate envelopes on the process budget.
    /// Envelopes are on the process budget only, never on a tenant budget.
    pub(crate) own: u64,
}

/// What a pool has charged on behalf of one consumer.
#[derive(Debug)]
struct ConsumerEntry {
    name: String,
    can_spill: bool,
    /// Bytes charged to the query budget for this consumer, including any
    /// the pool holds for it after a shrink.
    charged: usize,
    /// Its reserve-ahead envelope, while it builds.
    envelope: usize,
}

/// Every consumer a pool has charged for, with the totals the refusal
/// attribution and the spill-mode cap read.
#[derive(Debug, Default)]
struct ConsumerLedger {
    entries: HashMap<usize, ConsumerEntry>,
    /// Spill-capable consumers registered on the pool and not yet
    /// unregistered, whether or not they have charged anything.
    spillable_registered: usize,
    spillable_charged: usize,
    spillable_envelope: usize,
    unspillable_charged: usize,
    peak_unspillable: usize,
}

impl ConsumerLedger {
    fn entry(&mut self, consumer: &MemoryConsumer) -> &mut ConsumerEntry {
        self.entries
            .entry(consumer.id())
            .or_insert_with(|| ConsumerEntry {
                name: consumer.name().to_string(),
                can_spill: consumer.can_spill(),
                charged: 0,
                envelope: 0,
            })
    }

    fn add_charged(&mut self, consumer: &MemoryConsumer, amount: usize) {
        let entry = self.entry(consumer);
        entry.charged = entry.charged.saturating_add(amount);
        if entry.can_spill {
            self.spillable_charged = self.spillable_charged.saturating_add(amount);
        } else {
            self.unspillable_charged = self.unspillable_charged.saturating_add(amount);
            self.peak_unspillable = self.peak_unspillable.max(self.unspillable_charged);
        }
    }

    fn sub_charged(&mut self, consumer: &MemoryConsumer, amount: usize) {
        let entry = self.entry(consumer);
        let amount = amount.min(entry.charged);
        entry.charged -= amount;
        if entry.can_spill {
            self.spillable_charged = self.spillable_charged.saturating_sub(amount);
        } else {
            self.unspillable_charged = self.unspillable_charged.saturating_sub(amount);
        }
    }

    fn set_envelope(&mut self, consumer: &MemoryConsumer, envelope: usize) {
        let entry = self.entry(consumer);
        let previous = std::mem::replace(&mut entry.envelope, envelope);
        if entry.can_spill {
            self.spillable_envelope = self
                .spillable_envelope
                .saturating_sub(previous)
                .saturating_add(envelope);
        }
    }

    fn remove(&mut self, id: usize) {
        let Some(entry) = self.entries.remove(&id) else {
            return;
        };
        if entry.can_spill {
            self.spillable_charged = self.spillable_charged.saturating_sub(entry.charged);
            self.spillable_envelope = self.spillable_envelope.saturating_sub(entry.envelope);
        } else {
            self.unspillable_charged = self.unspillable_charged.saturating_sub(entry.charged);
        }
    }

    /// The reserve-ahead envelope bytes this pool's consumers carry.
    fn envelope_bytes(&self) -> usize {
        self.entries
            .values()
            .fold(0usize, |sum, entry| sum.saturating_add(entry.envelope))
    }

    /// The bytes the spill-mode cap counts once `consumer`, a spill-capable
    /// consumer, is charged `charged` more bytes and carries an envelope of
    /// `envelope` (`None`: the one it has).
    fn spillable_after(
        &mut self,
        consumer: &MemoryConsumer,
        charged: usize,
        envelope: Option<usize>,
    ) -> (usize, usize) {
        let current = self
            .spillable_charged
            .saturating_add(self.spillable_envelope);
        let previous = self.entry(consumer).envelope;
        let after = current
            .saturating_sub(previous)
            .saturating_add(envelope.unwrap_or(previous))
            .saturating_add(charged);
        (current, after)
    }

    /// `consumer`'s own charged plus enveloped bytes now, and once it is
    /// charged `charged` more bytes and carries an envelope of `envelope`
    /// (`None`: the one it has).
    fn own_after(
        &mut self,
        consumer: &MemoryConsumer,
        charged: usize,
        envelope: Option<usize>,
    ) -> (usize, usize) {
        let entry = self.entry(consumer);
        let current = entry.charged.saturating_add(entry.envelope);
        let after = entry
            .charged
            .saturating_add(charged)
            .saturating_add(envelope.unwrap_or(entry.envelope));
        (current, after)
    }

    /// The consumer names and bytes that explain a refusal of `asked` bytes
    /// asked by `consumer`. The envelope figure is this pool's own: the
    /// process figure sums every tenant's envelopes, and a tenant can trigger
    /// this message at will.
    fn attribution(&self, consumer: &MemoryConsumer, asked: usize) -> String {
        let mut holders: Vec<(&str, usize, usize)> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.charged > 0)
            .map(|(id, entry)| (entry.name.as_str(), entry.charged, *id))
            .collect();
        holders.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.2.cmp(&b.2)));
        let top = if holders.is_empty() {
            "none".to_string()
        } else {
            holders
                .iter()
                .take(3)
                .map(|(name, charged, _)| format!("{name} {charged} bytes"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "; asked by {} (can_spill={}, {asked} bytes asked); top holders: {top}; \
             non-spillable consumers hold {} bytes; this query's aggregate envelope {} bytes",
            consumer.name(),
            consumer.can_spill(),
            self.unspillable_charged,
            self.envelope_bytes(),
        )
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
    /// Bytes each hash aggregate consumer, keyed by `MemoryConsumer::id`, has
    /// shrunk by and the pool still keeps charged on all three budgets. Only
    /// shrinks [`Self::holds_shrink`] selects ever add to it.
    held: Mutex<HashMap<usize, usize>>,
    /// Consumers registered and not yet unregistered, for the debug check
    /// that nothing is held once the last one is gone.
    registered: AtomicUsize,
    /// This pool's key on the process envelope ledger.
    pool_id: u64,
    /// How far ahead the migrated hash aggregate streams are charged.
    reserve_ahead: AggregateReserveAhead,
    /// How many of the process's largest envelopes are on the budget.
    envelope_width: usize,
    /// Each migrated aggregate consumer's state, keyed by
    /// `MemoryConsumer::id`. Locked before `held` and the process ledger.
    aggregates: Mutex<HashMap<usize, AggregateState>>,
    /// The layout of the query's grouped aggregates, once the executor has
    /// derived it from the plan. `None` sizes every envelope as
    /// [`AggregateProfile::Unknown`].
    aggregate_profile: Mutex<Option<AggregateProfile>>,
    /// Bytes this query's consumers currently ask for, before any
    /// reserve-ahead factor. Grows and shrinks by the amounts the
    /// `MemoryPool` entry points receive, never by the charged amounts.
    asked_used: AtomicUsize,
    /// The high-water mark of `asked_used`.
    peak_asked_bytes: AtomicU64,
    /// What each consumer holds, for refusal attribution and the spill-mode
    /// cap. Locked last: nothing else is locked while it is held.
    consumers: Mutex<ConsumerLedger>,
    /// Set once any `try_grow` on this pool was refused for memory.
    refused: AtomicBool,
    /// This pool's first memory refusal.
    first_refusal: OnceLock<RefusalRecord>,
    /// Set once any refusal on this pool was the host memory gate's.
    host_gate_refused: AtomicBool,
    /// The spill-mode headroom `H`, when this pool is in spill mode.
    spill_headroom: Option<usize>,
    /// Serializes spill-capable consumers' checked grows in spill mode, so
    /// two of them cannot both pass the cap on the same room. Locked before
    /// `aggregates`.
    spill_gate: Mutex<()>,
}

impl fmt::Debug for TenantDelegatingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantDelegatingPool")
            .field("query_limit", &self.query_limit)
            .field("query_used", &self.query_used.load(Ordering::Relaxed))
            .field("held", &self.held_bytes())
            .field("reserve_ahead", &self.reserve_ahead)
            .field("envelope_width", &self.envelope_width)
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
    ///
    /// The pool charges every consumer one to one
    /// ([`AggregateReserveAhead::OFF`]) until
    /// [`Self::with_aggregate_reserve_ahead`] says otherwise.
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
            held: Mutex::new(HashMap::new()),
            registered: AtomicUsize::new(0),
            pool_id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
            reserve_ahead: AggregateReserveAhead::OFF,
            envelope_width: 1,
            aggregates: Mutex::new(HashMap::new()),
            aggregate_profile: Mutex::new(None),
            asked_used: AtomicUsize::new(0),
            peak_asked_bytes: AtomicU64::new(0),
            consumers: Mutex::new(ConsumerLedger::default()),
            refused: AtomicBool::new(false),
            first_refusal: OnceLock::new(),
            host_gate_refused: AtomicBool::new(false),
            spill_headroom: None,
            spill_gate: Mutex::new(()),
        }
    }

    /// Spill mode: a checked grow of a consumer that can spill is refused,
    /// with the `ResourcesExhausted` DataFusion's spilling operators answer by
    /// spilling, once that consumer's own charged bytes plus its
    /// reserve-ahead envelope would exceed its fair share: `query_limit -
    /// headroom` (saturating at zero) divided by the number of spill-capable
    /// consumers registered on the pool. A grow within its share is admitted
    /// even when all spill-capable consumers together would then exceed
    /// `query_limit - headroom`; the consumer over its share is refused at its
    /// next checked grow, and until then the total is bounded by the query
    /// limit alone. Consumers that cannot spill may still grow to the query
    /// limit. That refusal is a spill trigger, not a memory refusal, so it
    /// leaves [`Self::refused`] unset. A spill-capable aggregate whose
    /// reserve-ahead envelope the process budget refuses gets the same spill
    /// trigger instead of a memory refusal.
    pub fn with_spill_headroom(mut self, headroom: usize) -> Self {
        self.spill_headroom = Some(headroom);
        self
    }

    /// Whether any checked grow on this pool was refused for memory: by the
    /// query, tenant or process budget, the host memory gate, or the
    /// aggregate reserve-ahead envelope. An unconditional `grow` never sets
    /// it, even one that overshoots a ceiling.
    pub fn refused(&self) -> bool {
        self.refused.load(Ordering::Acquire)
    }

    /// The kind of this pool's first memory refusal, if it had one.
    pub(crate) fn first_refusal(&self) -> Option<MemoryRefusal> {
        self.first_refusal.get().map(|record| record.kind)
    }

    /// This pool's first memory refusal, with the figures the spill retry's
    /// trigger reads.
    pub(crate) fn first_refusal_record(&self) -> Option<RefusalRecord> {
        self.first_refusal.get().copied()
    }

    /// Whether any memory refusal on this pool was the host memory gate's.
    pub(crate) fn host_gate_refused(&self) -> bool {
        self.host_gate_refused.load(Ordering::Acquire)
    }

    /// The most bytes consumers that cannot spill held at once on this pool,
    /// over its life.
    pub fn peak_unspillable_bytes(&self) -> u64 {
        self.consumer_ledger().peak_unspillable as u64
    }

    fn consumer_ledger(&self) -> MutexGuard<'_, ConsumerLedger> {
        self.consumers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a refusal of kind `kind` of `asked` bytes asked by `consumer`,
    /// with the refusing budget's `share` for a tenant or process budget
    /// refusal, and return the attribution to append to its message.
    fn refuse(
        &self,
        kind: MemoryRefusal,
        consumer: &MemoryConsumer,
        asked: usize,
        share: Option<BudgetShare>,
    ) -> String {
        // Only the first refusal is kept; a later set is a no-op.
        let _ = self.first_refusal.set(RefusalRecord { kind, share });
        if kind == MemoryRefusal::HostGate {
            self.host_gate_refused.store(true, Ordering::Release);
        }
        self.refused.store(true, Ordering::Release);
        // The process envelope spans every tenant, so it goes to the operator
        // log and never into the client-visible message.
        tracing::debug!(
            consumer = consumer.name(),
            asked,
            process_aggregate_envelope_bytes = envelope_on_budget(&self.tenant.budget),
            "SQL query memory grow refused"
        );
        self.attribution(consumer, asked)
    }

    fn attribution(&self, consumer: &MemoryConsumer, asked: usize) -> String {
        self.consumer_ledger().attribution(consumer, asked)
    }

    /// The spill-mode gate for a checked grow of `consumer`: held for the
    /// whole grow when this pool is in spill mode and `consumer` can spill.
    fn spill_gate(&self, consumer: &MemoryConsumer) -> Option<MutexGuard<'_, ()>> {
        (self.spill_headroom.is_some() && consumer.can_spill()).then(|| {
            self.spill_gate
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        })
    }

    /// The spill-mode check for a checked grow of `consumer` that charges
    /// `charged` more bytes and leaves it an envelope of `envelope` (`None`:
    /// unchanged). Admits everything outside spill mode, every consumer that
    /// cannot spill, any grow that adds nothing to the consumer's own charged
    /// and enveloped bytes, and any grow that leaves them within its fair
    /// share of the spill-mode cap, whatever the other consumers hold.
    fn spill_cap_admit(
        &self,
        consumer: &MemoryConsumer,
        asked: usize,
        charged: usize,
        envelope: Option<usize>,
    ) -> DFResult<()> {
        let Some(headroom) = self.spill_headroom else {
            return Ok(());
        };
        if !consumer.can_spill() {
            return Ok(());
        }
        let cap = self.query_limit.saturating_sub(headroom);
        let mut ledger = self.consumer_ledger();
        let consumers = ledger.spillable_registered.max(1);
        let share = cap / consumers;
        let (own, own_after) = ledger.own_after(consumer, charged, envelope);
        let (_, after) = ledger.spillable_after(consumer, charged, envelope);
        if own_after <= share || own_after <= own {
            if after > cap {
                // Some other consumer is over its share; it is refused at its
                // next checked grow. The query limit bounds the total until
                // then.
                tracing::debug!(
                    consumer = consumer.name(),
                    asked,
                    own_after,
                    share,
                    spillable_after = after,
                    cap,
                    "SQL spill-mode grow admitted within its fair share over the spill-mode cap"
                );
            }
            return Ok(());
        }
        let attribution = ledger.attribution(consumer, asked);
        drop(ledger);
        let message = if after > cap {
            format!(
                "query memory pool spill threshold reached: spill-capable consumers would hold \
                 {after} bytes charged and enveloped, over the spill-mode cap {cap} (per-query \
                 limit {} less headroom {headroom}){attribution}",
                self.query_limit,
            )
        } else {
            format!(
                "query memory pool spill threshold reached: {} would hold {own_after} bytes \
                 charged and enveloped, over its fair share {share} of the spill-mode cap {cap} \
                 ({consumers} spill-capable consumers; per-query limit {} less headroom \
                 {headroom}){attribution}",
                consumer.name(),
                self.query_limit,
            )
        };
        Err(DataFusionError::ResourcesExhausted(message))
    }

    /// Charge the migrated hash aggregate streams ahead of what they ask for,
    /// keeping the `envelope_width` largest envelopes in the process on its
    /// budget. A `reserve_ahead` that fails
    /// [`AggregateReserveAhead::validate`] is replaced by
    /// [`AggregateReserveAhead::DEFAULT`], with a warning.
    pub fn with_aggregate_reserve_ahead(
        mut self,
        reserve_ahead: AggregateReserveAhead,
        envelope_width: NonZeroUsize,
    ) -> Self {
        self.reserve_ahead = match reserve_ahead.validate() {
            Ok(()) => reserve_ahead,
            Err(error) => {
                tracing::warn!(%error, "using the default aggregate reserve-ahead");
                AggregateReserveAhead::DEFAULT
            }
        };
        self.envelope_width = envelope_width.get();
        self
    }

    /// The tenant accountant this pool delegates to (for tests and the
    /// endpoint that owns the tenant budget).
    pub fn tenant(&self) -> &Arc<TenantMemoryAccountant> {
        &self.tenant
    }

    /// Size the envelopes of this query's migrated aggregates from `profile`,
    /// the layout [`plan_aggregate_profile`] derived from its plan. Set it before
    /// the plan executes: a grow already made was classified without it.
    pub fn set_aggregate_profile(&self, profile: AggregateProfile) {
        #[cfg(test)]
        PROFILES_SET_ON_THIS_THREAD.with(|set| set.borrow_mut().push(profile));
        *self
            .aggregate_profile
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(profile);
    }

    /// The profile [`Self::set_aggregate_profile`] set, if any.
    pub fn aggregate_profile(&self) -> Option<AggregateProfile> {
        *self
            .aggregate_profile
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The high-water mark, over this query, of the sum of the bytes its
    /// consumers' reservations hold live, before any reserve-ahead factor.
    /// This is a live-demand gauge, not a cumulative charge: a held shrink
    /// ([`Self::holds_shrink`]) takes bytes off it while the bytes it frees
    /// stay charged, held rather than released ([`Self::hold`]); a grow
    /// served from a hold ([`Self::grow_charged`]) adds to it while charging
    /// nothing new. So the asked peak is at most the charged peak
    /// (`peak_intermediate_bytes`), and equals it if the plan has no
    /// reserve-ahead consumer and no held release.
    pub fn peak_asked_bytes(&self) -> u64 {
        self.peak_asked_bytes.load(Ordering::Acquire)
    }

    fn asked_grow(&self, asked: usize) {
        let total = self
            .asked_used
            .fetch_add(asked, Ordering::AcqRel)
            .saturating_add(asked);
        self.peak_asked_bytes
            .fetch_max(total as u64, Ordering::AcqRel);
    }

    fn asked_shrink(&self, asked: usize) {
        let mut cur = self.asked_used.load(Ordering::Acquire);
        loop {
            match self.asked_used.compare_exchange_weak(
                cur,
                cur.saturating_sub(asked),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Bytes this pool currently holds for hash aggregate consumers. They are
    /// included in [`MemoryPool::reserved`] and in the tenant and process
    /// figures.
    pub fn held_bytes(&self) -> usize {
        self.held_map()
            .values()
            .fold(0usize, |sum, held| sum.saturating_add(*held))
    }

    /// Whether `consumer`'s shrinks are held rather than released: a legacy
    /// grouped hash aggregate stream whose consumer cannot spill.
    ///
    /// DataFusion marks the consumer `can_spill` when the stream answers memory
    /// pressure itself: a partial aggregate by emitting early into the exchange
    /// that feeds the final aggregate, which reserves the batches it buffers,
    /// and a final aggregate on a runtime with a disk by spilling its state.
    /// Holding either would charge those bytes twice and would make a peak
    /// depend on when each partial stream ends.
    fn holds(&self, consumer: &MemoryConsumer) -> bool {
        consumer.name().starts_with(HASH_AGGREGATE_CONSUMER_PREFIX) && !consumer.can_spill()
    }

    /// Whether `consumer`'s shrink to zero is held rather than released: a
    /// migrated final or single-stage hash aggregate stream whose consumer
    /// cannot spill. Those streams resize their reservation to zero as they
    /// return the last output slice, which still points into the output
    /// buffers. A consumer that can spill is not held, for the reason
    /// [`Self::holds`] gives.
    fn holds_last_release(&self, consumer: &MemoryConsumer) -> bool {
        let name = consumer.name();
        (name.starts_with(FINAL_HASH_AGGREGATE_CONSUMER_PREFIX)
            || name.starts_with(SINGLE_HASH_AGGREGATE_CONSUMER_PREFIX))
            && !consumer.can_spill()
    }

    /// Whether this shrink of `reservation` is held. `MemoryReservation`
    /// lowers its own size before it calls the pool, so a size of zero here
    /// means the shrink released the reservation's last byte.
    fn holds_shrink(&self, reservation: &MemoryReservation) -> bool {
        let consumer = reservation.consumer();
        self.holds(consumer) || (reservation.size() == 0 && self.holds_last_release(consumer))
    }

    /// Whether a grow of `reservation` may be served from its consumer's hold.
    fn may_hold(&self, consumer: &MemoryConsumer) -> bool {
        self.holds(consumer) || self.holds_last_release(consumer)
    }

    fn held_map(&self) -> MutexGuard<'_, HashMap<usize, usize>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether `consumer` is a migrated hash aggregate stream this pool
    /// charges ahead of what it asks for.
    fn reserves_ahead(&self, consumer: &MemoryConsumer) -> bool {
        let name = consumer.name();
        self.reserve_ahead != AggregateReserveAhead::OFF
            && (name.starts_with(PARTIAL_HASH_AGGREGATE_CONSUMER_PREFIX)
                || name.starts_with(FINAL_HASH_AGGREGATE_CONSUMER_PREFIX)
                || name.starts_with(SINGLE_HASH_AGGREGATE_CONSUMER_PREFIX))
    }

    fn aggregates_map(&self) -> MutexGuard<'_, HashMap<usize, AggregateState>> {
        self.aggregates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn set_envelope(
        &self,
        consumer_id: usize,
        envelope: usize,
        check: EnvelopeCheck,
    ) -> Result<(), EnvelopeRefused> {
        set_process_envelope(
            &self.tenant.budget,
            self.envelope_width,
            (self.pool_id, consumer_id),
            envelope,
            check,
        )
    }

    fn set_envelope_unchecked(&self, consumer_id: usize, envelope: usize) {
        // An unchecked change is never refused.
        self.set_envelope(consumer_id, envelope, EnvelopeCheck::Unchecked)
            .unwrap_or(());
    }

    /// The checked grow of a migrated aggregate. Its envelope moves first, so
    /// a doubling the process budget cannot hold is refused before any ledger
    /// is charged for it; a refused charge puts the envelope back.
    fn aggregate_try_grow(&self, consumer: &MemoryConsumer, additional: usize) -> DFResult<()> {
        let id = consumer.id();
        let _spill_gate = self.spill_gate(consumer);
        let mut aggregates = self.aggregates_map();
        let state = aggregates.get(&id).copied().unwrap_or_default();
        let next = state.grown(additional, &self.reserve_ahead, self.aggregate_profile());
        let charged = self
            .reserve_ahead
            .charge(next.asked)
            .saturating_sub(self.reserve_ahead.charge(state.asked));
        self.spill_cap_admit(consumer, additional, charged, Some(next.envelope))?;
        if next.envelope != state.envelope
            && let Err(refused) = self.set_envelope(id, next.envelope, EnvelopeCheck::Checked)
        {
            // In spill mode a consumer that can spill answers this by
            // spilling, so it is a spill trigger and not a memory refusal.
            if self.spill_headroom.is_some() && consumer.can_spill() {
                tracing::debug!(
                    consumer = consumer.name(),
                    asked = additional,
                    delta = refused.delta,
                    "SQL aggregate reserve-ahead refused in spill mode; the aggregate spills"
                );
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "query memory pool spill threshold reached: the process aggregate \
                     envelope needs to grow by {} bytes: {}{}",
                    refused.delta,
                    refused.exhausted,
                    self.attribution(consumer, additional)
                )));
            }
            let kind =
                MemoryRefusal::of_cause(&refused.exhausted.cause, MemoryRefusal::ReserveAhead);
            return Err(DataFusionError::ResourcesExhausted(format!(
                "aggregate reserve-ahead refused: the process aggregate envelope needs \
                 to grow by {} bytes: {}{}",
                refused.delta,
                refused.exhausted,
                self.refuse(kind, consumer, additional, None)
            )));
        }
        if let Err(error) = self.try_grow_charged(consumer, charged, additional) {
            if next.envelope != state.envelope {
                self.set_envelope_unchecked(id, state.envelope);
            }
            return Err(error);
        }
        self.asked_grow(next.asked.saturating_sub(state.asked));
        self.consumer_ledger().set_envelope(consumer, next.envelope);
        aggregates.insert(id, next);
        Ok(())
    }

    /// The unconditional grow of a migrated aggregate.
    fn aggregate_grow(&self, consumer: &MemoryConsumer, additional: usize) {
        let id = consumer.id();
        let mut aggregates = self.aggregates_map();
        let state = aggregates.get(&id).copied().unwrap_or_default();
        let next = state.grown(additional, &self.reserve_ahead, self.aggregate_profile());
        if next.envelope != state.envelope {
            self.set_envelope_unchecked(id, next.envelope);
        }
        let charged = self
            .reserve_ahead
            .charge(next.asked)
            .saturating_sub(self.reserve_ahead.charge(state.asked));
        self.grow_charged(consumer, charged);
        self.asked_grow(next.asked.saturating_sub(state.asked));
        self.consumer_ledger().set_envelope(consumer, next.envelope);
        aggregates.insert(id, next);
    }

    /// Record a migrated aggregate's shrink and return the charged bytes it
    /// frees, and the asked bytes it takes off the asked gauge. Its first
    /// shrink ends its build, and its envelope with it.
    fn aggregate_shrink(&self, consumer: &MemoryConsumer, shrink: usize) -> (usize, usize) {
        let id = consumer.id();
        let mut aggregates = self.aggregates_map();
        let state = aggregates.entry(id).or_default();
        let asked = state.asked.saturating_sub(shrink);
        let released = self
            .reserve_ahead
            .charge(state.asked)
            .saturating_sub(self.reserve_ahead.charge(asked));
        let asked_released = state.asked - asked;
        state.asked = asked;
        if !state.shrunk {
            state.shrunk = true;
            if state.envelope > 0 {
                state.envelope = 0;
                self.set_envelope_unchecked(id, 0);
                self.consumer_ledger().set_envelope(consumer, 0);
            }
        }
        (released, asked_released)
    }

    /// Charge `amount` bytes for `consumer` unconditionally, serving what its
    /// hold covers from the hold.
    fn grow_charged(&self, consumer: &MemoryConsumer, amount: usize) {
        // Bytes this consumer already holds are already charged and already
        // passed the ceiling checks, so only the excess is charged here.
        let from_held = if self.may_hold(consumer) {
            self.take_held(consumer.id(), amount)
        } else {
            0
        };
        let excess = amount - from_held;
        if excess > 0 {
            self.charge(excess);
            self.consumer_ledger().add_charged(consumer, excess);
        }
    }

    /// Charge `amount` bytes for `consumer` on all three budgets or none,
    /// serving what its hold covers from the hold. `asked` is what the
    /// consumer asked for, which a refusal names.
    fn try_grow_charged(
        &self,
        consumer: &MemoryConsumer,
        amount: usize,
        asked: usize,
    ) -> DFResult<()> {
        let from_held = if self.may_hold(consumer) {
            self.take_held(consumer.id(), amount)
        } else {
            0
        };
        let excess = amount - from_held;
        if excess == 0 {
            return Ok(());
        }
        let charged = self.try_charge(consumer, excess, asked);
        match charged {
            Ok(()) => self.consumer_ledger().add_charged(consumer, excess),
            // A refused try_grow leaves the reservation's size unchanged, so
            // the bytes taken from the hold go back to it.
            Err(_) if from_held > 0 => self.hold(consumer.id(), from_held),
            Err(_) => {}
        }
        charged
    }

    /// Add `amount` to consumer `id`'s hold. The bytes stay charged.
    fn hold(&self, id: usize, amount: usize) {
        let mut held = self.held_map();
        let entry = held.entry(id).or_insert(0);
        *entry = entry.saturating_add(amount);
        HELD_BYTES.fetch_add(amount as u64, Ordering::AcqRel);
    }

    /// Take up to `wanted` bytes out of consumer `id`'s hold, returning how
    /// many were taken. The taken bytes stay charged: the caller's grow now
    /// owns them.
    fn take_held(&self, id: usize, wanted: usize) -> usize {
        let mut held = self.held_map();
        let Some(amount) = held.get_mut(&id) else {
            return 0;
        };
        let taken = (*amount).min(wanted);
        *amount -= taken;
        if *amount == 0 {
            held.remove(&id);
        }
        HELD_BYTES.fetch_sub(taken as u64, Ordering::AcqRel);
        taken
    }

    /// Release `amount` charged bytes from the query budget and, through the
    /// accountant, the tenant and process budgets.
    fn release(&self, amount: usize) {
        self.query_shrink(amount);
        self.tenant.shrink(amount);
    }

    /// The unconditional charge behind [`MemoryPool::grow`].
    fn charge(&self, additional: usize) {
        // The trait requires this to be infallible, and both budgets grow
        // unconditionally -- this is not only reachable after a validated
        // try_grow. datafusion 55.2.0's MemoryReservation::resize() and at
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

    /// The checked charge behind [`MemoryPool::try_grow`]: all three budgets
    /// or none. A refusal's message ends with the attribution for `asked`
    /// bytes asked by `consumer`.
    fn try_charge(
        &self,
        consumer: &MemoryConsumer,
        additional: usize,
        asked: usize,
    ) -> DFResult<()> {
        // Query budget first: a query must trip its own pool before it can
        // threaten the tenant budget.
        let query_total = self.query_try_grow(additional).map_err(|used| {
            DataFusionError::ResourcesExhausted(format!(
                "query memory pool exhausted: {additional} more bytes on top of {used} \
                 already reserved exceeds per-query limit {}{}",
                self.query_limit,
                self.refuse(MemoryRefusal::Query, consumer, asked, None)
            ))
        })?;
        if let Err(refused) = self.tenant.try_grow(additional) {
            // Roll the query reservation back so a tenant- or process-budget
            // failure leaves nothing reserved on any of the three budgets. The
            // accountant has already rolled its own tenant charge back on a
            // process refusal, and the process reserved nothing to release.
            self.query_shrink(additional);
            let charged = self.query_used.load(Ordering::Acquire) as u64;
            let (kind, share) = match &refused {
                GrowRefused::Tenant { used } => (
                    MemoryRefusal::Tenant,
                    Some(BudgetShare {
                        budget_reserved: *used as u64,
                        own: charged,
                    }),
                ),
                GrowRefused::Process { exhausted, .. } => {
                    let kind = MemoryRefusal::of_cause(&exhausted.cause, MemoryRefusal::Process);
                    let share = (kind == MemoryRefusal::Process).then(|| BudgetShare {
                        budget_reserved: exhausted.reserved,
                        own: charged.saturating_add(self.consumer_ledger().envelope_bytes() as u64),
                    });
                    (kind, share)
                }
            };
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
                } => match exhausted.cause {
                    ExhaustionCause::Accounted => format!(
                        "process memory budget exhausted: {additional} more bytes on top of {} \
                         already reserved exceeds process limit {} (tenant reserved {} of \
                         tenant limit {})",
                        exhausted.reserved,
                        exhausted.limit,
                        tenant_held,
                        self.tenant.limit()
                    ),
                    // The ledger is usually far from its limit when the gate
                    // closes, so the accounted figures would misname the cause.
                    // The wording matches `MemoryExhausted`'s Display.
                    ExhaustionCause::HostMemory { available, floor } => format!(
                        "host memory gate refused the reservation: host available memory \
                         {available} bytes is below the {floor} byte floor (requested \
                         {additional}; tenant reserved {tenant_held} of tenant limit {})",
                        self.tenant.limit()
                    ),
                },
            };
            return Err(DataFusionError::ResourcesExhausted(
                message + &self.refuse(kind, consumer, asked, share),
            ));
        }
        // Observed only once BOTH budgets have granted the growth. Recording it
        // before the tenant check leaves a peak for bytes that no successful
        // reservation ever held, because the query reservation above is rolled
        // back on a tenant refusal: the peak would outlive the allocation.
        self.accounting
            .observe_intermediate_bytes(query_total as u64);
        Ok(())
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

    fn register(&self, consumer: &MemoryConsumer) {
        self.registered.fetch_add(1, Ordering::AcqRel);
        if consumer.can_spill() {
            self.consumer_ledger().spillable_registered += 1;
        }
        if self.may_hold(consumer) {
            tracing::debug!(
                consumer = consumer.name(),
                query_limit = self.query_limit,
                "SQL query memory pool holds this consumer's released bytes until it unregisters"
            );
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        // DataFusion calls this when the consumer's last reservation drops,
        // after that reservation's final shrink, so whatever the consumer
        // still holds is no longer live anywhere.
        let state = self.aggregates_map().remove(&consumer.id());
        if state.is_some_and(|state| state.envelope > 0) {
            self.set_envelope_unchecked(consumer.id(), 0);
        }
        let released = {
            let mut held = self.held_map();
            let released = held.remove(&consumer.id()).unwrap_or(0);
            HELD_BYTES.fetch_sub(released as u64, Ordering::AcqRel);
            released
        };
        if released > 0 {
            self.release(released);
        }
        {
            let mut ledger = self.consumer_ledger();
            ledger.remove(consumer.id());
            if consumer.can_spill() {
                ledger.spillable_registered = ledger.spillable_registered.saturating_sub(1);
            }
        }
        let before = self.registered.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(
            before != 1 || self.held_bytes() == 0,
            "the last consumer unregistered with {} bytes still held",
            self.held_bytes()
        );
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let consumer = reservation.consumer();
        if self.reserves_ahead(consumer) {
            self.aggregate_grow(consumer, additional);
        } else {
            self.grow_charged(consumer, additional);
            self.asked_grow(additional);
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        // Forwarded to the tenant accountant, including the shrink DataFusion
        // issues when a MemoryReservation is dropped: this is what
        // makes a cancelled or dropped stream return its tenant reservation to
        // zero. A held aggregate's bytes return at its `unregister` instead.
        let consumer = reservation.consumer();
        let (charged, asked) = if self.reserves_ahead(consumer) {
            self.aggregate_shrink(consumer, shrink)
        } else {
            (shrink, shrink)
        };
        self.asked_shrink(asked);
        if self.holds_shrink(reservation) {
            self.hold(consumer.id(), charged);
        } else {
            self.release(charged);
            self.consumer_ledger().sub_charged(consumer, charged);
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        let consumer = reservation.consumer();
        if self.reserves_ahead(consumer) {
            self.aggregate_try_grow(consumer, additional)
        } else {
            let _spill_gate = self.spill_gate(consumer);
            self.spill_cap_admit(consumer, additional, additional, None)?;
            self.try_grow_charged(consumer, additional, additional)?;
            self.asked_grow(additional);
            Ok(())
        }
    }

    fn reserved(&self) -> usize {
        self.query_used.load(Ordering::Acquire)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.query_limit)
    }
}

impl Drop for TenantDelegatingPool {
    fn drop(&mut self) {
        // One pool per statement, so this logs once per statement.
        let consumers = self
            .consumers
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        tracing::debug!(
            query_limit = self.query_limit,
            refused = self.refused.load(Ordering::Acquire),
            peak_unspillable_bytes = consumers.peak_unspillable,
            spill_headroom = ?self.spill_headroom,
            "SQL query memory pool closed"
        );
        // Every reservation keeps its pool alive, so a hold left here means a
        // consumer that never unregistered; release it rather than leak it
        // onto the tenant and process budgets, which outlive this pool. The
        // same goes for an envelope left on the process ledger.
        let aggregates = self
            .aggregates
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        if !aggregates.is_empty() {
            aggregates.clear();
            clear_pool_envelopes(&self.tenant.budget, self.pool_id);
        }
        let held = std::mem::take(self.held.get_mut().unwrap_or_else(PoisonError::into_inner));
        let total = held
            .values()
            .fold(0usize, |sum, held| sum.saturating_add(*held));
        if total > 0 {
            HELD_BYTES.fetch_sub(total as u64, Ordering::AcqRel);
            self.release(total);
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use datafusion::execution::memory_pool::UnboundedMemoryPool;
    use ravel_types::accounting::QueryAccounting;

    use super::*;

    /// The name DataFusion 55.2 gives a partition-0 legacy grouped hash
    /// aggregate.
    const AGGREGATE: &str = "GroupedHashAggregateStream[0] (count(1))";

    /// The three ledgers one query pool charges, plus the pool itself so a
    /// test can read its hold.
    struct Ledgers {
        budget: Arc<MemoryBudget>,
        tenant: Arc<TenantMemoryAccountant>,
        pool: Arc<TenantDelegatingPool>,
        breach: Arc<CeilingBreach>,
    }

    impl Ledgers {
        fn new(query_limit: usize) -> Self {
            let budget = Arc::new(MemoryBudget::new(1 << 30));
            let tenant = TenantMemoryAccountant::with_process_budget(1 << 30, Arc::clone(&budget));
            let breach = CeilingBreach::new();
            let pool = Arc::new(TenantDelegatingPool::new(
                query_limit,
                Arc::clone(&tenant),
                Arc::clone(&breach),
                QueryAccounting::new(),
            ));
            Ledgers {
                budget,
                tenant,
                pool,
                breach,
            }
        }

        fn holding() -> Self {
            Ledgers::new(1 << 30)
        }

        fn register(&self, name: &str) -> MemoryReservation {
            self.register_consumer(MemoryConsumer::new(name))
        }

        fn register_consumer(&self, consumer: MemoryConsumer) -> MemoryReservation {
            let pool: Arc<dyn MemoryPool> = Arc::clone(&self.pool) as Arc<dyn MemoryPool>;
            consumer.register(&pool)
        }

        /// The query, tenant and process figures, in that order.
        fn charged(&self) -> (usize, usize, u64) {
            (
                self.pool.reserved(),
                self.tenant.reserved(),
                self.budget.reserved(),
            )
        }
    }

    /// FLIP: forward every shrink as before (drop the `holds` branch in
    /// `shrink`) and the three figures read 200.
    #[test]
    fn an_aggregate_shrink_stays_charged_on_all_three_budgets_and_is_held() {
        let ledgers = Ledgers::holding();
        let res = ledgers.register(AGGREGATE);

        res.try_grow(1000).expect("within every ceiling");
        res.shrink(800);

        assert_eq!(ledgers.charged(), (1000, 1000, 1000));
        assert_eq!(ledgers.pool.held_bytes(), 800);
        assert!(
            sql_memory_held_bytes() >= 800,
            "the process-wide held figure includes this pool's hold"
        );
    }

    #[test]
    fn an_aggregate_regrow_is_served_from_its_hold_before_any_new_charge() {
        let ledgers = Ledgers::holding();
        let res = ledgers.register(AGGREGATE);
        res.try_grow(1000).expect("within every ceiling");
        res.shrink(800);

        res.grow(300);
        assert_eq!(ledgers.pool.held_bytes(), 500);
        assert_eq!(ledgers.charged(), (1000, 1000, 1000));

        res.try_grow(200).expect("served from the hold");
        assert_eq!(ledgers.pool.held_bytes(), 300);
        assert_eq!(ledgers.charged(), (1000, 1000, 1000));

        // 300 from the hold, the remaining 100 charged as a new grow.
        res.try_grow(400).expect("within every ceiling");
        assert_eq!(ledgers.pool.held_bytes(), 0);
        assert_eq!(ledgers.charged(), (1100, 1100, 1100));
        assert_eq!(res.size(), 1100);
    }

    #[test]
    fn dropping_an_aggregate_releases_its_hold_from_all_three_budgets() {
        let ledgers = Ledgers::holding();
        let res = ledgers.register(AGGREGATE);
        res.try_grow(1000).expect("within every ceiling");
        res.shrink(800);
        let split = res.split(100);

        // The registration is shared, so the consumer is still live while the
        // split half exists, and its hold stays.
        drop(res);
        assert_eq!(ledgers.charged(), (1000, 1000, 1000));
        assert_eq!(ledgers.pool.held_bytes(), 900);

        drop(split);
        assert_eq!(ledgers.charged(), (0, 0, 0));
        assert_eq!(ledgers.pool.held_bytes(), 0);
        assert_eq!(ledgers.tenant.process_outstanding(), 0);
    }

    /// FLIP: make `holds` ignore the consumer name and this reads 1000 with
    /// 800 held.
    #[test]
    fn a_non_aggregate_consumer_releases_at_once_under_the_hold() {
        let ledgers = Ledgers::holding();
        let res = ledgers.register("ExternalSorter[0]");

        res.try_grow(1000).expect("within every ceiling");
        res.shrink(800);

        assert_eq!(ledgers.charged(), (200, 200, 200));
        assert_eq!(ledgers.pool.held_bytes(), 0);
    }

    /// The ungrouped `AggregateStream` emits nothing large; only the grouped
    /// hash aggregate's prefix selects the hold.
    #[test]
    fn an_ungrouped_aggregate_stream_releases_at_once() {
        let ledgers = Ledgers::holding();
        let res = ledgers.register("AggregateStream[0]");

        res.try_grow(1000).expect("within every ceiling");
        res.shrink(800);

        assert_eq!(ledgers.charged(), (200, 200, 200));
    }

    /// The hold is chosen per stream, from the consumer: an aggregate whose
    /// consumer can spill (a partial aggregate, or a final one on a runtime
    /// with a disk) releases at once, one that cannot is held.
    ///
    /// FLIP: drop the `!consumer.can_spill()` conjunct from `holds` and the
    /// `can_spill` row reads 1000 charged, 800 held.
    #[test]
    fn an_aggregate_is_held_only_when_its_consumer_cannot_spill() {
        for (can_spill, charged, held) in [(true, 200, 0), (false, 1000, 800)] {
            let ledgers = Ledgers::holding();
            let res =
                ledgers.register_consumer(MemoryConsumer::new(AGGREGATE).with_can_spill(can_spill));

            res.try_grow(1000).expect("within every ceiling");
            res.shrink(800);

            let case = format!("can_spill={can_spill}");
            assert_eq!(
                ledgers.charged(),
                (charged, charged, charged as u64),
                "{case}"
            );
            assert_eq!(ledgers.pool.held_bytes(), held, "{case}");
            drop(res);
            assert_eq!(ledgers.charged(), (0, 0, 0), "{case}");
        }
    }

    /// The name DataFusion 55.2 gives a partition-0 migrated final hash
    /// aggregate.
    const FINAL_AGGREGATE: &str = "FinalHashAggregateStream[0]";

    /// A partial shrink, then the shrink to zero, of one consumer; the
    /// figures and the hold after each, then after the reservation drops.
    fn shrink_to_zero(consumer: MemoryConsumer) -> [((usize, usize, u64), usize); 3] {
        let ledgers = Ledgers::holding();
        let res = ledgers.register_consumer(consumer);
        res.try_grow(1000).expect("within every ceiling");
        res.shrink(400);
        let partial = (ledgers.charged(), ledgers.pool.held_bytes());
        res.shrink(600);
        let last = (ledgers.charged(), ledgers.pool.held_bytes());
        drop(res);
        let dropped = (ledgers.charged(), ledgers.pool.held_bytes());
        assert_eq!(ledgers.tenant.process_outstanding(), 0);
        [partial, last, dropped]
    }

    /// FLIP: drop `holds_last_release` from `holds_shrink` and the second
    /// shrink reads 0 on all three budgets with nothing held.
    #[test]
    fn a_final_aggregate_shrink_to_zero_is_held_until_it_unregisters() {
        let [partial, last, dropped] =
            shrink_to_zero(MemoryConsumer::new(FINAL_AGGREGATE).with_can_spill(false));
        assert_eq!(
            partial,
            ((600, 600, 600), 0),
            "a partial shrink is released"
        );
        assert_eq!(last, ((600, 600, 600), 600), "the shrink to zero is held");
        assert_eq!(dropped, ((0, 0, 0), 0), "unregister releases the hold");
    }

    #[test]
    fn a_single_stage_aggregate_shrink_to_zero_is_held_until_it_unregisters() {
        let [partial, last, dropped] = shrink_to_zero(
            MemoryConsumer::new("SingleHashAggregateStream[0]").with_can_spill(false),
        );
        assert_eq!(
            partial,
            ((600, 600, 600), 0),
            "a partial shrink is released"
        );
        assert_eq!(last, ((600, 600, 600), 600), "the shrink to zero is held");
        assert_eq!(dropped, ((0, 0, 0), 0), "unregister releases the hold");
    }

    /// FLIP: drop the `!consumer.can_spill()` conjunct from
    /// `holds_last_release` and the second shrink reads 600 with 600 held.
    #[test]
    fn a_spillable_final_aggregate_releases_its_last_shrink() {
        let [partial, last, dropped] =
            shrink_to_zero(MemoryConsumer::new(FINAL_AGGREGATE).with_can_spill(true));
        assert_eq!(partial, ((600, 600, 600), 0));
        assert_eq!(last, ((0, 0, 0), 0));
        assert_eq!(dropped, ((0, 0, 0), 0));
    }

    /// FLIP: make `holds_last_release` ignore the consumer name and the second
    /// shrink reads 600 with 600 held.
    #[test]
    fn a_non_aggregate_unspillable_consumer_releases_its_last_shrink() {
        let [partial, last, dropped] =
            shrink_to_zero(MemoryConsumer::new("ExternalSorter[0]").with_can_spill(false));
        assert_eq!(partial, ((600, 600, 600), 0));
        assert_eq!(last, ((0, 0, 0), 0));
        assert_eq!(dropped, ((0, 0, 0), 0));
    }

    /// A final aggregate's reservation dropped while still sized is a shrink
    /// to zero too (`MemoryReservation::free`), so it is held until the
    /// registration's other half drops.
    #[test]
    fn a_dropped_final_aggregate_reservation_is_held_until_it_unregisters() {
        let ledgers = Ledgers::holding();
        let res =
            ledgers.register_consumer(MemoryConsumer::new(FINAL_AGGREGATE).with_can_spill(false));
        res.try_grow(1000).expect("within every ceiling");
        let split = res.split(0);
        drop(res);
        assert_eq!(ledgers.charged(), (1000, 1000, 1000));
        assert_eq!(ledgers.pool.held_bytes(), 1000);
        drop(split);
        assert_eq!(ledgers.charged(), (0, 0, 0));
        assert_eq!(ledgers.pool.held_bytes(), 0);
    }

    /// A grow after the shrink to zero is served from the hold, so the
    /// output bytes are not charged twice.
    #[test]
    fn a_final_aggregate_regrow_after_its_last_shrink_is_served_from_the_hold() {
        let ledgers = Ledgers::holding();
        let res =
            ledgers.register_consumer(MemoryConsumer::new(FINAL_AGGREGATE).with_can_spill(false));
        res.try_grow(1000).expect("within every ceiling");
        res.shrink(1000);
        res.try_grow(300).expect("served from the hold");
        assert_eq!(ledgers.charged(), (1000, 1000, 1000));
        assert_eq!(ledgers.pool.held_bytes(), 700);
        drop(res);
        assert_eq!(ledgers.charged(), (0, 0, 0));
    }

    #[test]
    fn two_aggregate_holds_are_independent() {
        let ledgers = Ledgers::holding();
        let a = ledgers.register(AGGREGATE);
        let b = ledgers.register("GroupedHashAggregateStream[1] (count(1))");
        a.try_grow(1000).expect("within every ceiling");
        a.shrink(800);
        b.try_grow(500).expect("within every ceiling");
        b.shrink(100);
        assert_eq!(ledgers.pool.held_bytes(), 900);

        // A's 800 cover the first 800 of this grow; B's 100 are not A's.
        a.grow(900);
        assert_eq!(ledgers.pool.held_bytes(), 100);
        assert_eq!(ledgers.charged(), (1600, 1600, 1600));

        drop(a);
        assert_eq!(ledgers.charged(), (500, 500, 500));
        assert_eq!(ledgers.pool.held_bytes(), 100);
        drop(b);
        assert_eq!(ledgers.charged(), (0, 0, 0));
    }

    /// FLIP: release held bytes at once and the other consumer's grow reaches
    /// only 800 of the 1500 ceiling, so no breach trips and the try_grow fits.
    #[test]
    fn held_bytes_count_toward_the_query_ceiling() {
        let ledgers = Ledgers::new(1500);
        let aggregate = ledgers.register(AGGREGATE);
        aggregate.try_grow(1000).expect("within every ceiling");
        aggregate.shrink(800);

        let other = ledgers.register("ExternalSorter[0]");
        let err = other
            .try_grow(600)
            .expect_err("1000 charged plus 600 exceeds the 1500 query ceiling");
        assert!(
            err.to_string()
                .contains("600 more bytes on top of 1000 already reserved"),
            "{err}"
        );

        other.grow(600);
        assert_eq!(
            ledgers.breach.message(),
            Some("query memory ceiling breached: 1600 bytes reserved exceeds per-query limit 1500")
        );
    }

    #[test]
    fn a_refused_try_grow_returns_what_it_took_from_the_hold() {
        let ledgers = Ledgers::new(1500);
        let res = ledgers.register(AGGREGATE);
        res.try_grow(1000).expect("within every ceiling");
        res.shrink(800);

        // 800 from the hold, and the 600 excess on top of 1000 is refused.
        let err = res
            .try_grow(1400)
            .expect_err("the excess crosses the query ceiling");
        assert!(err.to_string().contains("600 more bytes"), "{err}");
        assert_eq!(ledgers.pool.held_bytes(), 800);
        assert_eq!(ledgers.charged(), (1000, 1000, 1000));
        assert_eq!(res.size(), 200);
    }

    /// A hold left behind by a consumer that never unregistered on this pool
    /// is released when the pool drops. The reservation is registered on a
    /// second pool so this one can drop while it lives.
    #[test]
    fn dropping_the_pool_releases_anything_still_held() {
        let ledgers = Ledgers::holding();
        let elsewhere = Ledgers::holding();
        let res = elsewhere.register(AGGREGATE);

        ledgers.pool.grow(&res, 1000);
        ledgers.pool.shrink(&res, 800);
        assert_eq!(ledgers.tenant.reserved(), 1000);

        let Ledgers {
            budget,
            tenant,
            pool,
            ..
        } = ledgers;
        drop(pool);
        assert_eq!(tenant.reserved(), 200);
        assert_eq!(budget.reserved(), 200);
    }

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

    /// ADR-3072 decision 3c (#2990): a refusal by the host memory gate names
    /// the gate and its figures, not the accounted process limit the ledger is
    /// nowhere near, and rolls back the tenant and query charges exactly as an
    /// accounted process refusal does.
    #[test]
    fn a_host_gate_refusal_names_the_gate_and_its_figures() {
        let budget = Arc::new(MemoryBudget::new(1 << 20));
        budget.set_host_memory_gate(false, 1_000, 4_000);
        let tenant = TenantMemoryAccountant::with_process_budget(600, Arc::clone(&budget));
        let pool = pool_over(Arc::clone(&tenant), CeilingBreach::new());
        let res = MemoryConsumer::new("tenant").register(&pool);

        let err = res
            .try_grow(500)
            .expect_err("a closed host gate refuses any nonzero reservation");
        let DataFusionError::ResourcesExhausted(message) = &err else {
            panic!("a gate refusal must stay a ResourcesExhausted; got {err:?}");
        };
        assert_eq!(
            message,
            "host memory gate refused the reservation: host available memory 1000 bytes \
             is below the 4000 byte floor (requested 500; tenant reserved 500 of tenant \
             limit 600); asked by tenant (can_spill=false, 500 bytes asked); top holders: \
             none; non-spillable consumers hold 0 bytes; this query's aggregate envelope 0 bytes"
        );
        assert!(
            !message.contains("exceeds process limit"),
            "the accounted limit is not the reason for a gate refusal: {message}"
        );
        assert_eq!(budget.gate_refusals(), 1, "the gate counted the refusal");
        assert_eq!(budget.accounted_refusals(), 0);

        assert_eq!(
            tenant.reserved(),
            0,
            "the refused tenant charge is rolled back"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "the refused query charge is rolled back"
        );
        assert_eq!(budget.reserved(), 0, "a gate refusal reserves nothing");
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
             exceeds process limit 100 (tenant reserved 50 of tenant limit 60); asked by \
             tenant-b (can_spill=false, 50 bytes asked); top holders: none; non-spillable \
             consumers hold 0 bytes; this query's aggregate envelope 0 bytes",
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

    /// A migrated partial aggregate's consumer name, partition 0.
    const PARTIAL: &str = "PartialHashAggregateStream[0]";

    const MIB: usize = 1 << 20;

    /// A tenant on `budget` and a pool over it that reserves ahead with
    /// `reserve_ahead`, keeping the `width` largest envelopes on the budget.
    fn ahead_pool(
        budget: &Arc<MemoryBudget>,
        width: usize,
        reserve_ahead: AggregateReserveAhead,
        accounting: QueryAccounting,
    ) -> (Arc<TenantMemoryAccountant>, Arc<TenantDelegatingPool>) {
        let tenant = TenantMemoryAccountant::with_process_budget(1 << 40, Arc::clone(budget));
        let pool = TenantDelegatingPool::new(
            1 << 40,
            Arc::clone(&tenant),
            CeilingBreach::new(),
            accounting,
        )
        .with_aggregate_reserve_ahead(
            reserve_ahead,
            NonZeroUsize::new(width).expect("a non-zero width"),
        );
        (tenant, Arc::new(pool))
    }

    /// Serializes the tests that reserve an envelope with the one that reads
    /// the process-wide envelope figure as 0.
    fn envelope_tests() -> MutexGuard<'static, ()> {
        static SERIAL: Mutex<()> = Mutex::new(());
        SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn register_on(pool: &Arc<TenantDelegatingPool>, name: &str) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::clone(pool) as Arc<dyn MemoryPool>;
        MemoryConsumer::new(name).register(&pool)
    }

    /// A reservation registered on a pool other than the one under test, so a
    /// test can call the pool's `MemoryPool` methods directly and drop the
    /// pool while the consumer is still building.
    fn detached(name: &str) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        MemoryConsumer::new(name).register(&pool)
    }

    /// The table and accumulator `Vec` steps of one partial aggregate
    /// building 3.1M groups of a q33-like shape (layout `(16, 52, 16)`), as
    /// DataFusion 55.2 asks for them: the table steps
    /// `7/8 x 2^k x 16` for k = 19, 20, 21 (7,340,032, 14,680,064 and
    /// 29,360,128 bytes), each followed by the `Vec` step `2^k x 52` that
    /// ends the batch (27,262,976, 54,525,952 and 109,051,904 bytes).
    const SERIES: [usize; 6] = [
        7_340_032,
        27_262_976,
        14_680_064,
        54_525_952,
        29_360_128,
        109_051_904,
    ];

    /// A q33-like layout: a 16-byte multi-column table entry, and per group
    /// 52 bytes of `Vec`s whose widest is exact `AVG`'s 16-byte sum. Not
    /// capacity accounted, so the envelope these tests read is the ladder.
    const Q33_LAYOUT: AggregateLayout = AggregateLayout {
        entry_size: 16,
        vec_width_total: 52,
        vec_width_max: 16,
        capacity_accounted: false,
    };

    /// q33's layout, as `the_aggregate_profile_of_the_six_measured_statements`
    /// in `executor.rs` derives it from the plan: per group 8 + 8 (keys), 8
    /// (`COUNT`), the checked integer `SUM`'s 8 and the exact `AVG`'s 16 + 8,
    /// so `W` is 56 and its two ladders coincide: `56 x 2^j = 14 x 2^(j + 2)`.
    const Q33_ACCOUNTED_LAYOUT: AggregateLayout = AggregateLayout {
        entry_size: 16,
        vec_width_total: 8 + 8 + 8 + crate::sum::EXACT_SUM_GROUP_WIDTHS[0] + 16 + 8,
        vec_width_max: 16,
        capacity_accounted: true,
    };

    /// A synthetic capacity-accounted layout whose two ladders stay apart:
    /// q33's shape with a 17-byte `SUM` (an `i128` and a `bool` per group),
    /// so `W` is 65.
    const W65_ACCOUNTED_LAYOUT: AggregateLayout = AggregateLayout {
        entry_size: 16,
        vec_width_total: 65,
        vec_width_max: 16,
        capacity_accounted: true,
    };

    /// Grow `res` by each of `steps`, returning the envelope on `budget`
    /// after each.
    fn envelopes_after(
        budget: &Arc<MemoryBudget>,
        pool: &TenantDelegatingPool,
        res: &MemoryReservation,
        steps: &[usize],
    ) -> Vec<u64> {
        steps
            .iter()
            .map(|step| {
                res.try_grow(*step).expect("within every ceiling");
                assert_eq!(
                    budget.reserved(),
                    pool.reserved() as u64 + envelope_on_budget(budget),
                    "the envelope is on the process budget and nowhere else"
                );
                envelope_on_budget(budget)
            })
            .collect()
    }

    /// The table step and `Vec` step the pool has recorded for `res`.
    fn chain_steps(pool: &TenantDelegatingPool, res: &MemoryReservation) -> (usize, usize) {
        let state = pool
            .aggregates_map()
            .get(&res.consumer().id())
            .copied()
            .unwrap_or_default();
        (state.table_step, state.vec_step)
    }

    /// FLIP: the starting branch's `min(2.43 x S, 5 x J_max)` reads
    /// 545,259,520 at the end; sizing the `Vec` family as one `Vec`
    /// (`1 + w_max / W` = 2) reads 458,017,997.
    #[test]
    fn the_q33_series_classify_to_two_chains_and_the_envelope_is_the_larger_next_block() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        pool.set_aggregate_profile(AggregateProfile::Layout(Q33_LAYOUT));
        let res = register_on(&pool, PARTIAL);

        let envelopes = envelopes_after(&budget, &pool, &res, &SERIES);
        // Under 8 MiB the size bound: 2.43 x 7,340,032. From then on
        // max(5.0 x J_t, 2.1 x J_v x 68 / 52), under 2.43 x S at every step.
        assert_eq!(
            envelopes,
            [
                17_836_278,
                74_868_327,
                74_868_327,
                149_736_653,
                149_736_653,
                299_473_306
            ]
        );
        assert_eq!(chain_steps(&pool, &res), (29_360_128, 109_051_904));
        let last = envelopes[5];
        assert!(
            last >= (1 << 22) * (52 + 16),
            "the next Vec step, with its widest Vec moving last: {last}"
        );
        assert!(
            last >= (1 << 23) * 17,
            "the next table step's old and new tables: {last}"
        );
        assert_eq!(
            pool.reserved(),
            (SERIES.iter().sum::<usize>() as f64 * 1.25).ceil() as usize
        );

        drop(res);
        assert_eq!(budget.reserved(), 0);
    }

    /// FLIP: the starting branch's `DEFAULT`, whose envelope terms are 0,
    /// reads 0 at the end.
    #[test]
    fn an_unknown_profile_falls_back_to_five_times_the_largest_step() {
        let _serial = envelope_tests();
        for profile in [None, Some(AggregateProfile::Unknown)] {
            let budget = Arc::new(MemoryBudget::new(1 << 40));
            let (_tenant, pool) = ahead_pool(
                &budget,
                1,
                AggregateReserveAhead::DEFAULT,
                QueryAccounting::new(),
            );
            if let Some(profile) = profile {
                pool.set_aggregate_profile(profile);
            }
            let res = register_on(&pool, PARTIAL);
            let envelopes = envelopes_after(&budget, &pool, &res, &SERIES);

            assert_eq!(envelopes[5], 545_259_520, "5 x 109,051,904 ({profile:?})");
            assert_eq!(envelopes[0], 17_836_278, "2.43 x 7,340,032 ({profile:?})");
            // From then on the smaller of 5 x J_max and 2.43 x S.
            let mut size = 0u64;
            let mut largest = 0u64;
            for (index, step) in SERIES.iter().enumerate() {
                size += *step as u64;
                largest = largest.max(*step as u64);
                if index > 0 {
                    let by_size = (size as f64 * 2.43).ceil() as u64;
                    assert_eq!(
                        envelopes[index],
                        (5 * largest).min(by_size),
                        "step {index} ({profile:?})"
                    );
                }
            }
            assert_eq!(chain_steps(&pool, &res), (0, 0), "{profile:?}");
            drop(res);
            assert_eq!(budget.reserved(), 0);
        }
    }

    /// With 32-byte entries and W = 28 the two ladders coincide.
    /// FLIP: classifying each delta as a doubling of the chain's last step
    /// takes the padding grows as steps, `(10,000,000, 10,000,000)`; the
    /// starting branch's `min(2.43 x S, 5 x J_max)` reads 144,245,112.
    #[test]
    fn a_delta_that_matches_both_ladders_takes_the_larger_multiplier() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        pool.set_aggregate_profile(AggregateProfile::Layout(AggregateLayout {
            entry_size: 32,
            vec_width_total: 28,
            vec_width_max: 16,
            capacity_accounted: false,
        }));
        let res = register_on(&pool, PARTIAL);
        // Off both ladders (28 x 2^18 = 7,340,032 and 28 x 2^19 =
        // 14,680,064): size the consumer so 2.43 x S does not bind.
        envelopes_after(&budget, &pool, &res, &[10_000_000; 3]);
        assert_eq!(chain_steps(&pool, &res), (0, 0));

        let delta = 28 << 20;
        let envelopes = envelopes_after(&budget, &pool, &res, &[delta]);
        assert_eq!(chain_steps(&pool, &res), (delta, delta));
        // max(4.9, 2.1 x (1 + 16 / 28)) x 29,360,128 = 4.9 x 29,360,128.
        assert_eq!(envelopes[0], 143_864_628);
        assert_eq!(envelopes[0], scale(delta, 4.9) as u64);
        drop(res);
    }

    /// q33 ([`Q33_ACCOUNTED_LAYOUT`], `W` 56): its table ladder `14 x 2^k`
    /// and `Vec` ladder `56 x 2^j` are one set, so every step is both chains'
    /// step and the ladder bound is `5 x J_max`. The capacity share of the
    /// asked size bounds it instead. One batch keeps the share bound's `Vec`
    /// term while it adds under `S x 16 / (128 x 72) = S / 576` groups,
    /// 14,563 at 8 MiB.
    /// FLIP: the PR head's `min(2.43 x S, ladder)` reads 587,202,560 at the
    /// end; taking the share over the charged bytes reads 361,267,416.
    #[test]
    fn the_q33_layout_with_coinciding_ladders_takes_the_capacity_share_bound() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        let layout = Q33_ACCOUNTED_LAYOUT;
        assert_eq!(layout.vec_width_total, 56);
        let (s, w) = (16, 56);
        assert_eq!(
            (8usize << 20) * s / ((2 * w + s) * (w + s)),
            14_563,
            "groups one batch may add at 8 MiB"
        );
        pool.set_aggregate_profile(AggregateProfile::Layout(layout));
        let res = register_on(&pool, PARTIAL);
        // Table, `Vec`, table, `Vec`, table, `Vec`: 14 x 2^19, 56 x 2^19,
        // 14 x 2^20, 56 x 2^20, 14 x 2^21, 56 x 2^21.
        let steps = [
            7_340_032,
            29_360_128,
            14_680_064,
            58_720_256,
            29_360_128,
            117_440_512,
        ];
        let envelopes = envelopes_after(&budget, &pool, &res, &steps);
        assert_eq!(chain_steps(&pool, &res), (117_440_512, 117_440_512));

        let asked: usize = steps.iter().sum();
        assert_eq!(asked, 256_901_120);
        let last = envelopes[5];
        // (S + 12 x 16) x 2 x (56 + 16) / (2 x 56 + 16) = 1.125 x 256,901,312.
        assert_eq!(last, 289_013_976);
        assert!(
            last <= (1.125 * (asked + 12 * 16) as f64).ceil() as u64,
            "the capacity share bound: {last}"
        );
        assert!(
            last < 5 * 117_440_512,
            "under the ladder bound 5 x J_max: {last}"
        );
        let vec_bytes: u64 = 29_360_128 + 58_720_256 + 117_440_512;
        assert!(
            last >= vec_bytes * (56 + 16) / 56,
            "the next Vec step, with its widest Vec moving last: {last}"
        );
        drop(res);
        assert_eq!(budget.reserved(), 0);
    }

    /// A `W` of 65 ([`W65_ACCOUNTED_LAYOUT`]) is no `14 x 2^m`: its `Vec` steps `65 x 2^j` sit 16%
    /// off the nearest table steps `14 x 2^k`, so no grow counts for both, and
    /// its next step is `max(5 x J_t, 2.1 x J_v x 81 / 65)`. Its share bound
    /// is `(S + 192) x 2 x 81 / 146 = 81/73 x (S + 192)`, under the 56-wide
    /// `1.125 x (S + 192)`. Neither bound always binds: a stream whose first
    /// step is the table step at 8 MiB takes the share bound, and one grown
    /// from a 224-byte table step takes the ladder bound at every step from
    /// 8 MiB. One batch keeps the share bound's `Vec` term while it adds under
    /// `S x 16 / (146 x 81) = S / 739.125` groups, 11,349 at 8 MiB.
    /// FLIP: dropping the share bound from the envelope reads 356,725,556 at
    /// the end of the first stream; dropping the ladder bound reads
    /// 11,488,259 against 11,147,674 at the second stream's first step over
    /// 8 MiB.
    #[test]
    fn a_65_wide_layout_keeps_its_ladders_apart_and_takes_the_smaller_bound() {
        let _serial = envelope_tests();
        let layout = W65_ACCOUNTED_LAYOUT;
        assert_eq!(layout.vec_width_total, 65);
        let (s, w, w_max) = (16, 65, 16);
        assert_eq!(
            (8usize << 20) * s / ((2 * w + s) * (w + s)),
            11_349,
            "groups one batch may add at 8 MiB"
        );
        let vec_factor: f64 = 2.1 * 81.0 / 65.0;
        assert_eq!(
            vec_factor.to_bits(),
            (2.1 * (w + w_max) as f64 / w as f64).to_bits()
        );

        // Starting at the 8 MiB table step: 14 x 2^19, 65 x 2^19, ...,
        // 14 x 2^21, 65 x 2^21.
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        pool.set_aggregate_profile(AggregateProfile::Layout(layout));
        let res = register_on(&pool, PARTIAL);
        let steps = [
            7_340_032,
            34_078_720,
            14_680_064,
            68_157_440,
            29_360_128,
            136_314_880,
        ];
        let envelopes = envelopes_after(&budget, &pool, &res, &steps);
        assert_eq!(chain_steps(&pool, &res), (29_360_128, 136_314_880));
        assert_eq!(
            envelopes,
            [
                17_836_278,
                45_958_007,
                62_246_845,
                137_873_593,
                170_451_270,
                321_704_767
            ]
        );
        let asked: usize = steps.iter().sum();
        assert_eq!(asked, 289_931_264);
        // 81/73 x (289,931,264 + 192), under the ladder bound
        // 2.1 x 136,314,880 x 81 / 65 = 356,725,556.
        assert_eq!(Some(envelopes[5] as usize), layout.share_bound(asked));
        assert_eq!(scale(136_314_880, vec_factor), 356_725_556);
        assert!(envelopes[5] <= (1.125 * (asked + 192) as f64) as u64);
        drop(res);
        assert_eq!(budget.reserved(), 0);

        // Grown from 224 bytes: 14 x 2^k then 65 x 2^k for k = 4 to 21.
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        pool.set_aggregate_profile(AggregateProfile::Layout(layout));
        let res = register_on(&pool, PARTIAL);
        let steps: Vec<usize> = (4..=21).flat_map(|k| [14 << k, 65 << k]).collect();
        let envelopes = envelopes_after(&budget, &pool, &res, &steps);
        assert_eq!(chain_steps(&pool, &res), (29_360_128, 136_314_880));
        let (mut asked, mut j_t, mut j_v, mut ladder_bound) = (0, 0, 0, 0);
        for (index, step) in steps.iter().enumerate() {
            asked += step;
            if index % 2 == 0 {
                j_t = *step;
            } else {
                j_v = *step;
            }
            if asked < ENVELOPE_STEP_BOUND_FROM {
                continue;
            }
            let ladder = scale(j_t, 5.0).max(scale(j_v, vec_factor));
            let share = layout.share_bound(asked).expect("capacity accounted");
            assert!(ladder < share, "step {index}: {ladder} against {share}");
            assert_eq!(envelopes[index], ladder as u64, "step {index}");
            ladder_bound += 1;
        }
        assert_eq!(ladder_bound, 11, "steps 25 to 35 are at or over 8 MiB");
        assert_eq!(asked, 331_348_752);
        assert_eq!(envelopes[34..], [178_362_778, 356_725_556]);
        assert_eq!(layout.share_bound(asked), Some(367_661_158));
        drop(res);
        assert_eq!(budget.reserved(), 0);
    }

    /// A single view key map reports its views by length, so the share of
    /// the asked size its table and `Vec`s hold is not bounded, and its
    /// envelope stays the ladder or size bound.
    /// FLIP: applying the share bound to every layout reads 101,761,141.
    #[test]
    fn a_view_key_layout_does_not_take_the_capacity_share_bound() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        let layout = AggregateLayout {
            entry_size: 32,
            vec_width_total: 24,
            vec_width_max: 16,
            capacity_accounted: false,
        };
        pool.set_aggregate_profile(AggregateProfile::Layout(layout));
        let res = register_on(&pool, PARTIAL);
        // Off both ladders, then the 28 x 2^20 table step.
        envelopes_after(&budget, &pool, &res, &[10_000_000; 3]);
        let delta = 28 << 20;
        let envelopes = envelopes_after(&budget, &pool, &res, &[delta]);
        assert_eq!(chain_steps(&pool, &res), (delta, 0));
        assert_eq!(layout.share_bound(30_000_000 + delta), None);
        // 4.9 x 29,360,128, under 2.43 x 59,360,128 = 144,245,112.
        assert_eq!(envelopes[0], 143_864_628);
        assert_eq!(envelopes[0], scale(delta, 4.9) as u64);
        drop(res);
    }

    /// The share bound is taken over the bytes the consumer asked for, not
    /// the 1.25 times that the pool charges.
    /// FLIP: taking it over the charged 45,875,200 reads 51,609,816; the PR
    /// head's `min(2.43 x S, ladder)` reads 89,181,389.
    #[test]
    fn the_share_bound_reads_asked_not_charged_bytes() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        pool.set_aggregate_profile(AggregateProfile::Layout(Q33_ACCOUNTED_LAYOUT));
        let res = register_on(&pool, PARTIAL);
        let envelopes = envelopes_after(&budget, &pool, &res, &[7_340_032, 29_360_128]);
        let asked = 7_340_032 + 29_360_128;
        assert_eq!(pool.reserved(), 45_875_200, "1.25 x {asked} charged");
        // 1.125 x (36,700,160 + 192), under 2.43 x S and 5 x 29,360,128.
        assert_eq!(envelopes[1], 41_287_896);
        assert_eq!(
            Some(envelopes[1] as usize),
            Q33_ACCOUNTED_LAYOUT.share_bound(asked)
        );
        assert_ne!(
            Q33_ACCOUNTED_LAYOUT.share_bound(pool.reserved()),
            Some(envelopes[1] as usize)
        );
        drop(res);
        assert_eq!(budget.reserved(), 0);
    }

    /// FLIP: classifying each delta as a doubling of the chain's last step
    /// does not take the 0.975 `Vec` step of the next rung, and the chains
    /// read `(7,340,032, 27,262,976)` after it; keeping the last matched step
    /// rather than the largest records 26,581,402; the starting branch's
    /// `min(2.43 x S, 5 x J_max)` reads 84,085,310 after the first two.
    #[test]
    fn a_delta_off_both_ladders_by_more_than_three_percent_is_not_a_step() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        pool.set_aggregate_profile(AggregateProfile::Layout(Q33_LAYOUT));
        let res = register_on(&pool, PARTIAL);
        let envelopes = envelopes_after(&budget, &pool, &res, &SERIES[..2]);
        assert_eq!(chain_steps(&pool, &res), (7_340_032, 27_262_976));
        assert_eq!(envelopes[1], 74_868_327);

        // 1,600,000 is 12.8% under 14 x 2^17 and 6.1% under 52 x 2^15.
        let envelopes = envelopes_after(&budget, &pool, &res, &[1_600_000]);
        assert_eq!(chain_steps(&pool, &res), (7_340_032, 27_262_976));
        assert_eq!(envelopes[0], 74_868_327);

        // 0.975 x 52 x 2^19, rounded up, is within 3% of that Vec step, and
        // smaller than the Vec step already recorded, so it does not replace
        // it.
        let near = 26_581_402;
        let envelopes = envelopes_after(&budget, &pool, &res, &[near]);
        assert_eq!(chain_steps(&pool, &res), (7_340_032, 27_262_976));
        assert_eq!(envelopes[0], 74_868_327);

        // 0.975 x 52 x 2^20, rounded up, is within 3% of the next Vec step.
        let near = 53_162_804;
        let envelopes = envelopes_after(&budget, &pool, &res, &[near]);
        assert_eq!(chain_steps(&pool, &res), (7_340_032, near));
        // 2.1 x 53,162,804 x 68 / 52, over 5.0 x 7,340,032.
        assert_eq!(envelopes[0], 145_993_239);
        drop(res);
    }

    /// A per-batch grow can land exactly on a low rung of a ladder:
    /// 1,703,936 is 52 x 2^15. The groups only grow while a stream builds,
    /// so its largest matched step is its latest real one.
    /// FLIP: keeping the last matched step records 1,703,936 as `J_v` and the
    /// envelope drops to 5.0 x 7,340,032 = 36,700,160, which leaves the next
    /// real `Vec` doubling (54,525,952) unreserved.
    #[test]
    fn a_low_rung_grow_does_not_shrink_the_envelope() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        pool.set_aggregate_profile(AggregateProfile::Layout(Q33_LAYOUT));
        let res = register_on(&pool, PARTIAL);
        let envelopes = envelopes_after(&budget, &pool, &res, &SERIES[..2]);
        assert_eq!(chain_steps(&pool, &res), (7_340_032, 27_262_976));
        assert_eq!(envelopes[1], 74_868_327);

        let low_rung = 52 << 15;
        assert_eq!(low_rung, 1_703_936);
        let envelopes = envelopes_after(&budget, &pool, &res, &[low_rung]);
        assert_eq!(chain_steps(&pool, &res), (7_340_032, 27_262_976));
        assert_eq!(envelopes[0], 74_868_327);
        drop(res);
    }

    #[test]
    fn a_consumer_under_eight_mebibytes_carries_the_size_bound() {
        let _serial = envelope_tests();
        let small = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &small,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        let res = register_on(&pool, PARTIAL);
        res.try_grow(4 * MIB).expect("within every ceiling");
        assert_eq!(envelope_on_budget(&small), 10_192_159, "2.43 x 4 MiB");

        let large = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &large,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        let res = register_on(&pool, "FinalHashAggregateStream[3]");
        res.try_grow(100_000_000).expect("within every ceiling");
        // min(5 x 100 MB, 2.43 x 100 MB), rounded up: 2.43 is not exact in
        // binary.
        assert_eq!(envelope_on_budget(&large), 243_000_001);
    }

    /// FLIP: summing every envelope (per query, or over the whole process)
    /// reads 30 MiB; charging them on the query ledger moves the pools off
    /// 1.25 x their asked bytes.
    #[test]
    fn only_the_w_largest_envelopes_in_the_process_are_on_the_budget() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (tenant_one, pool_one) = ahead_pool(
            &budget,
            2,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        let (tenant_two, pool_two) = ahead_pool(
            &budget,
            2,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        let a = register_on(&pool_one, "PartialHashAggregateStream[0]");
        let b = register_on(&pool_one, "PartialHashAggregateStream[1]");
        let c = register_on(&pool_two, "SingleHashAggregateStream[0]");
        for (res, step, count) in [(&a, MIB, 9), (&b, 2 * MIB, 5), (&c, 3 * MIB, 3)] {
            for _ in 0..count {
                res.try_grow(step).expect("within every ceiling");
            }
        }

        let pools = (pool_one.reserved() + pool_two.reserved()) as u64;
        assert_eq!(
            budget.reserved(),
            pools + 5 * (2 + 3) * MIB as u64,
            "only B's and C's envelopes, the two largest, are on the budget"
        );
        assert_eq!(envelope_on_budget(&budget), 5 * (2 + 3) * MIB as u64);
        assert!(sql_memory_aggregate_envelope_bytes() >= 25 * MIB as u64);
        assert_eq!(pool_one.reserved(), 19 * MIB * 5 / 4);
        assert_eq!(tenant_one.reserved(), 19 * MIB * 5 / 4);
        assert_eq!(pool_two.reserved(), 9 * MIB * 5 / 4);
        assert_eq!(tenant_two.reserved(), 9 * MIB * 5 / 4);

        // C's envelope leaving brings A's onto the budget.
        drop(c);
        assert_eq!(envelope_on_budget(&budget), 5 * (1 + 2) * MIB as u64);
        drop((a, b));
        assert_eq!(budget.reserved(), 0);
    }

    /// FLIP: checking the envelope after the charge records a 5 MiB peak for
    /// the query (and, without a rollback, leaves the charge in place).
    #[test]
    fn a_doubling_that_does_not_fit_the_budget_is_refused_before_it_is_made() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(6 * MIB as u64));
        let accounting = QueryAccounting::new();
        let (tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            accounting.clone(),
        );
        let res = register_on(&pool, PARTIAL);

        let err = res
            .try_grow(4 * MIB)
            .expect_err("a 10,192,159-byte envelope cannot fit a 6 MiB budget");
        let message = err.to_string();
        assert!(
            message.contains(
                "asked by PartialHashAggregateStream[0] (can_spill=false, 4194304 bytes asked)"
            ),
            "{message}"
        );
        assert!(message.contains("grow by 10192159 bytes"), "{message}");
        assert_eq!(res.size(), 0);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(tenant.reserved(), 0);
        assert_eq!(budget.reserved(), 0);
        assert_eq!(envelope_on_budget(&budget), 0);
        assert_eq!(accounting.snapshot().peak_intermediate_bytes, 0);

        // An envelope that fits, under a charge the query ceiling refuses, is
        // taken back with the charge.
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let tenant = TenantMemoryAccountant::with_process_budget(1 << 40, Arc::clone(&budget));
        let pool = Arc::new(
            TenantDelegatingPool::new(MIB, tenant, CeilingBreach::new(), QueryAccounting::new())
                .with_aggregate_reserve_ahead(AggregateReserveAhead::DEFAULT, NonZeroUsize::MIN),
        );
        let res = register_on(&pool, PARTIAL);
        res.try_grow(MIB)
            .expect_err("1.25 MiB charged over a 1 MiB query ceiling");
        assert_eq!(budget.reserved(), 0);
        assert_eq!(envelope_on_budget(&budget), 0);
    }

    /// FLIP: re-arming J_max after the first shrink leaves an envelope of
    /// 1.5 GB after the 300 MB grow.
    #[test]
    fn the_envelope_is_released_at_the_first_shrink_and_at_unregister() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        let res = register_on(&pool, "FinalHashAggregateStream[0]");
        for step in SERIES {
            res.try_grow(step).expect("within every ceiling");
        }
        assert_eq!(envelope_on_budget(&budget), 545_259_520);

        res.shrink(1);
        assert_eq!(
            envelope_on_budget(&budget),
            0,
            "released at the first shrink"
        );
        assert_eq!(budget.reserved(), pool.reserved() as u64);

        res.try_grow(300_000_000).expect("within every ceiling");
        assert_eq!(envelope_on_budget(&budget), 0, "the emit grow carries none");
        assert_eq!(budget.reserved(), pool.reserved() as u64);
        assert_eq!(tenant.reserved(), pool.reserved());

        drop(res);
        assert_eq!(budget.reserved(), 0);
        assert_eq!(tenant.reserved(), 0);

        // Unregister alone, without a shrink, releases the envelope too.
        let res = detached(PARTIAL);
        MemoryPool::try_grow(&*pool, &res, 4 * MIB).expect("within every ceiling");
        assert_eq!(envelope_on_budget(&budget), 10_192_159);
        pool.unregister(res.consumer());
        assert_eq!(envelope_on_budget(&budget), 0);
    }

    #[test]
    fn a_pool_dropped_mid_build_releases_its_envelopes() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (tenant, pool) = ahead_pool(
            &budget,
            4,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        let partial = detached("PartialHashAggregateStream[0]");
        let single = detached("SingleHashAggregateStream[1]");
        MemoryPool::try_grow(&*pool, &partial, 16 * MIB).expect("within every ceiling");
        MemoryPool::grow(&*pool, &single, 2 * MIB);
        assert_eq!(
            envelope_on_budget(&budget),
            ((16.0 * MIB as f64 * 2.43).ceil() + (2.0 * MIB as f64 * 2.43).ceil()) as u64
        );

        drop(pool);
        assert_eq!(envelope_on_budget(&budget), 0);
        assert_eq!(budget.reserved(), tenant.reserved() as u64);
    }

    /// FLIP: the starting branch's `DEFAULT`, whose envelope terms are 0,
    /// reserves no envelope at the first step.
    #[test]
    fn the_default_charges_the_factor_and_carries_the_envelope() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::default(),
            QueryAccounting::new(),
        );
        let res = register_on(&pool, "FinalHashAggregateStream[0]");
        res.try_grow(SERIES[0]).expect("within every ceiling");
        let charged = (SERIES[0] as f64 * 1.25).ceil() as usize;
        assert_eq!(pool.reserved(), charged, "query: 1.25 x the ask");
        assert_eq!(tenant.reserved(), charged, "tenant: 1.25 x the ask");
        assert_eq!(envelope_on_budget(&budget), 17_836_278, "2.43 x the ask");
        assert_eq!(budget.reserved(), charged as u64 + 17_836_278);
        drop(res);
        assert_eq!(budget.reserved(), 0);
    }

    /// Every envelope term at 0 charges the factor and reserves nothing.
    #[test]
    fn a_reserve_ahead_with_no_envelope_terms_charges_the_factor_alone() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let factor_only = AggregateReserveAhead {
            undercount_factor: 1.25,
            ..AggregateReserveAhead::OFF
        };
        let (tenant, pool) = ahead_pool(&budget, 1, factor_only, QueryAccounting::new());
        pool.set_aggregate_profile(AggregateProfile::Layout(Q33_LAYOUT));
        let res = register_on(&pool, "FinalHashAggregateStream[0]");

        let mut asked = 0usize;
        for step in SERIES {
            res.try_grow(step).expect("within every ceiling");
            asked += step;
            let charged = (asked as f64 * 1.25).ceil() as usize;
            assert_eq!(
                budget.reserved(),
                charged as u64,
                "process: 1.25 x {asked} and no envelope"
            );
            assert_eq!(pool.reserved(), charged, "query: 1.25 x {asked}");
            assert_eq!(tenant.reserved(), charged, "tenant: 1.25 x {asked}");
            assert_eq!(envelope_on_budget(&budget), 0);
            assert_eq!(sql_memory_aggregate_envelope_bytes(), 0);
        }

        drop(res);
        assert_eq!(budget.reserved(), 0);
        assert_eq!(sql_memory_aggregate_envelope_bytes(), 0);
    }

    #[test]
    fn the_opt_out_charges_every_consumer_one_to_one() {
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (tenant, pool) = ahead_pool(
            &budget,
            4,
            AggregateReserveAhead::OFF,
            QueryAccounting::new(),
        );
        let names = [
            "PartialHashAggregateStream[0]",
            "FinalHashAggregateStream[0]",
            "SingleHashAggregateStream[0]",
            AGGREGATE,
            "RepartitionExec[0]",
        ];
        let reservations: Vec<_> = names.iter().map(|name| register_on(&pool, name)).collect();
        let mut asked = 0;
        for (index, res) in reservations.iter().enumerate() {
            let step = 3 * MIB + index + 1;
            res.try_grow(step).expect("within every ceiling");
            res.grow(step);
            asked += 2 * step;
        }
        assert_eq!(pool.reserved(), asked);
        assert_eq!(tenant.reserved(), asked);
        assert_eq!(budget.reserved(), asked as u64);
        assert_eq!(envelope_on_budget(&budget), 0);

        for res in &reservations {
            res.shrink(1);
        }
        // The legacy aggregate holds its shrink; the rest release one byte.
        assert_eq!(pool.held_bytes(), 1);
        assert_eq!(budget.reserved(), (asked - names.len() + 1) as u64);
        drop(reservations);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn the_envelope_width_follows_each_callers_partition_count() {
        let parallelism = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        assert_eq!(process_envelope_width(1).get(), 1);
        assert_eq!(
            process_envelope_width(usize::MAX).get(),
            parallelism,
            "a later caller with more partitions is not held to the first caller's width"
        );
        assert_eq!(process_envelope_width(0).get(), 1);
    }

    #[test]
    fn validate_refuses_a_non_finite_value_a_factor_below_one_and_a_negative_multiplier() {
        assert_eq!(AggregateReserveAhead::DEFAULT.validate(), Ok(()));
        assert_eq!(AggregateReserveAhead::OFF.validate(), Ok(()));
        let nan = AggregateReserveAhead {
            envelope_size_factor: f64::NAN,
            ..AggregateReserveAhead::DEFAULT
        };
        assert!(matches!(
            nan.validate(),
            Err(AggregateReserveAheadError::NotFinite {
                name: "envelope_size_factor",
                ..
            })
        ));
        let below = AggregateReserveAhead {
            undercount_factor: 0.9,
            ..AggregateReserveAhead::DEFAULT
        };
        assert!(matches!(
            below.validate(),
            Err(AggregateReserveAheadError::FactorBelowOne { .. })
        ));
        let negative = AggregateReserveAhead {
            vec_multiplier: -1.0,
            ..AggregateReserveAhead::DEFAULT
        };
        assert!(matches!(
            negative.validate(),
            Err(AggregateReserveAheadError::Negative {
                name: "vec_multiplier",
                ..
            })
        ));
    }

    /// A held shrink takes its bytes off the asked gauge even though the
    /// charged bytes it frees stay held, and a later plain grow adds to the
    /// asked gauge without disturbing the earlier peak.
    ///
    /// FLIP: drop the `asked_shrink` call from `shrink` and the gauge reads
    /// 100, not 0, right after the held shrink.
    #[test]
    fn peak_asked_bytes_falls_on_a_held_shrink_while_charged_stays() {
        let accounting = QueryAccounting::new();
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let pool = Arc::new(TenantDelegatingPool::new(
            1 << 30,
            tenant,
            CeilingBreach::new(),
            accounting.clone(),
        ));
        let res = register_on(&pool, FINAL_AGGREGATE);

        res.try_grow(100).expect("within every ceiling");
        assert_eq!(pool.peak_asked_bytes(), 100);
        assert_eq!(accounting.snapshot().peak_intermediate_bytes, 100);

        // A shrink to zero on a consumer `holds_last_release` selects is held:
        // the charged bytes stay reserved, but the asked gauge still falls.
        res.shrink(100);
        assert_eq!(
            pool.held_bytes(),
            100,
            "the charged bytes are held, not released"
        );
        assert_eq!(accounting.snapshot().peak_intermediate_bytes, 100);
        assert_eq!(pool.asked_used.load(Ordering::Acquire), 0);
        assert_eq!(
            pool.peak_asked_bytes(),
            100,
            "the high-water mark does not fall"
        );

        // A plain consumer's grow charges fresh bytes (the hold belongs to the
        // other consumer) and raises both gauges, but stays under the peak.
        let plain = register_on(&pool, "ExternalSorter[0]");
        plain.try_grow(50).expect("within every ceiling");
        assert_eq!(pool.asked_used.load(Ordering::Acquire), 50);
        assert_eq!(pool.peak_asked_bytes(), 100);
        assert_eq!(accounting.snapshot().peak_intermediate_bytes, 150);
    }

    /// FLIP: summing the charged delta instead of the asked delta in
    /// `aggregate_try_grow` (`self.asked_grow(charged)`) reports 5,000,000
    /// asked bytes on the default pool instead of 4,000,000; dropping the
    /// `asked_shrink` call in `shrink` leaves the peak at 5,000,000 after the
    /// shrink and smaller grow instead of leaving it unchanged at 4,000,000;
    /// swapping the asked delta for the charged delta in `aggregate_grow`'s
    /// own `asked_grow` call (`next.asked.saturating_sub(state.asked)` to
    /// `charged`) pushes the default run's final peak to 5,750,000 (the
    /// 1.25x-charged delta of the last unconditional grow, added on top of
    /// 2,000,000 already live) instead of the exact 5,000,000 the off run
    /// also reaches.
    #[test]
    fn peak_asked_intermediate_bytes_ignores_the_reserve_ahead_factor() {
        const STEP: usize = 1_000_000;
        let _serial = envelope_tests();
        let drive = |reserve_ahead: AggregateReserveAhead| {
            let budget = Arc::new(MemoryBudget::new(1 << 40));
            let accounting = QueryAccounting::new();
            let (_tenant, pool) = ahead_pool(&budget, 1, reserve_ahead, accounting.clone());
            let res = register_on(&pool, "FinalHashAggregateStream[0]");
            for _ in 0..4 {
                res.try_grow(STEP).expect("within every ceiling");
            }
            let charged = accounting.snapshot().peak_intermediate_bytes;
            assert_eq!(pool.peak_asked_bytes(), 4 * STEP as u64);

            // A shrink then a smaller grow stays under the high-water mark.
            res.shrink(3 * STEP);
            res.try_grow(STEP).expect("within every ceiling");
            assert_eq!(pool.peak_asked_bytes(), 4 * STEP as u64);

            // An unconditional grow past the earlier peak raises it to an
            // exact new value: 2,000,000 asked live plus a 3,000,000 grow.
            res.grow(3 * STEP);
            assert_eq!(pool.peak_asked_bytes(), 5 * STEP as u64);
            (pool.peak_asked_bytes(), charged)
        };
        let (asked_default, charged_default) = drive(AggregateReserveAhead::DEFAULT);
        let (asked_off, charged_off) = drive(AggregateReserveAhead::OFF);
        assert_eq!(asked_default, asked_off);
        assert_eq!(charged_off, 4_000_000);
        assert_eq!(charged_default, 5_000_000);
        assert_eq!(charged_default * 4, charged_off * 5);

        // A refused try_grow adds nothing: the 5,000,000 charged bytes fill a
        // 5,000,000 byte query ceiling, so 1,000 more asked (1,250 charged)
        // is refused.
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let tenant = TenantMemoryAccountant::with_process_budget(1 << 40, Arc::clone(&budget));
        let pool = Arc::new(
            TenantDelegatingPool::new(
                5_000_000,
                tenant,
                CeilingBreach::new(),
                QueryAccounting::new(),
            )
            .with_aggregate_reserve_ahead(AggregateReserveAhead::DEFAULT, NonZeroUsize::MIN),
        );
        let res = register_on(&pool, "FinalHashAggregateStream[0]");
        res.try_grow(4 * STEP).expect("exactly at the ceiling");
        assert_eq!(pool.peak_asked_bytes(), 4_000_000);
        res.try_grow(1_000).expect_err("over the query ceiling");
        assert_eq!(pool.peak_asked_bytes(), 4_000_000);
        assert_eq!(pool.reserved(), 5_000_000);
    }

    /// A refused `try_grow` never calls `asked_grow`: the early `?` return in
    /// `try_grow_charged`'s caller skips it, same as the aggregate case above.
    #[test]
    fn peak_asked_bytes_ignores_a_refused_plain_try_grow() {
        let accounting = QueryAccounting::new();
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let pool = Arc::new(TenantDelegatingPool::new(
            100,
            tenant,
            CeilingBreach::new(),
            accounting,
        ));
        let res = register_on(&pool, "ExternalSorter[0]");
        res.try_grow(100).expect("exactly at the query ceiling");
        assert_eq!(pool.peak_asked_bytes(), 100);
        assert_eq!(pool.asked_used.load(Ordering::Acquire), 100);

        res.try_grow(1).expect_err("over the query ceiling");
        assert_eq!(pool.peak_asked_bytes(), 100, "unchanged by the refusal");
        assert_eq!(
            pool.asked_used.load(Ordering::Acquire),
            100,
            "unchanged by the refusal"
        );
    }

    #[test]
    fn peak_asked_bytes_counts_a_plain_consumer_one_to_one() {
        const N: usize = 1_000_003;
        let accounting = QueryAccounting::new();
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let pool = Arc::new(
            TenantDelegatingPool::new(1 << 30, tenant, CeilingBreach::new(), accounting.clone())
                .with_aggregate_reserve_ahead(AggregateReserveAhead::DEFAULT, NonZeroUsize::MIN),
        );
        let res = register_on(&pool, "ExternalSorter[0]");
        res.grow(N - 3);
        res.try_grow(3).expect("within every ceiling");
        assert_eq!(pool.peak_asked_bytes(), N as u64);
        assert_eq!(pool.asked_used.load(Ordering::Acquire), N);
        assert_eq!(accounting.snapshot().peak_intermediate_bytes, N as u64);

        // A shrink below the peak, then a smaller regrow: the gauge tracks
        // both exactly and the peak stays at the high-water mark.
        res.shrink(3);
        assert_eq!(pool.asked_used.load(Ordering::Acquire), N - 3);
        assert_eq!(pool.peak_asked_bytes(), N as u64);

        res.try_grow(2).expect("within every ceiling");
        assert_eq!(pool.asked_used.load(Ordering::Acquire), N - 1);
        assert_eq!(pool.peak_asked_bytes(), N as u64);

        res.shrink(N - 1);
        assert_eq!(pool.asked_used.load(Ordering::Acquire), 0);
        assert_eq!(pool.peak_asked_bytes(), N as u64);
    }

    /// Charges a migrated aggregate one to one and gives it an envelope of
    /// exactly what it asked for while it is under 8 MiB.
    const ENVELOPE_AS_ASKED: AggregateReserveAhead = AggregateReserveAhead {
        undercount_factor: 1.0,
        envelope_size_factor: 1.0,
        ..AggregateReserveAhead::OFF
    };

    /// A pool of `query_limit` bytes over a tenant of `tenant_limit` bytes on
    /// `budget` that reserves ahead with [`ENVELOPE_AS_ASKED`], in spill mode
    /// with `headroom` when that is `Some`.
    fn attributed_pool(
        budget: &Arc<MemoryBudget>,
        query_limit: usize,
        tenant_limit: usize,
        headroom: Option<usize>,
    ) -> Arc<TenantDelegatingPool> {
        let tenant = TenantMemoryAccountant::with_process_budget(tenant_limit, Arc::clone(budget));
        let pool = TenantDelegatingPool::new(
            query_limit,
            tenant,
            CeilingBreach::new(),
            QueryAccounting::new(),
        )
        .with_aggregate_reserve_ahead(ENVELOPE_AS_ASKED, NonZeroUsize::MIN);
        Arc::new(match headroom {
            Some(headroom) => pool.with_spill_headroom(headroom),
            None => pool,
        })
    }

    fn register_spilling(pool: &Arc<TenantDelegatingPool>, name: &str) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::clone(pool) as Arc<dyn MemoryPool>;
        MemoryConsumer::new(name)
            .with_can_spill(true)
            .register(&pool)
    }

    fn exhausted_message(err: DataFusionError) -> String {
        match err {
            DataFusionError::ResourcesExhausted(message) => message,
            other => panic!("a memory refusal must be a ResourcesExhausted; got {other:?}"),
        }
    }

    /// Issue #3899: a query-pool refusal names the asker and whether it can
    /// spill, the top three holders by charged bytes, what the consumers that
    /// cannot spill hold in total, and this query's own aggregate envelope,
    /// after the message's existing leading text.
    #[test]
    fn a_query_pool_refusal_names_the_asker_and_the_top_three_holders() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 30));
        let pool = attributed_pool(&budget, 1000, 1 << 30, None);
        let aggregate = register_spilling(&pool, PARTIAL);
        aggregate.try_grow(100).expect("within every ceiling");
        let join = register_on(&pool, "HashJoinInput[0]");
        join.try_grow(300).expect("within every ceiling");
        let merge = register_on(&pool, "SortPreservingMergeExec[0]");
        merge.try_grow(200).expect("within every ceiling");
        let repartition = register_on(&pool, "RepartitionExec[0]");
        repartition.try_grow(50).expect("within every ceiling");
        assert_eq!(pool.reserved(), 650);
        assert_eq!(envelope_on_budget(&budget), 100);

        let sorter = register_spilling(&pool, "ExternalSorter[0]");
        let message = exhausted_message(
            sorter
                .try_grow(400)
                .expect_err("650 + 400 exceeds the 1000-byte query limit"),
        );
        assert!(
            message.starts_with(
                "query memory pool exhausted: 400 more bytes on top of 650 already reserved \
                 exceeds per-query limit 1000; "
            ),
            "{message}"
        );
        for part in [
            "asked by ExternalSorter[0] (can_spill=true, 400 bytes asked)",
            "top holders: HashJoinInput[0] 300 bytes, SortPreservingMergeExec[0] 200 bytes, \
             PartialHashAggregateStream[0] 100 bytes;",
            "non-spillable consumers hold 550 bytes",
            "this query's aggregate envelope 100 bytes",
        ] {
            assert!(message.contains(part), "missing {part:?} in {message}");
        }
        assert!(
            !message.contains("RepartitionExec"),
            "only the top three holders: {message}"
        );
        assert_eq!(
            message,
            "query memory pool exhausted: 400 more bytes on top of 650 already reserved \
             exceeds per-query limit 1000; asked by ExternalSorter[0] (can_spill=true, 400 \
             bytes asked); top holders: HashJoinInput[0] 300 bytes, SortPreservingMergeExec[0] \
             200 bytes, PartialHashAggregateStream[0] 100 bytes; non-spillable consumers hold \
             550 bytes; this query's aggregate envelope 100 bytes"
        );
        assert_eq!(pool.peak_unspillable_bytes(), 550);
    }

    /// Issue #3899: the tenant, process and reserve-ahead refusals carry the
    /// same attribution after their existing leading text.
    #[test]
    fn the_tenant_process_and_reserve_ahead_refusals_carry_the_attribution() {
        let _serial = envelope_tests();
        let tenant_budget = Arc::new(MemoryBudget::new(1 << 30));
        let pool = attributed_pool(&tenant_budget, 1000, 50, None);
        let res = register_on(&pool, "HashJoinInput[0]");
        res.try_grow(20).expect("within every ceiling");
        let message = exhausted_message(res.try_grow(40).expect_err("60 exceeds the tenant's 50"));
        assert_eq!(
            message,
            "tenant memory budget exhausted: 40 more bytes on top of 20 already reserved \
             exceeds tenant limit 50; asked by HashJoinInput[0] (can_spill=false, 40 bytes \
             asked); top holders: HashJoinInput[0] 20 bytes; non-spillable consumers hold 20 \
             bytes; this query's aggregate envelope 0 bytes"
        );

        let process_budget = Arc::new(MemoryBudget::new(40));
        let pool = attributed_pool(&process_budget, 1000, 1000, None);
        let res = register_spilling(&pool, "ExternalSorter[0]");
        let message = exhausted_message(res.try_grow(60).expect_err("60 exceeds the process's 40"));
        assert_eq!(
            message,
            "process memory budget exhausted: 60 more bytes on top of 0 already reserved \
             exceeds process limit 40 (tenant reserved 60 of tenant limit 1000); asked by \
             ExternalSorter[0] (can_spill=true, 60 bytes asked); top holders: none; \
             non-spillable consumers hold 0 bytes; this query's aggregate envelope 0 bytes"
        );

        let envelope_budget = Arc::new(MemoryBudget::new(1000));
        let pool = attributed_pool(&envelope_budget, 1000, 1000, None);
        let join = register_on(&pool, "HashJoinInput[0]");
        join.try_grow(950).expect("within every ceiling");
        let aggregate = register_spilling(&pool, PARTIAL);
        let message = exhausted_message(
            aggregate
                .try_grow(60)
                .expect_err("a 60-byte envelope does not fit the process's last 50"),
        );
        assert_eq!(
            message.matches(PARTIAL).count(),
            1,
            "the asker is named once: {message}"
        );
        assert_eq!(
            message.matches("60 bytes asked").count(),
            1,
            "the asked byte count is stated once: {message}"
        );
        assert!(
            message.starts_with(
                "aggregate reserve-ahead refused: the process aggregate envelope needs to grow \
                 by 60 bytes: "
            ),
            "{message}"
        );
        assert!(
            message.ends_with(
                "; asked by PartialHashAggregateStream[0] (can_spill=true, 60 bytes asked); \
                 top holders: HashJoinInput[0] 950 bytes; non-spillable consumers hold 950 \
                 bytes; this query's aggregate envelope 0 bytes"
            ),
            "{message}"
        );
    }

    /// Issue #3899: a refusal reports this query's own aggregate envelope,
    /// never the process figure, which sums the envelopes of every tenant on
    /// the shared process budget. Pool B, another tenant's query, holds a
    /// 777-byte envelope; pool A holds none and is refused on its per-query
    /// limit, and its message must not carry B's bytes.
    #[test]
    fn a_refusal_reports_only_this_querys_envelope() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 30));
        let pool_b = attributed_pool(&budget, 1 << 20, 1 << 30, None);
        let aggregate_b = register_spilling(&pool_b, PARTIAL);
        aggregate_b.try_grow(777).expect("within every ceiling");
        assert_eq!(envelope_on_budget(&budget), 777);

        let pool_a = attributed_pool(&budget, 100, 1 << 30, None);
        let join_a = register_on(&pool_a, "HashJoinInput[0]");
        let message = exhausted_message(
            join_a
                .try_grow(101)
                .expect_err("101 exceeds A's 100-byte query limit"),
        );
        assert!(!message.contains("777"), "B's envelope leaked: {message}");
        assert_eq!(
            message,
            "query memory pool exhausted: 101 more bytes on top of 0 already reserved \
             exceeds per-query limit 100; asked by HashJoinInput[0] (can_spill=false, 101 \
             bytes asked); top holders: none; non-spillable consumers hold 0 bytes; this \
             query's aggregate envelope 0 bytes"
        );
    }

    /// Issue #3899: `refused` is set by each of the four kinds of memory
    /// refusal and by nothing else: not a granted grow, not an unconditional
    /// grow past a ceiling, and not the spill-mode cap's spill trigger.
    #[test]
    fn refused_is_set_by_every_memory_refusal_and_nothing_else() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 30));

        let query = attributed_pool(&budget, 100, 1 << 30, None);
        let res = register_on(&query, "HashJoinInput[0]");
        res.try_grow(100).expect("within every ceiling");
        assert!(!query.refused(), "a granted grow refuses nothing");
        res.grow(1000);
        assert!(
            !query.refused(),
            "an unconditional grow past the query limit is not a refusal"
        );
        res.free();
        res.try_grow(101).expect_err("over the query limit");

        let tenant = attributed_pool(&budget, 1000, 50, None);
        let res = register_on(&tenant, "HashJoinInput[0]");
        assert!(!tenant.refused());
        res.try_grow(60).expect_err("over the tenant limit");

        let small = Arc::new(MemoryBudget::new(40));
        let process = attributed_pool(&small, 1000, 1000, None);
        let res = register_on(&process, "HashJoinInput[0]");
        assert!(!process.refused());
        res.try_grow(60).expect_err("over the process limit");

        let envelope_budget = Arc::new(MemoryBudget::new(1000));
        let ahead = attributed_pool(&envelope_budget, 1000, 1000, None);
        let join = register_on(&ahead, "HashJoinInput[0]");
        join.try_grow(950).expect("within every ceiling");
        let aggregate = register_spilling(&ahead, PARTIAL);
        assert!(!ahead.refused());
        let message = exhausted_message(
            aggregate
                .try_grow(60)
                .expect_err("the envelope does not fit"),
        );
        assert!(
            message.starts_with("aggregate reserve-ahead refused"),
            "{message}"
        );

        assert_eq!(
            [
                ("query", query.refused()),
                ("tenant", tenant.refused()),
                ("process", process.refused()),
                ("reserve-ahead", ahead.refused()),
            ],
            [
                ("query", true),
                ("tenant", true),
                ("process", true),
                ("reserve-ahead", true),
            ],
            "every kind of memory refusal sets refused"
        );

        let spill = attributed_pool(&budget, 1000, 1 << 30, Some(100));
        let sorter = register_spilling(&spill, "ExternalSorter[0]");
        sorter.try_grow(901).expect_err("over the spill-mode cap");
        assert!(
            !spill.refused(),
            "the spill trigger is not a memory refusal"
        );
    }

    /// Issue #3899 (M2): a tenant or process budget refusal records the
    /// budget's reserved bytes before the refused grow and the refused pool's
    /// own bytes on it: its charged bytes on the tenant budget, its charged
    /// bytes plus its aggregate envelopes on the process budget.
    ///
    /// FLIP: drop the envelope from the process `own` and it reads 300.
    #[test]
    fn a_budget_refusal_records_the_budgets_reserved_bytes_and_the_pools_own() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 30));
        let tenant = TenantMemoryAccountant::with_process_budget(1000, Arc::clone(&budget));
        let query_over_tenant = || {
            Arc::new(TenantDelegatingPool::new(
                1 << 30,
                Arc::clone(&tenant),
                CeilingBreach::new(),
                QueryAccounting::new(),
            ))
        };
        let (own, other) = (query_over_tenant(), query_over_tenant());
        let mine = register_on(&own, "ExternalSorter[0]");
        let theirs = register_on(&other, "ExternalSorter[0]");
        mine.try_grow(300).expect("300 fits the tenant");
        theirs.try_grow(600).expect("900 fits the tenant");
        mine.try_grow(200)
            .expect_err("1100 is over the tenant's 1000");
        assert_eq!(
            own.first_refusal_record(),
            Some(RefusalRecord {
                kind: MemoryRefusal::Tenant,
                share: Some(BudgetShare {
                    budget_reserved: 900,
                    own: 300,
                }),
            })
        );
        drop((mine, theirs));

        let budget = Arc::new(MemoryBudget::new(1000));
        let own = attributed_pool(&budget, 1 << 30, 1 << 30, None);
        let other = attributed_pool(&budget, 1 << 30, 1 << 30, None);
        let aggregate = register_on(&own, PARTIAL);
        aggregate
            .try_grow(300)
            .expect("300 plus a 300-byte envelope fit");
        assert_eq!(envelope_on_budget(&budget), 300);
        let theirs = register_on(&other, "ExternalSorter[0]");
        theirs.try_grow(200).expect("800 fits the process budget");
        let join = register_on(&own, "HashJoinInput[0]");
        join.try_grow(300)
            .expect_err("1100 is over the process budget's 1000");
        assert_eq!(
            own.first_refusal_record(),
            Some(RefusalRecord {
                kind: MemoryRefusal::Process,
                share: Some(BudgetShare {
                    budget_reserved: 800,
                    own: 600,
                }),
            })
        );
    }

    /// Issue #3899: in spill mode with `L = 1000` and `H = 100` a consumer
    /// that can spill is refused once the spill-capable consumers would hold
    /// 901 bytes, counting charged bytes and envelopes, while a consumer that
    /// cannot spill grows to 1000. With two spill-capable consumers the one
    /// over its fair share is refused and the one within it is admitted. The
    /// pool without spill mode lets both reach 1000.
    #[test]
    fn spill_mode_refuses_a_spill_capable_consumer_over_l_less_h() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 30));

        let spill = attributed_pool(&budget, 1000, 1 << 30, Some(100));
        let sorter = register_spilling(&spill, "ExternalSorter[0]");
        sorter.try_grow(900).expect("900 is the cap");
        let message = exhausted_message(sorter.try_grow(1).expect_err("901 is over the cap"));
        assert_eq!(
            message,
            "query memory pool spill threshold reached: spill-capable consumers would hold 901 \
             bytes charged and enveloped, over the spill-mode cap 900 (per-query limit 1000 \
             less headroom 100); asked by ExternalSorter[0] (can_spill=true, 1 bytes asked); \
             top holders: ExternalSorter[0] 900 bytes; non-spillable consumers hold 0 bytes; \
             this query's aggregate envelope 0 bytes"
        );
        assert_eq!(spill.reserved(), 900, "the refused grow reserved nothing");
        sorter.free();
        let join = register_on(&spill, "HashJoinInput[0]");
        join.try_grow(1000)
            .expect("a consumer that cannot spill grows to L");
        assert_eq!(spill.reserved(), 1000);
        join.free();
        drop(sorter);

        // 450 charged plus a 450-byte envelope fills the cap. A sorter that
        // registers next is within its fair share of 450 and is admitted
        // over the cap; the aggregate, over its share, is refused.
        let aggregate = register_spilling(&spill, PARTIAL);
        aggregate.try_grow(450).expect("450 + 450 is the cap");
        assert_eq!(envelope_on_budget(&budget), 450);
        let sorter = register_spilling(&spill, "ExternalSorter[0]");
        sorter
            .try_grow(1)
            .expect("1 is within the sorter's share of 450, though 901 is over the cap");
        aggregate
            .try_grow(1)
            .expect_err("451 + 451 is over the aggregate's share of 450");
        assert_eq!(spill.reserved(), 451);
        join.try_grow(549)
            .expect("a consumer that cannot spill grows to L");
        assert_eq!(spill.reserved(), 1000);
        drop((aggregate, sorter, join));

        let default = attributed_pool(&budget, 1000, 1 << 30, None);
        let sorter = register_spilling(&default, "ExternalSorter[0]");
        sorter.try_grow(1000).expect("no spill-mode cap");
        assert_eq!(default.reserved(), 1000);
        sorter.free();
        let join = register_on(&default, "HashJoinInput[0]");
        join.try_grow(1000).expect("within the query limit");
        assert_eq!(default.reserved(), 1000);
        join.free();
        let aggregate = register_spilling(&default, PARTIAL);
        aggregate.try_grow(450).expect("within the query limit");
        sorter
            .try_grow(550)
            .expect("no spill-mode cap counts the envelope");
        assert_eq!(default.reserved(), 1000);
        assert!(!default.refused());
    }

    /// Issue #3899 (M2): with `L = 1000` and `H = 100`, consumer A grows to
    /// 700 alone, then consumer B registers and asks 50. B is admitted,
    /// because 50 is within its share of `900 / 2 = 450`, though the total
    /// 750 is not what decides it. A's next grow of 1 is refused, because
    /// 701 is over its share, and names the share; a grow of 0 adds nothing
    /// and is admitted.
    #[test]
    fn spill_mode_admits_a_consumer_within_its_fair_share_and_refuses_the_holder() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 30));
        let spill = attributed_pool(&budget, 1000, 1 << 30, Some(100));
        let a = register_spilling(&spill, "ExternalSorter[0]");
        a.try_grow(700)
            .expect("700 is within A's share of 900 alone");
        let b = register_spilling(&spill, "ExternalSorter[1]");
        b.try_grow(50).expect("50 is within B's share of 450");
        assert_eq!(spill.reserved(), 750);
        let message = exhausted_message(a.try_grow(1).expect_err("701 is over A's share of 450"));
        assert_eq!(
            message,
            "query memory pool spill threshold reached: ExternalSorter[0] would hold 701 bytes \
             charged and enveloped, over its fair share 450 of the spill-mode cap 900 (2 \
             spill-capable consumers; per-query limit 1000 less headroom 100); asked by \
             ExternalSorter[0] (can_spill=true, 1 bytes asked); top holders: ExternalSorter[0] \
             700 bytes, ExternalSorter[1] 50 bytes; non-spillable consumers hold 0 bytes; this \
             query's aggregate envelope 0 bytes"
        );
        assert_eq!(spill.reserved(), 750, "the refused grow reserved nothing");
        assert!(
            !spill.refused(),
            "the spill trigger is not a memory refusal"
        );
        a.try_grow(0)
            .expect("a grow of 0 adds nothing to A's 700, over its share or not");
    }

    /// Issue #3899 (M2), the row 14 shape: A holds 880 of the 900-byte cap
    /// alone, then B registers and asks 50. The total 930 is over the cap,
    /// but 50 is within B's share of 450, so B is admitted and the total
    /// passes `L - H`, bounded by `L`; A, over its share, is refused at its
    /// next grow, and A's shrink brings the total back.
    #[test]
    fn spill_mode_admits_an_asker_within_its_share_over_the_cap() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 30));
        let spill = attributed_pool(&budget, 1000, 1 << 30, Some(100));
        let a = register_spilling(&spill, "ExternalSorter[0]");
        a.try_grow(880).expect("880 is within the cap");
        let b = register_spilling(&spill, "ExternalSorter[1]");
        b.try_grow(50)
            .expect("50 is within B's share of 450, though 930 is over the cap");
        assert_eq!(spill.reserved(), 930);
        let message = exhausted_message(a.try_grow(1).expect_err("881 is over A's share"));
        assert!(
            message.starts_with(
                "query memory pool spill threshold reached: spill-capable consumers would hold \
                 931 bytes charged and enveloped, over the spill-mode cap 900"
            ),
            "{message}"
        );
        b.try_grow(71)
            .expect_err("B at 121 is within its share, but L = 1000 is the outer bound");
        a.shrink(480);
        assert_eq!(spill.reserved(), 450);
        a.try_grow(1).expect("401 is within A's share of 450");
    }

    /// Issue #3899: a spill-capable aggregate's own envelope counts inside
    /// `L - H`: its grow is refused when charged bytes plus its envelope
    /// would pass the cap, though the charged bytes alone would not, and the
    /// room comes back when its first shrink drops the envelope.
    #[test]
    fn a_spill_capable_envelope_counts_inside_l_less_h() {
        let _serial = envelope_tests();
        let budget = Arc::new(MemoryBudget::new(1 << 30));
        let spill = attributed_pool(&budget, 1000, 1 << 30, Some(100));
        let aggregate = register_spilling(&spill, PARTIAL);
        aggregate.try_grow(450).expect("450 + 450 is the cap");
        let message = exhausted_message(
            aggregate
                .try_grow(1)
                .expect_err("451 + 451 is over the cap"),
        );
        assert!(
            message.starts_with(
                "query memory pool spill threshold reached: spill-capable consumers would hold \
                 902 bytes charged and enveloped, over the spill-mode cap 900"
            ),
            "{message}"
        );
        assert_eq!(spill.reserved(), 450, "the refused grow charged nothing");
        assert_eq!(
            envelope_on_budget(&budget),
            450,
            "the refused grow left the envelope"
        );

        aggregate.shrink(1);
        assert_eq!(envelope_on_budget(&budget), 0, "the first shrink drops it");
        aggregate
            .try_grow(451)
            .expect("449 + 451 charged and no envelope is the cap");
        assert_eq!(spill.reserved(), 900);
    }

    /// Issue #3899: in a spill-mode pool a reserve-ahead refusal of an
    /// aggregate that can spill is the spill trigger: a `ResourcesExhausted`
    /// with the spill-threshold text DataFusion's aggregate answers by
    /// spilling, which sets neither `refused` nor the first refusal. The
    /// same refusal of an aggregate that cannot spill, and of any aggregate
    /// in a pool out of spill mode, is the memory refusal it always was.
    #[test]
    fn a_spill_mode_reserve_ahead_refusal_of_a_spillable_aggregate_is_a_spill_trigger() {
        let _serial = envelope_tests();
        // The process budget leaves 50 bytes once the join holds 950, so a
        // 60-byte envelope does not fit; the query limit and a zero headroom
        // keep the spill-mode cap itself out of the way.
        let setup = |headroom: Option<usize>, spillable: bool| {
            let budget = Arc::new(MemoryBudget::new(1000));
            let pool = attributed_pool(&budget, 1000, 1000, headroom);
            let join = register_on(&pool, "HashJoinInput[0]");
            join.try_grow(950).expect("within every ceiling");
            let aggregate = if spillable {
                register_spilling(&pool, PARTIAL)
            } else {
                register_on(&pool, PARTIAL)
            };
            let message = exhausted_message(
                aggregate
                    .try_grow(60)
                    .expect_err("a 60-byte envelope does not fit the process's last 50"),
            );
            (pool, message, join, aggregate)
        };

        let (pool, message, _join, _aggregate) = setup(Some(0), true);
        assert!(
            message.starts_with(
                "query memory pool spill threshold reached: the process aggregate envelope \
                 needs to grow by 60 bytes: "
            ),
            "{message}"
        );
        assert!(!pool.refused(), "the spill trigger is not a memory refusal");
        assert_eq!(pool.first_refusal(), None);
        assert_eq!(pool.reserved(), 950, "the refused grow charged nothing");

        let (pool, message, _join, _aggregate) = setup(Some(0), false);
        assert!(
            message.starts_with(
                "aggregate reserve-ahead refused: the process aggregate envelope needs to grow \
                 by 60 bytes: "
            ),
            "{message}"
        );
        assert!(pool.refused());
        assert_eq!(pool.first_refusal(), Some(MemoryRefusal::ReserveAhead));

        let (pool, message, _join, _aggregate) = setup(None, true);
        assert!(
            message.starts_with(
                "aggregate reserve-ahead refused: the process aggregate envelope needs to grow \
                 by 60 bytes: "
            ),
            "{message}"
        );
        assert!(pool.refused());
        assert_eq!(pool.first_refusal(), Some(MemoryRefusal::ReserveAhead));
    }

    /// Issue #3899: the pool records which layer refused first, and whether
    /// any refusal came from the host memory gate, which the spill retry does
    /// not answer.
    #[test]
    fn the_pool_records_its_first_refusal_and_a_host_gate_refusal() {
        let budget = Arc::new(MemoryBudget::new(1 << 20));
        let query = attributed_pool(&budget, 100, 1 << 20, None);
        let res = register_on(&query, "HashJoinInput[0]");
        res.try_grow(101).expect_err("over the query limit");
        let tenant = attributed_pool(&budget, 1000, 50, None);
        let res = register_on(&tenant, "HashJoinInput[0]");
        res.try_grow(60).expect_err("over the tenant limit");
        let small = Arc::new(MemoryBudget::new(40));
        let process = attributed_pool(&small, 1000, 1000, None);
        let res = register_on(&process, "HashJoinInput[0]");
        res.try_grow(60).expect_err("over the process limit");
        let gated = Arc::new(MemoryBudget::new(1 << 20));
        gated.set_host_memory_gate(false, 1_000, 4_000);
        let gate = attributed_pool(&gated, 1000, 1000, None);
        let res = register_on(&gate, "HashJoinInput[0]");
        res.try_grow(60).expect_err("the host gate is closed");
        res.try_grow(1).expect_err("the host gate is still closed");

        assert_eq!(
            [
                (query.first_refusal(), query.host_gate_refused()),
                (tenant.first_refusal(), tenant.host_gate_refused()),
                (process.first_refusal(), process.host_gate_refused()),
                (gate.first_refusal(), gate.host_gate_refused()),
            ],
            [
                (Some(MemoryRefusal::Query), false),
                (Some(MemoryRefusal::Tenant), false),
                (Some(MemoryRefusal::Process), false),
                (Some(MemoryRefusal::HostGate), true),
            ]
        );
        assert_eq!(
            [
                MemoryRefusal::Query,
                MemoryRefusal::Tenant,
                MemoryRefusal::Process,
                MemoryRefusal::HostGate,
                MemoryRefusal::ReserveAhead,
            ]
            .map(MemoryRefusal::as_str),
            [
                "query_pool",
                "tenant_budget",
                "process_budget",
                "host_memory_gate",
                "aggregate_reserve_ahead",
            ]
        );
    }

    /// `query_pool_spill_mode` builds the `query_pool` pool with the headroom
    /// `spill_mode_headroom` gives its limit; `query_pool` stays out of spill
    /// mode.
    #[test]
    fn query_pool_spill_mode_is_query_pool_with_the_headroom() {
        let config = crate::config::SqlConfig {
            max_query_bytes: 1000 << 20,
            ..crate::config::SqlConfig::default()
        };
        let tenant = TenantMemoryAccountant::new(1 << 40);
        let (spill, _) = config.query_pool_spill_mode(Arc::clone(&tenant), QueryAccounting::new());
        let (plain, _) = config.query_pool(tenant, QueryAccounting::new());
        let spill = spill
            .downcast_ref::<TenantDelegatingPool>()
            .expect("a TenantDelegatingPool");
        let plain = plain
            .downcast_ref::<TenantDelegatingPool>()
            .expect("a TenantDelegatingPool");
        assert_eq!(spill.spill_headroom, Some(100 << 20));
        assert_eq!(plain.spill_headroom, None);
        assert_eq!(spill.query_limit, plain.query_limit);
        assert_eq!(spill.reserve_ahead, plain.reserve_ahead);
        assert_eq!(spill.envelope_width, plain.envelope_width);
    }

    /// The spill-mode headroom is the larger of its floor and its share.
    #[test]
    fn the_spill_mode_headroom_is_the_larger_of_the_floor_and_the_share() {
        assert_eq!(spill_mode_headroom(0), 64 << 20);
        assert_eq!(spill_mode_headroom(640 << 20), 64 << 20);
        assert_eq!(spill_mode_headroom(1000 << 20), 100 << 20);
        assert_eq!(spill_mode_headroom(usize::MAX), usize::MAX / 10);
    }
}
