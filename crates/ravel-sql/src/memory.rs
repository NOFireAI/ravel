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
//!   envelope for the next doubling it can make: `envelope_size_factor x S`
//!   while its asked size `S` is under 8 MiB, else the smaller of that and
//!   `envelope_multiplier x J_max`, where `J_max` is its largest single grow.
//!   Only the `W` largest envelopes in the process are reserved, on the
//!   process [`MemoryBudget`] alone, where `W` is the number of aggregate
//!   streams that can run `aggregate_batch` at once. A grow whose envelope
//!   change the budget refuses is itself refused before anything is charged.
//!
//! [`AggregateReserveAhead::OFF`] restores one-to-one charging and no
//! envelope.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
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

/// How far ahead of what they ask for the migrated hash aggregate streams are
/// charged (issue #3181). See the module documentation for the two parts.
///
/// [`Self::OFF`] (`1.0`, `0`, `0`) charges every consumer one to one and
/// reserves no envelope.
#[derive(Debug, Clone, Copy)]
pub struct AggregateReserveAhead {
    /// Multiplier on every grow and shrink a migrated aggregate asks for,
    /// applied on the query, tenant and process ledgers.
    pub undercount_factor: f64,
    /// Multiplier on a building consumer's largest single grow, once it has
    /// asked for 8 MiB or more.
    pub envelope_multiplier: f64,
    /// Multiplier on a building consumer's asked size: its envelope while it
    /// is under 8 MiB, and the cap on its envelope after.
    pub envelope_size_factor: f64,
}

impl AggregateReserveAhead {
    /// One-to-one charging and no envelope.
    pub const OFF: AggregateReserveAhead = AggregateReserveAhead {
        undercount_factor: 1.0,
        envelope_multiplier: 0.0,
        envelope_size_factor: 0.0,
    };

    /// The default: a 1.25 undercount factor, and an envelope of five times
    /// the largest step, capped at 2.43 times the asked size.
    pub const DEFAULT: AggregateReserveAhead = AggregateReserveAhead {
        undercount_factor: 1.25,
        envelope_multiplier: 5.0,
        envelope_size_factor: 2.43,
    };

    /// Every value finite, the factor at least 1 and both multipliers at
    /// least 0.
    pub fn validate(&self) -> Result<(), AggregateReserveAheadError> {
        let values = [
            ("undercount_factor", self.undercount_factor),
            ("envelope_multiplier", self.envelope_multiplier),
            ("envelope_size_factor", self.envelope_size_factor),
        ];
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

    /// The envelope of a building consumer that has asked for `size` bytes in
    /// steps no larger than `largest_step`.
    fn envelope(&self, size: usize, largest_step: usize) -> usize {
        let by_size = scale(size, self.envelope_size_factor);
        if size < ENVELOPE_STEP_BOUND_FROM {
            by_size
        } else {
            scale(largest_step, self.envelope_multiplier).min(by_size)
        }
    }
}

impl Default for AggregateReserveAhead {
    fn default() -> Self {
        AggregateReserveAhead::DEFAULT
    }
}

impl PartialEq for AggregateReserveAhead {
    fn eq(&self, other: &Self) -> bool {
        self.undercount_factor.to_bits() == other.undercount_factor.to_bits()
            && self.envelope_multiplier.to_bits() == other.envelope_multiplier.to_bits()
            && self.envelope_size_factor.to_bits() == other.envelope_size_factor.to_bits()
    }
}

impl Eq for AggregateReserveAhead {}

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
/// Resolved by the first caller and fixed for the life of the process.
pub fn process_envelope_width(partitions: usize) -> NonZeroUsize {
    static WIDTH: OnceLock<NonZeroUsize> = OnceLock::new();
    *WIDTH.get_or_init(|| {
        let parallelism = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        NonZeroUsize::new(parallelism.min(partitions)).unwrap_or(NonZeroUsize::MIN)
    })
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

/// Envelope bytes currently on `budget`, for tests that share the process
/// ledger with every other test in the binary.
#[cfg(test)]
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
    /// Its envelope on the process ledger.
    envelope: usize,
    /// Whether it has shrunk; from then on it carries no envelope.
    shrunk: bool,
}

impl AggregateState {
    fn grown(self, additional: usize, reserve_ahead: &AggregateReserveAhead) -> Self {
        let asked = self.asked.saturating_add(additional);
        if self.shrunk {
            return AggregateState { asked, ..self };
        }
        let largest_step = self.largest_step.max(additional);
        AggregateState {
            asked,
            largest_step,
            envelope: reserve_ahead.envelope(asked, largest_step),
            shrunk: false,
        }
    }
}

/// Source of [`TenantDelegatingPool`] ids, which key a pool's envelopes on
/// the process ledger.
static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(0);

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
        }
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
        let mut aggregates = self.aggregates_map();
        let state = aggregates.get(&id).copied().unwrap_or_default();
        let next = state.grown(additional, &self.reserve_ahead);
        if next.envelope != state.envelope {
            self.set_envelope(id, next.envelope, EnvelopeCheck::Checked)
                .map_err(|refused| {
                    DataFusionError::ResourcesExhausted(format!(
                        "aggregate reserve-ahead refused: {additional} bytes asked by {} need                          the process aggregate envelope to grow by {} bytes: {}",
                        consumer.name(),
                        refused.delta,
                        refused.exhausted
                    ))
                })?;
        }
        let charged = self
            .reserve_ahead
            .charge(next.asked)
            .saturating_sub(self.reserve_ahead.charge(state.asked));
        if let Err(error) = self.try_grow_charged(consumer, charged) {
            if next.envelope != state.envelope {
                self.set_envelope_unchecked(id, state.envelope);
            }
            return Err(error);
        }
        aggregates.insert(id, next);
        Ok(())
    }

    /// The unconditional grow of a migrated aggregate.
    fn aggregate_grow(&self, consumer: &MemoryConsumer, additional: usize) {
        let id = consumer.id();
        let mut aggregates = self.aggregates_map();
        let state = aggregates.get(&id).copied().unwrap_or_default();
        let next = state.grown(additional, &self.reserve_ahead);
        if next.envelope != state.envelope {
            self.set_envelope_unchecked(id, next.envelope);
        }
        let charged = self
            .reserve_ahead
            .charge(next.asked)
            .saturating_sub(self.reserve_ahead.charge(state.asked));
        self.grow_charged(consumer, charged);
        aggregates.insert(id, next);
    }

    /// Record a migrated aggregate's shrink and return the charged bytes it
    /// frees. Its first shrink ends its build, and its envelope with it.
    fn aggregate_shrink(&self, consumer: &MemoryConsumer, shrink: usize) -> usize {
        let id = consumer.id();
        let mut aggregates = self.aggregates_map();
        let state = aggregates.entry(id).or_default();
        let asked = state.asked.saturating_sub(shrink);
        let released = self
            .reserve_ahead
            .charge(state.asked)
            .saturating_sub(self.reserve_ahead.charge(asked));
        state.asked = asked;
        if !state.shrunk {
            state.shrunk = true;
            if state.envelope > 0 {
                state.envelope = 0;
                self.set_envelope_unchecked(id, 0);
            }
        }
        released
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
        }
    }

    /// Charge `amount` bytes for `consumer` on all three budgets or none,
    /// serving what its hold covers from the hold.
    fn try_grow_charged(&self, consumer: &MemoryConsumer, amount: usize) -> DFResult<()> {
        let from_held = if self.may_hold(consumer) {
            self.take_held(consumer.id(), amount)
        } else {
            0
        };
        let excess = amount - from_held;
        if excess == 0 {
            return Ok(());
        }
        let charged = self.try_charge(excess);
        if charged.is_err() && from_held > 0 {
            // A refused try_grow leaves the reservation's size unchanged, so
            // the bytes taken from the hold go back to it.
            self.hold(consumer.id(), from_held);
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
    /// or none.
    fn try_charge(&self, additional: usize) -> DFResult<()> {
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
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        // Forwarded to the tenant accountant, including the shrink DataFusion
        // issues when a MemoryReservation is dropped: this is what
        // makes a cancelled or dropped stream return its tenant reservation to
        // zero. A held aggregate's bytes return at its `unregister` instead.
        let consumer = reservation.consumer();
        let charged = if self.reserves_ahead(consumer) {
            self.aggregate_shrink(consumer, shrink)
        } else {
            shrink
        };
        if self.holds_shrink(reservation) {
            self.hold(consumer.id(), charged);
        } else {
            self.release(charged);
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        let consumer = reservation.consumer();
        if self.reserves_ahead(consumer) {
            self.aggregate_try_grow(consumer, additional)
        } else {
            self.try_grow_charged(consumer, additional)
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
        let res = ledgers.register_consumer(MemoryConsumer::new(FINAL_AGGREGATE));
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
        let res = ledgers.register_consumer(MemoryConsumer::new(FINAL_AGGREGATE));
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
             limit 600)"
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
    /// building 3.1M groups of q33's shape, as DataFusion 55.2 asks for them:
    /// three table doublings of 14 bytes a bucket, each followed by the `Vec`
    /// step that ends the batch.
    const SERIES: [usize; 6] = [
        7_340_032,
        27_262_976,
        14_680_064,
        54_525_952,
        29_360_128,
        109_051_904,
    ];

    /// FLIP: an `envelope_multiplier` of 2 leaves 218,103,808 bytes, under
    /// the 4 x 109,051,904 the next `Vec` doubling needs.
    #[test]
    fn the_envelope_is_five_times_the_largest_step_and_covers_both_doublings() {
        let budget = Arc::new(MemoryBudget::new(1 << 40));
        let (_tenant, pool) = ahead_pool(
            &budget,
            1,
            AggregateReserveAhead::DEFAULT,
            QueryAccounting::new(),
        );
        let res = register_on(&pool, PARTIAL);

        let mut envelopes = Vec::new();
        for step in SERIES {
            res.try_grow(step).expect("within every ceiling");
            envelopes.push(envelope_on_budget(&budget));
            assert_eq!(
                budget.reserved() - pool.reserved() as u64,
                envelope_on_budget(&budget),
                "the envelope is on the process budget and nowhere else"
            );
        }

        // Under 8 MiB the size bound alone: 2.43 x 7,340,032.
        assert_eq!(envelopes[0], 17_836_278);
        // From then on the smaller of 5 x J_max and 2.43 x S.
        let sizes: Vec<u64> = SERIES
            .iter()
            .scan(0u64, |size, step| {
                *size += *step as u64;
                Some(*size)
            })
            .collect();
        let mut largest = 0u64;
        for (index, step) in SERIES.iter().enumerate().skip(1) {
            largest = largest.max(*step as u64);
            let by_size = (sizes[index] as f64 * 2.43).ceil() as u64;
            assert_eq!(envelopes[index], (5 * largest).min(by_size), "step {index}");
        }
        let last = *envelopes.last().expect("six steps");
        assert_eq!(last, 545_259_520, "5 x the 109,051,904-byte Vec step");
        assert!(
            last >= 4 * 109_051_904,
            "the next Vec doubling's transient: {last}"
        );
        assert!(
            last as f64 >= 4.86 * 29_360_128.0,
            "the next table doubling's transient: {last}"
        );
        assert_eq!(
            pool.reserved(),
            (SERIES.iter().sum::<usize>() as f64 * 1.25).ceil() as usize
        );

        drop(res);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn a_consumer_under_eight_mebibytes_carries_the_size_bound() {
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
            budget.reserved() - pools,
            5 * (2 + 3) * MIB as u64,
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
            message.contains("4194304 bytes asked by PartialHashAggregateStream[0]"),
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
            envelope_multiplier: -1.0,
            ..AggregateReserveAhead::DEFAULT
        };
        assert!(matches!(
            negative.validate(),
            Err(AggregateReserveAheadError::Negative {
                name: "envelope_multiplier",
                ..
            })
        ));
    }
}
