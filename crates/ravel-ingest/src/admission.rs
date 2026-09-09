//! Per-tenant admission control (ADR-0051 sections 1, 2, 6), implementing
//! the quota decision ADR-0009 left unenforced ("per-tenant quotas … hard
//! limits precede allocation").
//!
//! [`AdmissionController`] is a self-contained accounting unit: it holds no
//! reference to a shard, router, or gateway and enforces nothing by itself.
//! A later wiring task is expected to call it in this order, once per
//! ingest request, before any shard-buffer allocation (ADR-0051 section 1):
//!
//! 1. [`AdmissionController::check_byte_rate`] on the wire body size,
//!    before decode. Whole-request rejection.
//! 2. For metrics and logs only, [`AdmissionController::check_series_creation_rate`]
//!    / [`AdmissionController::check_stream_creation_rate`] on the
//!    normalized batch's distinct new-identity demand. Whole-request
//!    rejection: a partially admitted batch that the client retries
//!    re-ingests the admitted remainder, and for logs that duplication is
//!    user-visible.
//! 3. [`AdmissionController::admit_series`] / [`AdmissionController::admit_streams`],
//!    which is where the active-cap rejects individual new identities
//!    while already-active ones keep flowing (per-series partial success).
//!
//! Spans have no stable series identity (`trace_id` is sender-chosen and
//! naturally unbounded, ADR-0051 section 1) and so get neither a creation
//! rate nor an active cap here. There is deliberately no `admit_spans`
//! method or span variant of [`AdmissionLimits`]'s count caps: span ingest
//! is bounded only by [`AdmissionController::check_byte_rate`], and that
//! absence is the documented answer ADR-0051 requires, not a gap.
//!
//! Enforcement state is per process (ADR-0051 section 2): with N ingest
//! replicas the fleet-wide effective bound is N times each configured
//! limit. This is a deliberate trade (see ADR-0051 "Rejected alternatives"
//! 1), not an oversight.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use ravel_types::logstream::LogStreamId;
use ravel_types::{SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

use crate::Clock;

/// Width of the rotating active-identity epoch (ADR-0051 section 2). Fixed,
/// not a configuration knob: an active series/stream is one seen in the
/// current or previous epoch.
const ACTIVE_EPOCH_NS: i64 = 3_600 * 1_000_000_000;

/// A per-tenant count cap, or an explicit opt-in to no cap at all.
///
/// ADR-0051 section 3: enforcement is on by default with finite shipped
/// defaults; a tenant needing no limit sets `unlimited = true` explicitly,
/// visible in config review rather than silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountLimit {
    Bounded(u64),
    Unlimited,
}

impl CountLimit {
    fn admits_one_more(self, active_count: u64) -> bool {
        match self {
            CountLimit::Bounded(cap) => active_count < cap,
            CountLimit::Unlimited => true,
        }
    }
}

/// A per-tenant token-bucket rate cap, or an explicit opt-in to no cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimit {
    Bounded { per_sec: u64, burst: u64 },
    Unlimited,
}

/// Per-tenant admission limits (ADR-0051 section 3).
///
/// The `--limits-file` TOML config task (landing separately) is expected to
/// deserialize a `[defaults]` table into one `AdmissionLimits`, then for
/// each `[tenants.<tenant-id>]` table merge overridden fields over a clone
/// of the default to build that tenant's `AdmissionLimits`, and hand each
/// one to [`AdmissionController::new`] (defaults) or
/// [`AdmissionController::set_tenant_limits`] (a specific tenant's
/// override) at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionLimits {
    pub max_active_series: CountLimit,
    pub max_active_streams: CountLimit,
    pub ingest_byte_rate: RateLimit,
    pub series_creation_rate: RateLimit,
}

impl AdmissionLimits {
    pub const DEFAULT_MAX_ACTIVE_SERIES: u64 = 1_000_000;
    pub const DEFAULT_MAX_ACTIVE_STREAMS: u64 = 1_000_000;
    pub const DEFAULT_INGEST_BYTES_PER_SEC: u64 = 32 * 1024 * 1024;
    pub const DEFAULT_INGEST_BYTE_BURST: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_SERIES_CREATION_RATE_PER_SEC: u64 = 10_000;
    pub const DEFAULT_SERIES_CREATION_BURST: u64 = 100_000;
}

/// ADR-0051 section 3's defaults, NOT what `services/ravel-server` ships.
///
/// `DEFAULT_MAX_ACTIVE_SERIES` / `DEFAULT_MAX_ACTIVE_STREAMS` here are
/// 1,000,000, the ADR's original figure, assuming ~16 bytes per tracked
/// identity in the two-epoch `HashSet` tracker. Measurements put the
/// real cost at 35-56 bytes per live entry (hashbrown's power-of-two
/// table sizing plus allocator headroom), so at 1,000,000 the worst case
/// (cap x bytes-per-entry x 2 rotating epochs x 2 tracked signals) is
/// roughly 140-224 MiB per fully active tenant. `ravel-server` overrides
/// both to 200,000 in its own shipped defaults
/// (`services/ravel-server/src/config.rs`, `limits::shipped_defaults`),
/// cutting that worst case to roughly 27-43 MiB, and does not construct
/// this `Default` impl in its startup path. The two crates carrying
/// different defaults is a known discrepancy, not yet reconciled.
impl Default for AdmissionLimits {
    fn default() -> Self {
        AdmissionLimits {
            max_active_series: CountLimit::Bounded(Self::DEFAULT_MAX_ACTIVE_SERIES),
            max_active_streams: CountLimit::Bounded(Self::DEFAULT_MAX_ACTIVE_STREAMS),
            ingest_byte_rate: RateLimit::Bounded {
                per_sec: Self::DEFAULT_INGEST_BYTES_PER_SEC,
                burst: Self::DEFAULT_INGEST_BYTE_BURST,
            },
            series_creation_rate: RateLimit::Bounded {
                per_sec: Self::DEFAULT_SERIES_CREATION_RATE_PER_SEC,
                burst: Self::DEFAULT_SERIES_CREATION_BURST,
            },
        }
    }
}

/// Why a whole request was rejected. Both variants are whole-request,
/// retryable-later rejections (ADR-0051 section 1), unlike the per-series
/// active-cap outcome carried by [`IdentityAdmission`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RequestRejectionReason {
    #[error("ingest byte rate exceeded")]
    ByteRate,
    #[error("series/stream creation rate exceeded")]
    SeriesCreationRate,
}

/// A whole-request rejection. `retry_after_ns` is the time until the
/// request's own cost would fit in the bucket, for the gateway to surface
/// as `Retry-After` / the gRPC retry-info equivalent (ADR-0051 section 1's
/// status-code table); mapping it to a header is the wiring task's job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestRejection {
    pub reason: RequestRejectionReason,
    pub retry_after_ns: i64,
}

/// Outcome of admitting a batch of series or log-stream identities: an
/// already-active identity is always admitted; a new one is admitted only
/// while the tenant's active cap has room (ADR-0051 section 1, per-series
/// partial success — never a whole-batch rejection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityAdmission<T> {
    pub admitted: Vec<T>,
    pub rejected: Vec<T>,
}

// Hand-written rather than `#[derive(Default)]`: the derive adds a spurious
// `T: Default` bound, but an empty `Vec<T>` needs no such bound on `T`.
impl<T> Default for IdentityAdmission<T> {
    fn default() -> Self {
        IdentityAdmission {
            admitted: Vec::new(),
            rejected: Vec::new(),
        }
    }
}

/// Token bucket over an injected clock. Never partially drains on a
/// rejected request (ADR-0051 section 1: an over-budget request must not
/// pay for the part of itself that didn't fit).
#[derive(Debug, Clone, Copy)]
struct TokenBucket {
    capacity: u64,
    rate_per_sec: u64,
    tokens: u64,
    last_refill_ns: i64,
}

impl TokenBucket {
    fn new(rate_per_sec: u64, burst: u64, now_ns: i64) -> Self {
        TokenBucket {
            capacity: burst,
            rate_per_sec,
            tokens: burst,
            last_refill_ns: now_ns,
        }
    }

    fn refill(&mut self, now_ns: i64) {
        let elapsed_ns = now_ns.saturating_sub(self.last_refill_ns).max(0) as u128;
        let refilled = elapsed_ns.saturating_mul(self.rate_per_sec as u128) / 1_000_000_000;
        self.tokens = self
            .tokens
            .saturating_add(u64::try_from(refilled).unwrap_or(u64::MAX))
            .min(self.capacity);
        // A caller-supplied `now_ns` can move backward (an adversarial
        // caller, or two honest concurrent callers reaching this bucket's
        // lock in reverse clock order): never let the anchor regress, or a
        // later forward call measures elapsed time from that stale earlier
        // point and double-credits the reordered interval.
        self.last_refill_ns = self.last_refill_ns.max(now_ns);
    }

    /// Attempts to take `amount` tokens; on failure nothing is taken, and
    /// the error carries the wait until `amount` tokens would be available.
    fn try_take(&mut self, amount: u64, now_ns: i64) -> Result<(), i64> {
        self.refill(now_ns);
        if self.tokens >= amount {
            self.tokens -= amount;
            Ok(())
        } else if self.rate_per_sec == 0 {
            Err(i64::MAX)
        } else {
            let deficit = (amount - self.tokens) as u128;
            let retry_after_ns = deficit
                .saturating_mul(1_000_000_000)
                .div_ceil(self.rate_per_sec as u128);
            Err(i64::try_from(retry_after_ns).unwrap_or(i64::MAX))
        }
    }

    /// Whether `amount` tokens are currently available, taking none. Refills
    /// first (crediting elapsed time) exactly as [`Self::try_take`], so a peek
    /// and an immediately following take at the same `now_ns` agree; on
    /// insufficient tokens the error carries the same wait `try_take` reports.
    fn peek(&mut self, amount: u64, now_ns: i64) -> Result<(), i64> {
        self.refill(now_ns);
        if self.tokens >= amount {
            Ok(())
        } else if self.rate_per_sec == 0 {
            Err(i64::MAX)
        } else {
            let deficit = (amount - self.tokens) as u128;
            let retry_after_ns = deficit
                .saturating_mul(1_000_000_000)
                .div_ceil(self.rate_per_sec as u128);
            Err(i64::try_from(retry_after_ns).unwrap_or(i64::MAX))
        }
    }
}

/// Exact active-identity tracker over two rotating one-hour epochs
/// (ADR-0051 section 2). An identity is active if seen in the current or
/// previous epoch; the tracked count is bounded by `cap` because a genuinely
/// new identity is refused once `cap` distinct identities are active, which
/// is the whole point: the set never grows past the limit it enforces.
#[derive(Debug)]
struct EpochIdSet<T> {
    epoch_index: i64,
    current: HashSet<T>,
    previous: HashSet<T>,
    active_count: u64,
    /// Monotonic count of genuinely new identities admitted over this set's
    /// lifetime (never decremented on rotation). The reconciliation snapshot
    /// (ADR-0057) reports the delta of this since its previous snapshot as
    /// `series_creation_consumed_since_last_snapshot`: a flow, not the stock
    /// `active_count` tracks.
    created_total: u64,
}

impl<T: Eq + Hash + Copy> EpochIdSet<T> {
    fn new(now_ns: i64) -> Self {
        EpochIdSet {
            epoch_index: Self::epoch_of(now_ns),
            current: HashSet::new(),
            previous: HashSet::new(),
            active_count: 0,
            created_total: 0,
        }
    }

    fn epoch_of(now_ns: i64) -> i64 {
        now_ns.div_euclid(ACTIVE_EPOCH_NS)
    }

    /// Rotates epochs if `now_ns` has crossed into a new one. Exactly one
    /// elapsed epoch demotes `current` to `previous` (the old `previous`
    /// ages out); more than one elapsed epoch means everything in both
    /// generations is now stale, so both are cleared. Because a rotation by
    /// exactly one epoch always sets `previous := old current` and
    /// `current := {}`, the new active count is just `old current`'s size:
    /// no set-difference walk is needed on the hot path.
    fn rotate(&mut self, now_ns: i64) {
        let epoch = Self::epoch_of(now_ns);
        let elapsed = epoch.saturating_sub(self.epoch_index);
        if elapsed <= 0 {
            return;
        }
        if elapsed == 1 {
            self.previous = std::mem::take(&mut self.current);
            self.active_count = self.previous.len() as u64;
        } else {
            self.previous.clear();
            self.current.clear();
            self.active_count = 0;
        }
        self.epoch_index = epoch;
    }

    fn is_active(&self, id: &T) -> bool {
        self.current.contains(id) || self.previous.contains(id)
    }

    /// Admits `id`, returning whether it was accepted. An already-active id
    /// is always accepted (and refreshed into the current epoch so it
    /// survives the next rotation); a new id is accepted only if `cap` has
    /// room.
    fn admit(&mut self, id: T, cap: CountLimit) -> bool {
        if self.current.contains(&id) {
            return true;
        }
        if self.previous.contains(&id) {
            self.current.insert(id);
            return true;
        }
        if cap.admits_one_more(self.active_count) {
            self.current.insert(id);
            self.active_count += 1;
            self.created_total = self.created_total.saturating_add(1);
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SignalUsage {
    requests_admitted_total: u64,
    bytes_admitted_total: u64,
    requests_rejected_byte_rate_total: u64,
    requests_rejected_series_rate_total: u64,
    /// Whole requests rejected because the receiver's admission clock was
    /// implausible (below `MIN_PLAUSIBLE_INGEST_CLOCK_NS` or non-representable,
    /// ADR-0051 amendment). The fault is the replica's, not
    /// the request's, but the counter is per (tenant, signal) so it renders in
    /// the `ravel_admission_rejected_total{reason="clock"}` family (§6). See
    /// [`AdmissionController::record_clock_rejection`].
    requests_rejected_clock_total: u64,
    series_admitted_total: u64,
    series_rejected_cap_total: u64,
    /// Fleet-admission reconciliation read failures for this (tenant, signal)
    /// (ADR-0057 section 3): a LIST or GET of the sibling snapshots failed, so
    /// this process kept its last-computed `local_soft_threshold` rather than
    /// failing admission closed. Surfaced as
    /// `ravel_admission_reconciliation_failures_total`.
    reconciliation_failures_total: u64,
}

/// One (tenant, signal) row of the usage snapshot (ADR-0051 section 6).
/// `tenant_hash` rather than the raw [`TenantId`] so a caller building
/// Prometheus labels never has an unbounded tenant-supplied string to
/// bound itself: hashing to a fixed 16 bytes happens here, at the source
/// (the `tenant_hash` → `other` fold for unconfigured tenants is the
/// exporter's job, mirroring `TenantHashLabel` in
/// `services/ravel-server/src/metrics.rs`, which stays outside this
/// crate's scope).
///
/// `active_series` holds the active-series count for `Signal::Metrics` and
/// the active-stream count for `Signal::Logs`; it is always 0 for every
/// other signal, since only those two have a tracked identity set.
///
/// Counters are split by the unit the controller actually observes at each
/// layer, rather than one conflated `admitted_total`: `requests_admitted_total`
/// / `bytes_admitted_total` count whole requests that passed
/// [`AdmissionController::check_byte_rate`] (layer 2); `series_admitted_total`
/// counts individual series/stream ids admitted by
/// [`AdmissionController::admit_series`] / `admit_streams` (layer 4). A
/// caller rendering ADR-0051 section 6's `ravel_admission_admitted_total`
/// family picks whichever of these matches the exported unit it wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantUsage {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub active_series: u64,
    pub requests_admitted_total: u64,
    pub bytes_admitted_total: u64,
    pub series_admitted_total: u64,
    pub requests_rejected_byte_rate_total: u64,
    pub requests_rejected_series_rate_total: u64,
    /// Whole requests rejected for an implausible receiver admission clock
    /// (ADR-0051 amendment). Rendered under
    /// `ravel_admission_rejected_total{reason="clock"}`. See
    /// [`SignalUsage::requests_rejected_clock_total`].
    pub requests_rejected_clock_total: u64,
    pub series_rejected_cap_total: u64,
    /// Fleet-admission reconciliation read failures (ADR-0057 section 3),
    /// per (tenant, signal). See [`SignalUsage::reconciliation_failures_total`].
    pub reconciliation_failures_total: u64,
}

struct TenantState {
    limits: AdmissionLimits,
    series: EpochIdSet<SeriesId>,
    streams: EpochIdSet<LogStreamId>,
    byte_rate: Option<TokenBucket>,
    creation_rate: Option<TokenBucket>,
    usage: HashMap<Signal, SignalUsage>,
    /// Effective fleet-reconciled count caps (ADR-0057 section 2). These, not
    /// `limits.max_active_series`/`max_active_streams`, are what the hot-path
    /// `admit_series`/`admit_streams` compare against. They start equal to the
    /// configured caps, so before the first reconciliation cycle (and after a
    /// limits change) enforcement is byte-identical to ADR-0051's per-process
    /// behavior. A successful reconciliation overwrites them with
    /// `local_soft_threshold`; a failed one leaves them at their last-known
    /// value (never fails closed to zero, ADR-0057 section 3). The rate caps
    /// are reconciled in place on the `byte_rate`/`creation_rate` buckets'
    /// `rate_per_sec`, so no separate effective field is needed for them.
    fleet_max_active_series: CountLimit,
    fleet_max_active_streams: CountLimit,
    /// Cumulative byte flow captured at this tenant's previous reconciliation
    /// snapshot, so the next snapshot reports the delta since it (ADR-0057
    /// section 1: a delta, not a running total).
    snapshot_baseline_bytes: u64,
    /// Cumulative new-identity (series + streams) creation flow captured at the
    /// previous snapshot, for the same delta reason.
    snapshot_baseline_created: u64,
}

impl TenantState {
    fn new(limits: AdmissionLimits, now_ns: i64) -> Self {
        let byte_rate = match limits.ingest_byte_rate {
            RateLimit::Bounded { per_sec, burst } => Some(TokenBucket::new(per_sec, burst, now_ns)),
            RateLimit::Unlimited => None,
        };
        let creation_rate = match limits.series_creation_rate {
            RateLimit::Bounded { per_sec, burst } => Some(TokenBucket::new(per_sec, burst, now_ns)),
            RateLimit::Unlimited => None,
        };
        TenantState {
            limits,
            series: EpochIdSet::new(now_ns),
            streams: EpochIdSet::new(now_ns),
            byte_rate,
            creation_rate,
            usage: HashMap::new(),
            fleet_max_active_series: limits.max_active_series,
            fleet_max_active_streams: limits.max_active_streams,
            snapshot_baseline_bytes: 0,
            snapshot_baseline_created: 0,
        }
    }

    /// Replaces this tenant's limits, resetting only the dimensions whose
    /// *configured* value actually changed since the last apply.
    ///
    /// Per-dimension guarding is load-bearing, not an optimization. The
    /// lifecycle refresh loop (ADR-0066 decision 6) now calls this every
    /// horizon for every known tenant, even when the durable config record is
    /// byte-identical to what was last applied -- it is no longer the
    /// startup-only "changing limits is a restart" path of ADR-0051 section 3.
    /// An unconditional overwrite would, on every horizon tick, clobber the
    /// per-process fleet share that [`AdmissionController::apply_reconciliation`]
    /// (ADR-0057) narrowed `fleet_max_active_series`/`fleet_max_active_streams`
    /// to, and rebuild each rate bucket at full burst under the raw un-divided
    /// `per_sec` -- discarding the narrowed `rate_per_sec` and handing out a
    /// free full burst refill that the next reconciliation cannot claw back (it
    /// only ever narrows `rate_per_sec`, never token counts). The result: every
    /// such tenant admits the full un-divided fleet cap for up to one reconcile
    /// interval out of every horizon (N times the intended ceiling across an
    /// N-process fleet). Guarding per dimension keeps a genuinely changed config
    /// taking effect within one horizon (a changed dimension still resets and
    /// its fleet cap reverts to the new configured value, to be re-narrowed by
    /// the next reconciliation) while leaving reconciliation's narrowing intact
    /// for every unchanged dimension -- honouring this method's long-standing
    /// "only a changed rate's bucket is reset" contract for the fleet-cap fields
    /// too. A changed rate's bucket is still rebuilt at full burst: limits
    /// genuinely changing is rare and operator-driven, so a one-time burst there
    /// is acceptable, whereas an unchanged-config refresh must be a true no-op.
    fn set_limits(&mut self, limits: AdmissionLimits, now_ns: i64) {
        if limits.ingest_byte_rate != self.limits.ingest_byte_rate {
            self.byte_rate = match limits.ingest_byte_rate {
                RateLimit::Bounded { per_sec, burst } => {
                    Some(TokenBucket::new(per_sec, burst, now_ns))
                }
                RateLimit::Unlimited => None,
            };
        }
        if limits.series_creation_rate != self.limits.series_creation_rate {
            self.creation_rate = match limits.series_creation_rate {
                RateLimit::Bounded { per_sec, burst } => {
                    Some(TokenBucket::new(per_sec, burst, now_ns))
                }
                RateLimit::Unlimited => None,
            };
        }
        if limits.max_active_series != self.limits.max_active_series {
            self.fleet_max_active_series = limits.max_active_series;
        }
        if limits.max_active_streams != self.limits.max_active_streams {
            self.fleet_max_active_streams = limits.max_active_streams;
        }
        self.limits = limits;
    }
}

/// Owns per-tenant admission state: exact active-series and active-stream
/// sets, a series-creation-rate token bucket, an ingest byte-rate token
/// bucket, and bounded usage counters. See the [module docs](self) for the
/// call sequence a wiring task drives this with.
///
/// Internally a single [`Mutex`] guards a `HashMap<TenantId, TenantState>`.
/// Every operation is O(1) amortized arithmetic and hash-map/hash-set work
/// with no `.await` under the lock, so contention is short; a busier
/// design (sharded locks, one per tenant) is not needed at the process
/// throughput this crate's shard actors already bound.
pub struct AdmissionController {
    clock: Arc<dyn Clock>,
    defaults: AdmissionLimits,
    tenants: Mutex<HashMap<TenantId, TenantState>>,
    /// A value stable for this process's lifetime and unique fleet-wide
    /// (ADR-0057 section 1). It names the one snapshot key this process owns and
    /// alone ever writes, `t/<tenant_hash>/<sig>/admission/<process_id>.snapshot`,
    /// and is how a reconciling reader excludes its own snapshot from the
    /// sibling sum. Generated once at construction; it need not be meaningful
    /// outside this mechanism.
    process_id: Uuid,
}

impl AdmissionController {
    pub fn new(clock: Arc<dyn Clock>, defaults: AdmissionLimits) -> Self {
        AdmissionController {
            clock,
            defaults,
            tenants: Mutex::new(HashMap::new()),
            process_id: Uuid::new_v4(),
        }
    }

    /// This process's stable, fleet-unique snapshot-key owner id (ADR-0057
    /// section 1). Used by the reconciliation task to name its own key and to
    /// exclude it from the sibling sum.
    pub fn process_id(&self) -> Uuid {
        self.process_id
    }

    fn now_ns(&self) -> i64 {
        self.clock.now_ns()
    }

    /// Installs or replaces a tenant's limits. Called once at startup from the
    /// limits file (ADR-0051 section 3) and, since ADR-0066 decision 6, again
    /// every refresh horizon by the lifecycle loop with each tenant's durable
    /// config record laid over its startup base. Existing active-series/stream
    /// state is preserved, and only a dimension whose configured value actually
    /// changed since the last apply is reset (see [`TenantState::set_limits`]);
    /// an unchanged-config refresh is a true no-op that leaves any
    /// fleet-reconciled narrowing (ADR-0057) in force.
    pub fn set_tenant_limits(&self, tenant: TenantId, limits: AdmissionLimits) {
        let now_ns = self.now_ns();
        let mut tenants = lock(&self.tenants);
        tenants
            .entry(tenant)
            .and_modify(|state| state.set_limits(limits, now_ns))
            .or_insert_with(|| TenantState::new(limits, now_ns));
    }

    fn with_tenant<R>(
        &self,
        tenant: &TenantId,
        now_ns: i64,
        f: impl FnOnce(&mut TenantState) -> R,
    ) -> R {
        let mut tenants = lock(&self.tenants);
        let state = tenants
            .entry(tenant.clone())
            .or_insert_with(|| TenantState::new(self.defaults, now_ns));
        f(state)
    }

    /// Charges `request_bytes` against the tenant's ingest byte-rate
    /// bucket. Whole-request: on rejection no tokens are consumed
    /// (ADR-0051 section 1, layer 2). Applies uniformly to every signal,
    /// including spans, since it is a per-tenant, not per-signal, budget.
    pub fn check_byte_rate(
        &self,
        tenant: &TenantId,
        signal: Signal,
        request_bytes: u64,
        now_ns: i64,
    ) -> Result<(), RequestRejection> {
        self.with_tenant(tenant, now_ns, |state| {
            let usage = state.usage.entry(signal).or_default();
            match &mut state.byte_rate {
                None => {
                    usage.requests_admitted_total += 1;
                    usage.bytes_admitted_total += request_bytes;
                    Ok(())
                }
                Some(bucket) => match bucket.try_take(request_bytes, now_ns) {
                    Ok(()) => {
                        usage.requests_admitted_total += 1;
                        usage.bytes_admitted_total += request_bytes;
                        Ok(())
                    }
                    Err(retry_after_ns) => {
                        usage.requests_rejected_byte_rate_total += 1;
                        Err(RequestRejection {
                            reason: RequestRejectionReason::ByteRate,
                            retry_after_ns,
                        })
                    }
                },
            }
        })
    }

    /// Non-consuming pre-check of `request_bytes` against the tenant's ingest
    /// byte-rate bucket, for the OTLP gzip path (ADR-0084 decision 4). The
    /// compressed body length is a strict lower bound on the decompressed
    /// length, so if even the compressed size exceeds the available tokens the
    /// gateway rejects 429 before inflating anything; the real charge is then
    /// made on the decompressed size via [`Self::check_byte_rate`].
    ///
    /// Unlike [`Self::check_byte_rate`] this takes no tokens and increments no
    /// admitted counter on success; on rejection it records the same
    /// `requests_rejected_byte_rate_total` a consuming rejection would, so a
    /// pre-check 429 is attributed identically in `/metrics`.
    pub fn peek_byte_rate(
        &self,
        tenant: &TenantId,
        signal: Signal,
        request_bytes: u64,
        now_ns: i64,
    ) -> Result<(), RequestRejection> {
        self.with_tenant(tenant, now_ns, |state| {
            let usage = state.usage.entry(signal).or_default();
            match &mut state.byte_rate {
                None => Ok(()),
                Some(bucket) => match bucket.peek(request_bytes, now_ns) {
                    Ok(()) => Ok(()),
                    Err(retry_after_ns) => {
                        usage.requests_rejected_byte_rate_total += 1;
                        Err(RequestRejection {
                            reason: RequestRejectionReason::ByteRate,
                            retry_after_ns,
                        })
                    }
                },
            }
        })
    }

    /// Records a whole-request rejection for an implausible receiver admission
    /// clock (ADR-0051 amendment), so it surfaces under
    /// `ravel_admission_rejected_total{reason="clock"}` (§6). The gateway calls
    /// this at admission when [`crate::plausible_ingest_clock`] fails, then
    /// rejects the whole request with HTTP 503 / gRPC `UNAVAILABLE`.
    ///
    /// `now_ns` is the plausibility floor rather than the bad clock reading:
    /// the reading is by definition nonsense here, and seeding a fresh tenant's
    /// epoch/token state from it would be worse than a fixed in-range anchor.
    pub fn record_clock_rejection(&self, tenant: &TenantId, signal: Signal) {
        self.with_tenant(tenant, crate::MIN_PLAUSIBLE_INGEST_CLOCK_NS, |state| {
            state
                .usage
                .entry(signal)
                .or_default()
                .requests_rejected_clock_total += 1;
        });
    }

    /// Charges the batch's distinct new-series demand against the tenant's
    /// shared series-creation-rate bucket (ADR-0051 section 1, layer 4).
    /// `candidate_series` may repeat ids and may include already-active
    /// ones; only ids not currently active count toward the charge.
    /// Whole-request: on rejection nothing is admitted and no tokens are
    /// consumed, so a caller must not call [`Self::admit_series`] for this
    /// batch.
    pub fn check_series_creation_rate(
        &self,
        tenant: &TenantId,
        candidate_series: &[SeriesId],
        now_ns: i64,
    ) -> Result<(), RequestRejection> {
        self.with_tenant(tenant, now_ns, |state| {
            state.series.rotate(now_ns);
            let new_count = count_new(&state.series, candidate_series);
            charge_creation_rate(state, Signal::Metrics, new_count, now_ns)
        })
    }

    /// Log-stream analogue of [`Self::check_series_creation_rate`], sharing
    /// the same per-tenant creation-rate bucket (ADR-0051 section 3 lists
    /// one `series_creation_rate_per_sec` knob, not one per signal).
    pub fn check_stream_creation_rate(
        &self,
        tenant: &TenantId,
        candidate_streams: &[LogStreamId],
        now_ns: i64,
    ) -> Result<(), RequestRejection> {
        self.with_tenant(tenant, now_ns, |state| {
            state.streams.rotate(now_ns);
            let new_count = count_new(&state.streams, candidate_streams);
            charge_creation_rate(state, Signal::Logs, new_count, now_ns)
        })
    }

    /// Admits a batch of metrics series ids against the tenant's active-
    /// series cap (ADR-0051 section 1, layer 4 / section 2). Per-series:
    /// already-active ids are always admitted, a new id is rejected only
    /// once the cap is full. Call only after a prior
    /// [`Self::check_series_creation_rate`] on the same batch succeeded.
    pub fn admit_series(
        &self,
        tenant: &TenantId,
        candidate_series: impl IntoIterator<Item = SeriesId>,
        now_ns: i64,
    ) -> IdentityAdmission<SeriesId> {
        self.with_tenant(tenant, now_ns, |state| {
            state.series.rotate(now_ns);
            // The effective fleet-reconciled cap (ADR-0057 section 2), which
            // starts equal to `limits.max_active_series` and is updated only by
            // reconciliation off the hot path. Same field read, same cost.
            let cap = state.fleet_max_active_series;
            let usage = state.usage.entry(Signal::Metrics).or_default();
            admit_batch(&mut state.series, candidate_series, cap, usage)
        })
    }

    /// Log-stream analogue of [`Self::admit_series`], against
    /// `max_active_streams`.
    pub fn admit_streams(
        &self,
        tenant: &TenantId,
        candidate_streams: impl IntoIterator<Item = LogStreamId>,
        now_ns: i64,
    ) -> IdentityAdmission<LogStreamId> {
        self.with_tenant(tenant, now_ns, |state| {
            state.streams.rotate(now_ns);
            // The effective fleet-reconciled cap (ADR-0057 section 2), as in
            // `admit_series`.
            let cap = state.fleet_max_active_streams;
            let usage = state.usage.entry(Signal::Logs).or_default();
            admit_batch(&mut state.streams, candidate_streams, cap, usage)
        })
    }

    /// Bounded per-tenant usage snapshot for export (ADR-0051 section 6):
    /// one row per (tenant, signal) pair that has recorded any activity.
    /// Cardinality is bounded by (observed tenants) × (`Signal` variants,
    /// a closed 6-member enum); it is never keyed by a raw tenant-supplied
    /// string; see [`TenantUsage`].
    pub fn usage_snapshot(&self) -> Vec<TenantUsage> {
        let tenants = lock(&self.tenants);
        let mut out = Vec::new();
        for (tenant, state) in tenants.iter() {
            let tenant_hash = tenant.hash();
            for (&signal, usage) in &state.usage {
                let active_series = match signal {
                    Signal::Metrics => state.series.active_count,
                    Signal::Logs => state.streams.active_count,
                    _ => 0,
                };
                out.push(TenantUsage {
                    tenant_hash,
                    signal,
                    active_series,
                    requests_admitted_total: usage.requests_admitted_total,
                    bytes_admitted_total: usage.bytes_admitted_total,
                    series_admitted_total: usage.series_admitted_total,
                    requests_rejected_byte_rate_total: usage.requests_rejected_byte_rate_total,
                    requests_rejected_series_rate_total: usage.requests_rejected_series_rate_total,
                    requests_rejected_clock_total: usage.requests_rejected_clock_total,
                    series_rejected_cap_total: usage.series_rejected_cap_total,
                    reconciliation_failures_total: usage.reconciliation_failures_total,
                });
            }
        }
        out
    }

    // ----- Fleet-global admission reconciliation (ADR-0057) -----
    //
    // The hooks the periodic reconciliation task in [`crate::reconcile`] drives.
    // Both lock the tenant map briefly and do NO I/O and NO `.await` under the
    // lock (the same discipline as the hot-path checks): the task does its
    // object-store LIST/GET between these two calls, holding no lock.

    /// Capture every tracked tenant's current usage for one reconciliation
    /// cycle (ADR-0057 sections 1-2). Each returned entry carries the flow
    /// deltas "since the previous snapshot" and the running totals those deltas
    /// were computed from, but does **not** advance the flow baselines: the
    /// baseline only commits once this tenant's reconciliation cycle actually
    /// succeeds, in [`Self::apply_reconciliation`] (which the caller reaches
    /// only after a successful sibling read). If the read fails, the caller
    /// skips the apply and the baseline stays put, so this interval's delta is
    /// rolled into the next successful interval rather than silently dropped --
    /// a given delta is counted in exactly one interval's threshold
    /// computation, never dropped and never double-counted. Returns one entry
    /// per tenant that has any recorded activity; the caller writes a per-signal
    /// snapshot for each and reads the siblings.
    pub(crate) fn snapshot_for_reconciliation(&self, now_ns: i64) -> Vec<TenantReconcileState> {
        let mut tenants = lock(&self.tenants);
        let mut out = Vec::with_capacity(tenants.len());
        for (tenant, state) in tenants.iter_mut() {
            if state.usage.is_empty() {
                continue;
            }
            state.series.rotate(now_ns);
            state.streams.rotate(now_ns);

            // Tenant-wide flow: bytes admitted across every signal, and new
            // identities created across series and streams. Both buckets are
            // per-tenant (ADR-0051), so the deltas are tenant-wide, carried
            // identically in each of the tenant's per-signal snapshots.
            let bytes_total: u64 = state
                .usage
                .values()
                .map(|u| u.bytes_admitted_total)
                .fold(0u64, u64::saturating_add);
            let created_total = state
                .series
                .created_total
                .saturating_add(state.streams.created_total);
            let byte_delta = bytes_total.saturating_sub(state.snapshot_baseline_bytes);
            let creation_delta = created_total.saturating_sub(state.snapshot_baseline_created);
            // Baselines are NOT advanced here: they commit in
            // `apply_reconciliation`, only after this tenant's sibling read
            // succeeds. The running totals ride along so that commit can pin the
            // baseline to exactly the totals this delta was measured against
            // (traffic that arrives between here and the commit is attributed to
            // the next interval, not this one).

            let mut signals: Vec<Signal> = state.usage.keys().copied().collect();
            signals.sort_by_key(|s| s.key_prefix());

            out.push(TenantReconcileState {
                tenant: tenant.clone(),
                tenant_hash: tenant.hash(),
                configured_series: state.limits.max_active_series,
                configured_streams: state.limits.max_active_streams,
                configured_byte_rate: state.limits.ingest_byte_rate,
                configured_creation_rate: state.limits.series_creation_rate,
                active_series: state.series.active_count,
                active_streams: state.streams.active_count,
                byte_delta,
                creation_delta,
                snapshot_bytes_total: bytes_total,
                snapshot_created_total: created_total,
                signals,
            });
        }
        out
    }

    /// Install one tenant's freshly-computed `local_soft_threshold`s (ADR-0057
    /// section 2) and commit its flow baselines. A `None` cap field is left
    /// unchanged (its configured cap is unlimited, so there is no fleet cap to
    /// enforce). Called only after a successful reconciliation read; a failed
    /// read never reaches here, so the last-known thresholds stay in force
    /// (ADR-0057 section 3) and the flow baselines stay put so the skipped
    /// interval's delta is rolled into the next successful one rather than
    /// dropped (the commit half of [`Self::snapshot_for_reconciliation`]'s
    /// exactly-once contract).
    pub(crate) fn apply_reconciliation(&self, update: &TenantReconcileApply) {
        let mut tenants = lock(&self.tenants);
        let Some(state) = tenants.get_mut(&update.tenant) else {
            return;
        };
        // Commit the flow baselines to the totals the snapshot's deltas were
        // measured against, now that this tenant's cycle has succeeded.
        state.snapshot_baseline_bytes = update.baseline_bytes;
        state.snapshot_baseline_created = update.baseline_created;
        if let Some(cap) = update.series_cap {
            state.fleet_max_active_series = cap;
        }
        if let Some(cap) = update.streams_cap {
            state.fleet_max_active_streams = cap;
        }
        // Rate caps are reconciled in place on the existing buckets'
        // `rate_per_sec` (ADR-0057 section 2): the hot-path `try_take` reads
        // whatever rate is set, unchanged in cost. Tokens and capacity (burst)
        // are left untouched, so an in-flight burst allowance is preserved.
        if let (Some(per_sec), Some(bucket)) = (update.byte_rate_per_sec, state.byte_rate.as_mut())
        {
            bucket.rate_per_sec = per_sec;
        }
        if let (Some(per_sec), Some(bucket)) =
            (update.creation_rate_per_sec, state.creation_rate.as_mut())
        {
            bucket.rate_per_sec = per_sec;
        }
    }

    /// Record a reconciliation read failure for one (tenant, signal) (ADR-0057
    /// section 3): the LIST or a GET of the sibling snapshots failed, so the
    /// last-computed threshold stays in force and this counter increments so a
    /// sustained failure is observable.
    pub(crate) fn record_reconciliation_failure(&self, tenant: &TenantId, signal: Signal) {
        let now_ns = self.now_ns();
        self.with_tenant(tenant, now_ns, |state| {
            state
                .usage
                .entry(signal)
                .or_default()
                .reconciliation_failures_total += 1;
        });
    }

    /// The effective (fleet-reconciled) active-series cap now in force for a
    /// tenant, or `None` if the tenant has no state yet. Test-only observation
    /// hook for the reconciliation tests; the hot path reads this same field.
    #[cfg(test)]
    pub(crate) fn effective_max_active_series(&self, tenant: &TenantId) -> Option<CountLimit> {
        lock(&self.tenants)
            .get(tenant)
            .map(|state| state.fleet_max_active_series)
    }

    /// The effective (fleet-reconciled) ingest byte-rate `rate_per_sec` now in
    /// force for a tenant, or `None` if the tenant has no state or no bounded
    /// byte-rate bucket. Test-only observation hook for the rate-reconciliation
    /// tests; the hot-path `try_take` reads this same bucket field.
    #[cfg(test)]
    pub(crate) fn effective_byte_rate_per_sec(&self, tenant: &TenantId) -> Option<u64> {
        lock(&self.tenants)
            .get(tenant)
            .and_then(|state| state.byte_rate.as_ref())
            .map(|bucket| bucket.rate_per_sec)
    }
}

/// One tenant's usage captured for a reconciliation cycle (ADR-0057). Owned and
/// lock-free, so the reconciliation task can do its object-store I/O without
/// holding the controller's mutex.
pub(crate) struct TenantReconcileState {
    pub(crate) tenant: TenantId,
    pub(crate) tenant_hash: TenantHash,
    pub(crate) configured_series: CountLimit,
    pub(crate) configured_streams: CountLimit,
    pub(crate) configured_byte_rate: RateLimit,
    pub(crate) configured_creation_rate: RateLimit,
    pub(crate) active_series: u64,
    pub(crate) active_streams: u64,
    pub(crate) byte_delta: u64,
    pub(crate) creation_delta: u64,
    /// The running byte/creation totals `byte_delta`/`creation_delta` were
    /// measured against. Carried so [`AdmissionController::apply_reconciliation`]
    /// can commit the flow baselines to exactly these values once the cycle
    /// succeeds (ADR-0057; a failed read leaves the baselines untouched so this
    /// interval's delta is not dropped).
    pub(crate) snapshot_bytes_total: u64,
    pub(crate) snapshot_created_total: u64,
    pub(crate) signals: Vec<Signal>,
}

/// The thresholds one reconciliation cycle computed for one tenant, applied by
/// [`AdmissionController::apply_reconciliation`]. A `None` means "leave
/// unchanged" (configured unlimited, or nothing to reconcile).
pub(crate) struct TenantReconcileApply {
    pub(crate) tenant: TenantId,
    pub(crate) series_cap: Option<CountLimit>,
    pub(crate) streams_cap: Option<CountLimit>,
    pub(crate) byte_rate_per_sec: Option<u64>,
    pub(crate) creation_rate_per_sec: Option<u64>,
    /// The flow baselines to commit for this tenant now that its reconciliation
    /// cycle succeeded (ADR-0057). These are the running totals captured in
    /// [`AdmissionController::snapshot_for_reconciliation`], not the deltas.
    pub(crate) baseline_bytes: u64,
    pub(crate) baseline_created: u64,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // Admission bookkeeping is in-memory and per-process; a panic while
    // holding this lock (should one ever happen) must not turn into a
    // permanent quota-enforcement outage for every tenant, so a poisoned
    // lock is recovered rather than propagated.
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn count_new<T: Eq + Hash + Copy>(set: &EpochIdSet<T>, candidates: &[T]) -> u64 {
    let mut seen = HashSet::new();
    let mut new_count: u64 = 0;
    for id in candidates {
        if seen.insert(*id) && !set.is_active(id) {
            new_count += 1;
        }
    }
    new_count
}

fn charge_creation_rate(
    state: &mut TenantState,
    signal: Signal,
    new_count: u64,
    now_ns: i64,
) -> Result<(), RequestRejection> {
    let result = match &mut state.creation_rate {
        None => Ok(()),
        Some(bucket) => bucket
            .try_take(new_count, now_ns)
            .map_err(|retry_after_ns| RequestRejection {
                reason: RequestRejectionReason::SeriesCreationRate,
                retry_after_ns,
            }),
    };
    let usage = state.usage.entry(signal).or_default();
    if result.is_err() {
        usage.requests_rejected_series_rate_total += 1;
    }
    result
}

fn admit_batch<T: Eq + Hash + Copy>(
    set: &mut EpochIdSet<T>,
    candidates: impl IntoIterator<Item = T>,
    cap: CountLimit,
    usage: &mut SignalUsage,
) -> IdentityAdmission<T> {
    let mut out = IdentityAdmission::default();
    for id in candidates {
        if set.admit(id, cap) {
            usage.series_admitted_total += 1;
            out.admitted.push(id);
        } else {
            usage.series_rejected_cap_total += 1;
            out.rejected.push(id);
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    struct TestClock(AtomicI64);

    impl TestClock {
        fn new(start_ns: i64) -> Arc<Self> {
            Arc::new(TestClock(AtomicI64::new(start_ns)))
        }

        fn advance(&self, delta_ns: i64) {
            self.0.fetch_add(delta_ns, Ordering::SeqCst);
        }

        fn now(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn series(byte: u8) -> SeriesId {
        SeriesId([byte; 16])
    }

    fn stream(byte: u8) -> LogStreamId {
        LogStreamId([byte; 16])
    }

    fn tight_limits(max_active_series: u64) -> AdmissionLimits {
        AdmissionLimits {
            max_active_series: CountLimit::Bounded(max_active_series),
            max_active_streams: CountLimit::Bounded(max_active_series),
            ingest_byte_rate: RateLimit::Unlimited,
            series_creation_rate: RateLimit::Unlimited,
        }
    }

    #[test]
    fn active_series_cap_rejects_new_series_only() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let tenant = TenantId::new("acme");
        controller.set_tenant_limits(tenant.clone(), tight_limits(2));

        let first = controller.admit_series(&tenant, [series(1), series(2)], clock.now());
        assert_eq!(first.admitted, vec![series(1), series(2)]);
        assert!(first.rejected.is_empty());

        // series(1) is already active and keeps flowing; series(3) is new
        // and the cap (2) has no room, so only it is rejected.
        let second = controller.admit_series(&tenant, [series(1), series(3)], clock.now());
        assert_eq!(second.admitted, vec![series(1)]);
        assert_eq!(second.rejected, vec![series(3)]);
    }

    #[test]
    fn byte_rate_exhaustion_rejects_whole_request() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let tenant = TenantId::new("acme");
        controller.set_tenant_limits(
            tenant.clone(),
            AdmissionLimits {
                ingest_byte_rate: RateLimit::Bounded {
                    per_sec: 1_000,
                    burst: 1_000,
                },
                ..tight_limits(u64::MAX)
            },
        );

        assert!(
            controller
                .check_byte_rate(&tenant, Signal::Metrics, 1_000, clock.now())
                .is_ok()
        );
        let rejection = controller
            .check_byte_rate(&tenant, Signal::Metrics, 1, clock.now())
            .expect_err("bucket is empty, one more byte must be rejected");
        assert_eq!(rejection.reason, RequestRejectionReason::ByteRate);
        assert!(rejection.retry_after_ns > 0);
    }

    #[test]
    fn creation_rate_exhaustion_rejects_while_existing_series_still_flow() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let tenant = TenantId::new("acme");
        controller.set_tenant_limits(
            tenant.clone(),
            AdmissionLimits {
                series_creation_rate: RateLimit::Bounded {
                    per_sec: 1,
                    burst: 1,
                },
                ..tight_limits(u64::MAX)
            },
        );

        // Spend the single creation token on series(1).
        assert!(
            controller
                .check_series_creation_rate(&tenant, &[series(1)], clock.now())
                .is_ok()
        );
        let admitted = controller.admit_series(&tenant, [series(1)], clock.now());
        assert_eq!(admitted.admitted, vec![series(1)]);

        // A new series exhausts the creation-rate bucket: whole-request reject.
        let rejection = controller
            .check_series_creation_rate(&tenant, &[series(2)], clock.now())
            .expect_err("creation-rate bucket is empty");
        assert_eq!(rejection.reason, RequestRejectionReason::SeriesCreationRate);

        // series(1) is already active, so a batch containing only it costs
        // zero new-series tokens and keeps flowing even with an empty bucket.
        assert!(
            controller
                .check_series_creation_rate(&tenant, &[series(1)], clock.now())
                .is_ok()
        );
    }

    #[test]
    fn token_buckets_refill_deterministically_under_injected_clock() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let tenant = TenantId::new("acme");
        controller.set_tenant_limits(
            tenant.clone(),
            AdmissionLimits {
                ingest_byte_rate: RateLimit::Bounded {
                    per_sec: 100,
                    burst: 100,
                },
                ..tight_limits(u64::MAX)
            },
        );

        assert!(
            controller
                .check_byte_rate(&tenant, Signal::Metrics, 100, clock.now())
                .is_ok()
        );
        assert!(
            controller
                .check_byte_rate(&tenant, Signal::Metrics, 1, clock.now())
                .is_err()
        );

        // Half a second at 100/s refills exactly 50 tokens.
        clock.advance(500_000_000);
        assert!(
            controller
                .check_byte_rate(&tenant, Signal::Metrics, 50, clock.now())
                .is_ok()
        );
        assert!(
            controller
                .check_byte_rate(&tenant, Signal::Metrics, 1, clock.now())
                .is_err()
        );

        // A long idle period never refills past the burst capacity.
        clock.advance(1_000_000_000_000);
        assert!(
            controller
                .check_byte_rate(&tenant, Signal::Metrics, 100, clock.now())
                .is_ok()
        );
        assert!(
            controller
                .check_byte_rate(&tenant, Signal::Metrics, 1, clock.now())
                .is_err()
        );
    }

    #[test]
    fn token_bucket_does_not_double_credit_on_backward_clock() {
        // Rate 100/s, burst 100, starting drained at real t=2s.
        let mut bucket = TokenBucket::new(100, 100, 2_000_000_000);

        // try_take(100, now=2s): refills 100, takes them, anchor = 2s.
        assert!(bucket.try_take(100, 2_000_000_000).is_ok());

        // try_take(100, now=1s): credits nothing (backward clock), and must
        // not regress the anchor to 1s either.
        assert!(bucket.try_take(100, 1_000_000_000).is_err());

        // try_take(100, now=2s) again: an honest monotonic clock has
        // elapsed zero time since the drain, so this must still fail. Were
        // the anchor allowed to regress to 1s, this would see 1s of
        // elapsed time and wrongly credit (then take) 100 more tokens,
        // yielding 200 total at real t=2s instead of 100.
        assert!(bucket.try_take(100, 2_000_000_000).is_err());
    }

    #[test]
    fn per_tenant_isolation_one_tenants_exhaustion_does_not_affect_another() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let noisy = TenantId::new("noisy");
        let quiet = TenantId::new("quiet");
        let limits = AdmissionLimits {
            ingest_byte_rate: RateLimit::Bounded {
                per_sec: 10,
                burst: 10,
            },
            ..tight_limits(1)
        };
        controller.set_tenant_limits(noisy.clone(), limits);
        controller.set_tenant_limits(quiet.clone(), limits);

        // Exhaust the noisy tenant's byte-rate bucket and active-series cap.
        assert!(
            controller
                .check_byte_rate(&noisy, Signal::Metrics, 10, clock.now())
                .is_ok()
        );
        assert!(
            controller
                .check_byte_rate(&noisy, Signal::Metrics, 1, clock.now())
                .is_err()
        );
        let noisy_admit = controller.admit_series(&noisy, [series(1), series(2)], clock.now());
        assert_eq!(noisy_admit.admitted, vec![series(1)]);
        assert_eq!(noisy_admit.rejected, vec![series(2)]);

        // The quiet tenant is unaffected: its own bucket and cap are fresh.
        assert!(
            controller
                .check_byte_rate(&quiet, Signal::Metrics, 10, clock.now())
                .is_ok()
        );
        let quiet_admit = controller.admit_series(&quiet, [series(1)], clock.now());
        assert_eq!(quiet_admit.admitted, vec![series(1)]);
        assert!(quiet_admit.rejected.is_empty());
    }

    #[test]
    fn active_series_survive_one_epoch_rotation_then_expire_after_two() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let tenant = TenantId::new("acme");
        controller.set_tenant_limits(tenant.clone(), tight_limits(1));

        let first = controller.admit_series(&tenant, [series(1)], clock.now());
        assert_eq!(first.admitted, vec![series(1)]);

        // One epoch later, series(1) is still active (current epoch's
        // window looks at current + previous), so a fresh series is still
        // rejected: the cap has no room.
        clock.advance(ACTIVE_EPOCH_NS);
        let still_active = controller.admit_series(&tenant, [series(2)], clock.now());
        assert!(still_active.admitted.is_empty());
        assert_eq!(still_active.rejected, vec![series(2)]);

        // Two more epochs with no traffic for series(1) age it out entirely,
        // freeing the cap for a new series.
        clock.advance(ACTIVE_EPOCH_NS * 2);
        let after_expiry = controller.admit_series(&tenant, [series(2)], clock.now());
        assert_eq!(after_expiry.admitted, vec![series(2)]);
    }

    #[test]
    fn admit_streams_mirrors_series_cap_behavior_for_logs() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let tenant = TenantId::new("acme");
        controller.set_tenant_limits(tenant.clone(), tight_limits(1));

        let first = controller.admit_streams(&tenant, [stream(1)], clock.now());
        assert_eq!(first.admitted, vec![stream(1)]);

        let second = controller.admit_streams(&tenant, [stream(1), stream(2)], clock.now());
        assert_eq!(second.admitted, vec![stream(1)]);
        assert_eq!(second.rejected, vec![stream(2)]);
    }

    #[test]
    fn usage_snapshot_is_bounded_and_reflects_activity() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let tenant = TenantId::new("acme");
        controller.set_tenant_limits(tenant.clone(), tight_limits(10));

        controller.admit_series(&tenant, [series(1), series(2)], clock.now());
        let _ = controller.check_byte_rate(&tenant, Signal::Metrics, 128, clock.now());

        let snapshot = controller.usage_snapshot();
        let metrics_row = snapshot
            .iter()
            .find(|row| row.tenant_hash == tenant.hash() && row.signal == Signal::Metrics)
            .expect("a metrics usage row must exist after metrics activity");
        assert_eq!(metrics_row.active_series, 2);
        assert_eq!(metrics_row.series_admitted_total, 2);
        assert_eq!(metrics_row.bytes_admitted_total, 128);

        // Never keyed by anything other than the fixed-width tenant hash.
        assert_eq!(metrics_row.tenant_hash, tenant.hash());
    }

    #[test]
    fn record_clock_rejection_counts_per_tenant_and_signal() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let tenant = TenantId::new("acme");

        controller.record_clock_rejection(&tenant, Signal::Metrics);
        controller.record_clock_rejection(&tenant, Signal::Metrics);
        controller.record_clock_rejection(&tenant, Signal::Logs);

        let snapshot = controller.usage_snapshot();
        let metrics_row = snapshot
            .iter()
            .find(|row| row.tenant_hash == tenant.hash() && row.signal == Signal::Metrics)
            .expect("a metrics usage row exists after a clock rejection");
        assert_eq!(metrics_row.requests_rejected_clock_total, 2);
        let logs_row = snapshot
            .iter()
            .find(|row| row.tenant_hash == tenant.hash() && row.signal == Signal::Logs)
            .expect("a logs usage row exists after a clock rejection");
        assert_eq!(logs_row.requests_rejected_clock_total, 1);
    }

    /// ADR-0084 decision 4: `TokenBucket::peek` and an immediately following
    /// `try_take` at the SAME `now_ns` must agree on whether `amount` is
    /// available. This is the correctness contract the gzip pre-check relies on:
    /// a peek that says "would fit" must be followed by a take that does fit
    /// (and vice versa), or the gzip path would 429 a request the charge would
    /// have admitted, or admit one the charge then rejects.
    ///
    /// Non-vacuity: change `peek`'s `self.refill(now_ns)` to a no-op (drop the
    /// line) and the "drained then refilled, amount exactly the refilled
    /// balance" case fails: peek sees 0 tokens and reports unavailable while the
    /// refilling `try_take` accepts. Change `peek`'s `self.tokens >= amount` to
    /// `>` and the same exact-balance case fails the other way.
    #[test]
    fn peek_and_take_agree_at_same_instant() {
        // Peek then take on ONE bucket at the same instant, exactly as the
        // gzip path calls peek_byte_rate then check_byte_rate. peek takes no
        // tokens, so the following take at the same `now_ns` sees the same
        // balance; their Ok/Err verdict must match.
        fn assert_agree(mut bucket: TokenBucket, amount: u64, now_ns: i64) {
            let peeked = bucket.peek(amount, now_ns).is_ok();
            let taken = bucket.try_take(amount, now_ns).is_ok();
            assert_eq!(
                peeked, taken,
                "peek/try_take disagree for amount={amount} at now={now_ns}"
            );
        }

        // Fresh bucket: burst 100, rate 100/s, full at t=0.
        let fresh = TokenBucket::new(100, 100, 0);
        assert_agree(fresh, 0, 0); // trivially available
        assert_agree(fresh, 100, 0); // exactly the full burst: available
        assert_agree(fresh, 101, 0); // one past burst: unavailable

        // Drained, then refilled by exactly 50 tokens after 0.5s at 100/s. The
        // exact-balance amount (50) is the discriminating case: it catches both
        // a peek that fails to refill and a peek using `>` instead of `>=`.
        let mut drained = TokenBucket::new(100, 100, 0);
        drained.try_take(100, 0).expect("drain the fresh burst");
        assert_agree(drained, 50, 500_000_000); // exactly the refilled balance
        assert_agree(drained, 51, 500_000_000); // one past it: unavailable
        assert_agree(drained, 0, 500_000_000);
    }

    /// ADR-0084 decision 4: a `peek` that fails reports the same `retry_after`
    /// wait a `try_take` would, so the gzip pre-check's 429 `Retry-After` is the
    /// value the tenant would really wait. Neither consumes on failure, so the
    /// take at the same instant sees the identical deficit.
    #[test]
    fn peek_failure_reports_same_retry_after_as_try_take() {
        let mut bucket = TokenBucket::new(100, 100, 0);
        bucket.try_take(100, 0).expect("drain the burst");
        // 100 short at 100/s is a 1s wait; assert peek and take agree on it
        // exactly rather than hard-coding the arithmetic.
        let peek_err = bucket.peek(100, 0).expect_err("empty bucket, peek fails");
        let take_err = bucket
            .try_take(100, 0)
            .expect_err("empty bucket, take fails");
        assert_eq!(
            peek_err, take_err,
            "peek and try_take must report the same retry_after"
        );
        assert!(peek_err > 0);
    }

    /// ADR-0084 decision 4: the `rate_per_sec == 0` branch behaves identically
    /// in `peek` and `try_take`. A zero-rate bucket never refills, so an amount
    /// over the current balance is unsatisfiable and both report `i64::MAX`.
    #[test]
    fn peek_and_take_agree_when_rate_is_zero() {
        // rate 0, burst 10, full at t=0.
        let mut within = TokenBucket::new(0, 10, 0);
        assert!(within.peek(10, 0).is_ok());
        assert!(within.try_take(10, 0).is_ok(), "within burst is available");

        let mut over = TokenBucket::new(0, 10, 1_000_000_000);
        // Elapsed time credits nothing at rate 0, so 20 stays unsatisfiable.
        let peek_err = over
            .peek(20, 2_000_000_000)
            .expect_err("rate 0, over burst");
        let take_err = over
            .try_take(20, 2_000_000_000)
            .expect_err("rate 0, over burst");
        assert_eq!(peek_err, i64::MAX, "rate 0 reports an unbounded wait");
        assert_eq!(
            peek_err, take_err,
            "the rate==0 branch must match in peek and try_take"
        );
    }

    /// ADR-0084 decision 4: a request that PASSES the compressed-size pre-check
    /// (`peek_byte_rate`) and then FAILS the real charge (`check_byte_rate` on
    /// the larger decompressed size) is counted as exactly one byte-rate
    /// rejection, not two. The pre-check consumes nothing and, on success,
    /// records nothing; only the failing charge increments the counter.
    ///
    /// Non-vacuity: make `peek_byte_rate`'s success arm increment
    /// `requests_rejected_byte_rate_total` (or count on the pass path at all)
    /// and this reads 2.
    #[test]
    fn peek_pass_then_charge_fail_counts_one_rejection() {
        let clock = TestClock::new(0);
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let tenant = TenantId::new("acme");
        controller.set_tenant_limits(
            tenant.clone(),
            AdmissionLimits {
                ingest_byte_rate: RateLimit::Bounded {
                    per_sec: 1,
                    burst: 100,
                },
                ..tight_limits(u64::MAX)
            },
        );

        // Pre-check on a small compressed size (50 <= 100): passes, no charge,
        // no counter.
        controller
            .peek_byte_rate(&tenant, Signal::Metrics, 50, clock.now())
            .expect("the compressed-size pre-check passes");
        // Real charge on the larger decompressed size (200 > 100): rejected.
        let rejection = controller
            .check_byte_rate(&tenant, Signal::Metrics, 200, clock.now())
            .expect_err("the decompressed charge exceeds the burst");
        assert_eq!(rejection.reason, RequestRejectionReason::ByteRate);

        let snapshot = controller.usage_snapshot();
        let row = snapshot
            .iter()
            .find(|row| row.tenant_hash == tenant.hash() && row.signal == Signal::Metrics)
            .expect("a metrics usage row after the rejection");
        assert_eq!(
            row.requests_rejected_byte_rate_total, 1,
            "a peek-pass then charge-fail is exactly one rejection, not two"
        );
        assert_eq!(
            row.bytes_admitted_total, 0,
            "nothing is admitted when the charge is rejected"
        );
    }
}
