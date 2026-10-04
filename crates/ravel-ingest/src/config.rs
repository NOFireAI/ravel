//! Sizing knobs for the ingest pipeline (docs/ingest.md "Sizing defaults").

use std::time::Duration;

use crate::budget::IngestByteBudgetLimit;
use crate::metrics::FlushTrigger;

/// RSEG trailer version every flush emits. ADR-0027 leaves v7 the only
/// writable version (ADR-0092 bumped it from v6), so this is no longer a
/// configurable knob; it mirrors `ravel_segment`'s `VERSION_V7` constant, and
/// is stamped verbatim into the commit record's `segment_format_version`.
/// Changing it is a format-level ADR, not a routine edit.
pub const SEGMENT_FORMAT_VERSION: u16 = ravel_segment::VERSION_V7;

/// RLOG trailer version every log flush emits. Tied to `ravel_logseg`'s own
/// object trailer version (`docs/log-segment-format.md`, ADR-0029) at compile
/// time rather than hand-mirrored; like [`SEGMENT_FORMAT_VERSION`] it is not a
/// configurable knob, and is stamped verbatim into the commit record's
/// `segment_format_version`. Changing it is a format-level ADR, not a routine
/// edit.
///
/// It was a mirrored literal (`2`), which is the same defect the RSPAN v2 bump
/// shipped: the writer stamps the object trailer from the format crate while a
/// stale literal keeps claiming the old number, so every published object gets
/// a commit record naming a version it is not, permanently (commit records are
/// immutable, and `ravel-cli maintain audit-versions` reads them). The v2 -> v3
/// bump (ADR-0095) is what surfaced it.
pub const LOG_SEGMENT_FORMAT_VERSION: u16 = ravel_logseg::footer::VERSION;

/// RSPAN trailer version every span flush emits. Stamped verbatim into the
/// commit record's `segment_format_version`. Changing it is a format-level
/// ADR, not a routine edit.
///
/// Tied to `ravel_rspan`'s own trailer version at compile time rather than
/// hand-mirrored. It was a mirrored literal, and the RSPAN v2 bump left it
/// at 1: every v2 span object was published under a commit record claiming
/// version 1. Commit records are immutable, so each such record carries the
/// wrong version forever, and `ravel-cli maintain audit-versions` derives
/// `supported` from the real trailer version, so it flagged every live span
/// object as an unsupported-version anomaly. The tool whose only purpose is
/// catching this drift class was defeated by the drift it failed to prevent.
pub const SPAN_SEGMENT_FORMAT_VERSION: u16 = ravel_rspan::footer::VERSION;

/// Nanoseconds per hour, the unit `ingest_hour_bucket` counts in.
pub(crate) const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Reserve `strict_visibility_budget_ns` must exceed `max_flush_delay` by
/// (ADR-0076 decision 4): two PUT round trips (data object, then commit
/// record) plus one retry's base backoff, with margin. `visibility_ceiling_ns`
/// subtracts those same costs from the budget to get the adaptive corridor's
/// cap; setting the budget equal to `max_flush_delay` (as the pre-fix default
/// and the `ravel-server` call site both did) leaves nothing for that
/// subtraction to work with, so the ceiling collapses to the floor
/// unconditionally and `FlushTrigger::AgeAdaptive` becomes unreachable. This
/// preserves the same absolute corridor width the original hard-coded values
/// had (1s budget over a 500ms floor = 500ms of headroom before RTT/retry
/// subtraction).
pub const STRICT_VISIBILITY_RESERVE_NS: i64 = 500_000_000;

/// Receiver-clock plausibility floor: 2020-01-01T00:00:00Z in nanoseconds
/// (ADR-0051 amendment). No host legitimately
/// runs Ravel with a clock reading before the system existed, so a reading
/// below this floor is the replica's own fault, not the request's data. It is
/// the one floor derivable without a second reference clock: a wrong-but-post-
/// 2020 clock still cannot be detected against anything, and what the window
/// buys there is loud, attributable failure instead of silent pollution of the
/// hour-partitioned layout and the retention arithmetic anchored on it.
///
/// Enforced at both points the receiver clock is read: at admission (see
/// [`plausible_ingest_clock`], whole-request 503 / gRPC `UNAVAILABLE`) and at
/// flush open (see [`checked_ingest_hour_bucket`]).
pub const MIN_PLAUSIBLE_INGEST_CLOCK_NS: i64 = 1_577_836_800_000_000_000;

/// Checks a receiver admission-clock reading for plausibility before it is
/// used to build a normalize context (ADR-0051 amendment).
///
/// The reading must sit at or above [`MIN_PLAUSIBLE_INGEST_CLOCK_NS`] and yield
/// a representable `u32` ingest-hour bucket. A failure is the replica's fault,
/// not the request's, so the caller rejects the whole request with HTTP 503 /
/// gRPC `UNAVAILABLE`: no per-record decision is meaningful when the reference
/// clock itself is nonsense, and a retry against a healthy replica succeeds.
/// Never clamps a reading into range and never maps it to a fallback bucket.
pub fn plausible_ingest_clock(now_ns: i64) -> Result<(), String> {
    if now_ns < MIN_PLAUSIBLE_INGEST_CLOCK_NS {
        return Err(format!(
            "receiver clock reading is below the plausibility floor: now_ns={now_ns}, \
             floor={MIN_PLAUSIBLE_INGEST_CLOCK_NS} (2020-01-01T00:00:00Z)"
        ));
    }
    u32::try_from(now_ns.div_euclid(NS_PER_HOUR))
        .map(|_| ())
        .map_err(|_| {
            format!(
                "receiver clock reading yields a non-representable hour bucket: now_ns={now_ns}"
            )
        })
}

/// Derives the commit record's `ingest_hour_bucket` from a flush-open clock
/// reading, fail-loud (ADR-0051 section 7 and its amendment). A non-positive reading (a clock that reports zero or has gone
/// backwards past the epoch), or a positive-but-implausible one below
/// [`MIN_PLAUSIBLE_INGEST_CLOCK_NS`] (a host whose RTC reset to shortly after
/// the epoch), can never be a valid hour bucket. Silently mapping it to bucket
/// 0 (the previous `unwrap_or(0)` behavior) wrote data into an undiscoverable
/// bucket instead of surfacing the bad reading to the caller. Every one of the
/// three shard actors calls this at the same point `flush_open_ns` is read, so
/// the check runs once per flush, not per retry.
pub(crate) fn checked_ingest_hour_bucket(flush_open_ns: i64) -> Result<u32, String> {
    if flush_open_ns <= 0 {
        return Err(format!(
            "flush clock produced a non-positive reading: flush_open_ns={flush_open_ns}"
        ));
    }
    if flush_open_ns < MIN_PLAUSIBLE_INGEST_CLOCK_NS {
        return Err(format!(
            "flush clock produced a reading below the plausibility floor: \
             flush_open_ns={flush_open_ns}, floor={MIN_PLAUSIBLE_INGEST_CLOCK_NS}"
        ));
    }
    u32::try_from(flush_open_ns.div_euclid(NS_PER_HOUR)).map_err(|_| {
        format!(
            "flush clock produced a non-representable hour bucket: flush_open_ns={flush_open_ns}"
        )
    })
}

/// Upper bound on how far the per-writer monotonic floor (ADR-1307) may hold a
/// flush-open stamp above the raw clock reading before the flush is refused
/// rather than stamped.
///
/// A backwards clock step within this bound is absorbed: the stamp is held at
/// the floor so duplicate resolution stays monotonic, the step is counted
/// (`clock_regressions`), and the flush proceeds. A hold larger than this is
/// refused with a typed, retryable error (counted as `clock_regressions_refused`)
/// and the floor re-anchors to the raw reading, because a hold this large can
/// only arise two ways, both of which must fail loud rather than be papered over:
///
/// - a genuine multi-minute backwards step, which the floor cannot absorb
///   without drifting the stamp arbitrarily far from wall time and into a
///   stale ingest-hour bucket; and
/// - the tail of a spurious forward glitch that already ratcheted the floor
///   ahead of wall time. Absorbing here would stamp every later flush into a
///   future ingest hour that LIST-discovered resolve never scans, so one glitch
///   would silently strand all subsequent writes. Re-anchoring on refusal means
///   exactly the one flush that crosses the bound fails; the next normal
///   reading proceeds.
///
/// Sized as the catalog clock-skew allowance alone, derived from
/// [`ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`] rather than a literal.
/// That allowance is the only term that governs the *future* end of the resolve
/// window: `Catalog::window_hour_bounds` caps a token-less query's hour listing
/// at `now + clock_skew_allowance`, so a stamp held at most this far above wall
/// time still lands in an hour bucket that query lists. The fold safety margin
/// does not belong in this bound: it feeds the seal watermark, which governs how
/// far *back* the unsealed tail is scanned, not how far forward a query lists. A
/// stamp held past `now + clock_skew_allowance` sits in a future hour bucket a
/// token-less query skips (a hole in the result, not staleness), which is why
/// the bound is the clock-skew allowance and nothing more.
///
/// Known limitation: `clock_skew_allowance_ns` is operator-configurable per
/// catalog (`ravel_catalog::CatalogConfig`). This bound is fixed at compile time
/// from the *default* allowance, so an operator who lowers the catalog's
/// allowance below it widens the window in which an absorbed stamp is
/// undiscoverable. A runtime cross-check against the configured allowance is a
/// follow-up (reported, not fixed here).
pub const MAX_FLUSH_CLOCK_HOLD_NS: i64 = ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;

/// Outcome of the flush-open check of the raw clock reading against the
/// object store's observed clock (ADR-1685 decision 2).
pub(crate) enum StoreClockLag {
    /// The store's observed time is at most
    /// [`ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`] ahead of the reading.
    WithinAllowance,
    /// The store has not been observed yet, so nothing was checked (decision 4).
    Unobserved,
    /// The reading lags the store's observed time by more than the allowance.
    /// `lag_ns` is that lag, so a caller that bypasses the refusal can still
    /// name the figure it bypassed.
    Refused { lag_ns: i64, msg: String },
}

/// Whether the ADR-1685 store-clock lag check may refuse this flush attempt.
///
/// A lag refusal is not self-clearing the way an over-bound regression is: it
/// re-anchors nothing, so every pass of a drain reads the same lag and refuses
/// again, and on a teardown drain there is no later tick to retry the buffered
/// rows; see [`MAX_FLUSH_ALL_PASSES`] for what that costs and why the drain
/// bypasses.
///
/// The choice is per pass, not per drain: a teardown drain runs its bounded
/// enforced passes first and only then bypasses, so the counters still show
/// every refusal the clock earned.
#[derive(Clone, Copy)]
pub(crate) enum LagCheck {
    /// The normal path: a reading lagging the store's observed clock beyond
    /// the allowance is refused, retryably.
    Enforced,
    /// A bypass pass of a teardown drain (`Shutdown` or the channel-close arm),
    /// made after the bounded enforced passes left the buffer still refused.
    /// The lag is still measured, counted as `clock_lag_bypassed_at_shutdown`,
    /// and logged, but the flush proceeds: publishing acknowledged rows into a
    /// possibly sealed hour (recoverable by a HEAD rebuild) beats dropping them
    /// on a graceful path. The ADR-1307 floor rules are unchanged, so a
    /// regression refusal still applies here; see [`MAX_FLUSH_ALL_PASSES`] for
    /// why that makes the bypass passes a bounded loop rather than one pass.
    BypassedAtTeardown,
}

/// Checks the raw flush-open reading `raw_ns` against `observed_store_ns`, a
/// lower bound on the store's clock. One-sided: a reading ahead of the
/// observation is normal, since the observation only ages between responses
/// (ADR-1685 decision 3). The caller passes the raw reading, never the
/// floor-raised stamp, because the floor can only hide lag.
pub(crate) fn store_clock_lag(raw_ns: i64, observed_store_ns: Option<i64>) -> StoreClockLag {
    let Some(observed_ns) = observed_store_ns else {
        return StoreClockLag::Unobserved;
    };
    let lag_ns = observed_ns.saturating_sub(raw_ns);
    if lag_ns > ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS {
        StoreClockLag::Refused {
            lag_ns,
            msg: format!(
                "flush clock lags the object store's observed clock by {lag_ns} ns, beyond the \
                 clock-skew allowance of {} ns; refusing the flush so it cannot publish into an \
                 ingest hour the fold may already have sealed (ADR-1685)",
                ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
            ),
        }
    } else {
        StoreClockLag::WithinAllowance
    }
}

/// Bound on the number of drain passes a graceful `flush_all` makes before it
/// gives up and records the residue (ADR-1307 finding F1).
///
/// `flush_all` re-buffers a clock-refused flush and must retry it in the same
/// call, because on a graceful teardown there is no later actor tick to retry
/// it (the map is snapshotted per pass, and a refusal re-inserts a key the
/// snapshot already consumed). A *regression* refusal (ADR-1307) re-anchors the
/// monotonic floor to the raw reading, so with any clock that does not keep
/// stepping backwards the very next pass stamps that reading and proceeds: a
/// normal drain finishes in one pass, and a single absorbed regression in two.
/// The bound exists only so a pathological clock that steps back on *every*
/// reading cannot spin the drain forever; `4` leaves generous headroom above
/// the two passes the ADR-guaranteed "at most one flush refused per backwards
/// step" needs while still terminating such a clock in a handful of iterations.
///
/// A *lag* refusal (ADR-1685) re-anchors nothing: it changes neither the floor
/// nor the store's observation, so every pass reads the same lag and refuses
/// again. This bound is what ends the enforced loop for it, and on a
/// [`DrainIntent::Teardown`] the drain then makes passes with
/// [`LagCheck::BypassedAtTeardown`], bounded by this same value, so those rows
/// publish rather than becoming residue. More than one bypass pass can be
/// needed: the enforced passes never consulted the floor, so an over-bound
/// backwards step hidden behind the lag check refuses the first bypass pass and
/// re-anchors the floor, and the pass after it publishes.
///
/// Residue that survives all passes is never dropped silently: how it is
/// reported depends on whether the caller can still retry it, which is what
/// [`DrainIntent`] carries.
pub const MAX_FLUSH_ALL_PASSES: usize = 4;

/// What the caller of a shard actor's `flush_all` does after the drain returns,
/// which decides how residue left by the pass cap is reported.
///
/// Residue is the same state either way: the tenants are still in the actor's
/// buffer map with their arrival bookkeeping intact. Whether that state is a
/// durability defect depends entirely on whether anything will flush them
/// later, and only the caller knows that.
pub(crate) enum DrainIntent {
    /// The actor stops when the drain returns (`Shutdown`, or the channel-close
    /// arm): nothing will retry residue, so acknowledged buffered-mode rows are
    /// lost on a graceful path. Logged at ERROR and counted
    /// (`flush_all_residue_tenants`).
    Teardown,
    /// The actor keeps running (`FlushNow`, reachable from the router's
    /// `flush_all`, which benches, tests, and the `ravel-cli` load path call on
    /// a repeating ticker): residue stays buffered with its
    /// `oldest_arrival_ns`, so the age tick and the next explicit flush both
    /// retry it and nothing is lost. Logged at WARN and not counted, so a stuck
    /// tenant under a bad clock cannot grow a durability-defect counter without
    /// bound, or page on a ticker. The refusals behind it are still counted
    /// per flush attempt (`clock_regressions_refused`).
    Retryable,
}

/// Why [`monotonic_flush_open_ns`] declined to produce a flush-open stamp.
/// `InvalidReading` is fail-loud and non-retryable; the other two arms are
/// transient and surface the same retryable error.
///
/// [`monotonic_flush_open_ns`]: crate::shard::ShardActor::monotonic_flush_open_ns
pub(crate) enum FlushClockError {
    /// The raw flush-open reading is not a usable wall-clock value: non-positive,
    /// below the 2020 plausibility floor, or yielding no representable
    /// ingest-hour bucket. A grossly broken host clock, not a transient step:
    /// the next flush reads the same broken clock until an operator fixes it, so
    /// this is surfaced fail-loud as the non-retryable `SegmentBuild` (ADR-0051
    /// amendment) and counted as `abandoned_input_rejected`.
    InvalidReading(String),
    /// The per-writer floor would have to hold the stamp more than
    /// [`MAX_FLUSH_CLOCK_HOLD_NS`] above the raw reading (ADR-1307): a backwards
    /// step too large to absorb, or the tail of a spurious forward glitch. A
    /// transient condition the next flush recovers from once the floor
    /// re-anchors, and nothing in this flush was acknowledged, so it is surfaced
    /// as the retryable `Abandoned` and counted as `clock_regressions_refused`,
    /// never as an `abandoned_input_rejected` client signal.
    RegressionRefused(String),
    /// The raw reading lags the object store's observed clock by more than the
    /// clock-skew allowance (ADR-1685): stamping it could publish into an
    /// ingest hour the fold has already sealed. Nothing in this flush was
    /// acknowledged and the flush succeeds once the host clock converges, so it
    /// is surfaced exactly as `RegressionRefused` is, as the retryable
    /// `Abandoned`, and counted as `clock_lag_refused`.
    ///
    /// Never produced under [`LagCheck::BypassedAtTeardown`], where a lagging
    /// reading goes on to the floor rules instead of stranding acknowledged
    /// rows on a graceful drain. The floor can still refuse it there, as
    /// [`RegressionRefused`](FlushClockError::RegressionRefused).
    LagRefused(String),
}

/// Share of the process-wide ADR-0069 ceiling that one (shard, tenant) buffer
/// may hold before the memory backstop fires: an eighth, so seven eighths of
/// the budget stay available to every other tenant while one buffer fills
/// toward `target_bytes`.
const BUFFER_MEMORY_BACKSTOP_BUDGET_DIVISOR: u64 = 8;

/// Cap on the per-buffer memory backstop, so an operator who raises
/// `--max-ingest-buffer-bytes` to tens of gigabytes does not thereby let one
/// buffer hold gigabytes of RAM. An eighth of the 512 MiB default ceiling, so
/// the default sizing is unchanged by the cap.
const BUFFER_MEMORY_BACKSTOP_CAP_BYTES: usize = 64 * 1024 * 1024;

/// The memory a single (shard, tenant) buffer may hold before the size trigger
/// fires regardless of how few object bytes it would write.
///
/// The size trigger is stated in object bytes, and the ratio between object
/// bytes and buffered memory is client-controlled: a series with many short
/// labels holds roughly twenty times more RAM than the bytes it contributes to
/// the object. Without this backstop such a tenant would fill RAM toward the
/// ADR-0069 shed ceiling instead of flushing, and shedding a write is worse
/// than writing a smaller object.
///
/// Derived from the ceiling the operator configured, not from the default one:
/// `--max-ingest-buffer-bytes` is what a shed is measured against, so a
/// constant backstop calibrated against the default inverts this rationale on
/// any replica sized below it. At `Bounded(64 MiB)` a constant 64 MiB backstop
/// lets one label-heavy buffer hold the entire process budget and shed every
/// other tenant's write until an age trigger releases it.
/// [`IngestByteBudgetLimit::Unlimited`] has no ceiling to take a share of, so
/// the cap applies alone: nothing sheds under it, and the cap is what keeps one
/// buffer's resident memory bounded.
///
/// Never below `target_bytes`: a backstop under the target would fire first on
/// every buffer and make the memory figure, not the object estimate, the
/// effective size trigger, which is the defect issue #1305 fixed. When an
/// eighth of the ceiling is itself below `target_bytes` (a ceiling under
/// `8 * target_bytes`, so under 64 MiB at the default target) `target_bytes`
/// wins and one buffer's share of the budget is larger than an eighth. That
/// configuration is already degenerate: a ceiling that holds only a few
/// target-sized objects sheds on tenant count whatever the backstop does.
/// Exported so `ravel-server` can assert its `--max-queued-flushes` help text
/// against the value each arm actually returns, rather than against a formula
/// restated in prose (PR #1903 review finding 4).
pub fn buffer_memory_backstop_bytes(
    config: &IngestConfig,
    ceiling: IngestByteBudgetLimit,
) -> usize {
    let share = match ceiling {
        IngestByteBudgetLimit::Unlimited => BUFFER_MEMORY_BACKSTOP_CAP_BYTES,
        IngestByteBudgetLimit::Bounded(limit) => {
            usize::try_from(limit / BUFFER_MEMORY_BACKSTOP_BUDGET_DIVISOR)
                .unwrap_or(usize::MAX)
                .min(BUFFER_MEMORY_BACKSTOP_CAP_BYTES)
        }
    };
    share.max(config.target_bytes)
}

/// Whether a buffer holding `est_bytes` of memory has crossed its
/// [`buffer_memory_backstop_bytes`], which is the backstop half of
/// [`size_trigger_fires`] on its own.
///
/// Read a second time, after the trigger, by each shard actor's
/// `queued_flush_cap_reached`: the queued-flush cap (issue #1740) bounds a queue
/// of flush TASKS, while this backstop is the only thing bounding the BUFFER
/// those tasks drain. Refusing a crossing here would trade a bounded queue for
/// an unbounded buffer, which under [`IngestByteBudgetLimit::Unlimited`] nothing
/// else sheds against, so the cap exempts it (PR #1903 review finding 1).
pub(crate) fn memory_backstop_crossed(
    est_bytes: usize,
    config: &IngestConfig,
    ceiling: IngestByteBudgetLimit,
) -> bool {
    est_bytes >= buffer_memory_backstop_bytes(config, ceiling)
}

/// The size trigger, shared by the metrics, log, and span shard actors:
/// `flush_est_bytes` is the object-bytes estimate for the flush this buffer
/// would write and gates `target_bytes`; `est_bytes` is the conservative
/// buffered-memory figure the ADR-0069 ceiling charges and gates only the
/// memory backstop, whose bound comes from `ceiling`. Keeping both here keeps
/// one rule in one place rather than three copies that drift (issue #1305).
///
/// The backstop can only pre-empt the target when a buffer holds more memory
/// than the object bytes it would write, which is the case it exists for. The
/// reverse happens too and the backstop is silent there: a native histogram
/// charges a flat 16 bytes per point to `est_bytes` while contributing up to
/// `32 + 8 * (buckets + spans + custom_values)` object bytes, and a log record
/// with no attributes charges 32 against 48 object bytes. Those buffers reach
/// `target_bytes` on the object estimate first, which is the intended trigger.
pub(crate) fn size_trigger_fires(
    flush_est_bytes: usize,
    est_bytes: usize,
    config: &IngestConfig,
    ceiling: IngestByteBudgetLimit,
) -> bool {
    flush_est_bytes >= config.target_bytes || memory_backstop_crossed(est_bytes, config, ceiling)
}

/// The age threshold, and the trigger to record, for a buffer with no
/// strict-mode waiter and under `min_flush_bytes` of object, shared by the
/// metrics, log, and span shard actors (ADR-1737 decision 2). Below a non-zero
/// `idle_flush_byte_floor` the buffer waits for the sub-floor hold, one
/// `flush_tick` short of `max_flush_lifetime`; otherwise it waits for
/// `max_flush_delay_idle`. `flush_est_bytes` is the same object-bytes estimate
/// the caller compared against `min_flush_bytes`, so a buffer moves up a tier
/// as rows arrive and a trickle that reaches the floor flushes on the idle
/// clock measured from its oldest row.
///
/// The tick is subtracted because the age check runs on a `flush_tick`, not at
/// the instant the threshold is crossed, so a buffer whose threshold is `T`
/// opens its flush at an age of up to `T + flush_tick`. Holding the tick back
/// keeps the worst buffer age at flush open at exactly `max_flush_lifetime`,
/// which is the figure ADR-0052's
/// `ravel_catalog::FLUSH_BOUND_SLACK_HOURS = 2` is derived from
/// (ADR-1737 decision 3 as amended). It saturates at zero, so a `flush_tick`
/// at or above `max_flush_lifetime` gives a hold of zero rather than wrapping.
pub(crate) fn idle_age_threshold(
    flush_est_bytes: usize,
    config: &IngestConfig,
) -> (i64, FlushTrigger) {
    if config.idle_flush_byte_floor > 0 && flush_est_bytes < config.idle_flush_byte_floor {
        let lifetime_ns = config.max_flush_lifetime.as_nanos() as i64;
        let tick_ns = config.flush_tick.as_nanos() as i64;
        (
            lifetime_ns.saturating_sub(tick_ns).max(0),
            FlushTrigger::AgeFloor,
        )
    } else {
        (
            config.max_flush_delay_idle.as_nanos() as i64,
            FlushTrigger::Age,
        )
    }
}

/// The zstd level a log flush compresses its RLOG object at (ADR-2135 decision
/// 4). Built only through [`RlogZstdLevel::new`], which refuses a level outside
/// zstd's accepted range, so an [`IngestConfig`] cannot carry one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RlogZstdLevel(i32);

impl RlogZstdLevel {
    /// The lowest level zstd accepts, libzstd's `ZSTD_minCLevel()`.
    pub const MIN: i32 = -131_072;
    /// The highest level zstd accepts, libzstd's `ZSTD_maxCLevel()`.
    pub const MAX: i32 = 22;
    /// The ingest default.
    pub const DEFAULT: RlogZstdLevel = RlogZstdLevel(3);

    /// The level, or [`RlogZstdLevelError::OutOfRange`] when it is outside
    /// `MIN..=MAX`.
    pub fn new(level: i32) -> Result<Self, RlogZstdLevelError> {
        if (Self::MIN..=Self::MAX).contains(&level) {
            Ok(RlogZstdLevel(level))
        } else {
            Err(RlogZstdLevelError::OutOfRange {
                level,
                min: Self::MIN,
                max: Self::MAX,
            })
        }
    }

    /// The level as zstd takes it.
    pub fn get(self) -> i32 {
        self.0
    }
}

impl Default for RlogZstdLevel {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// All fields are overridable; defaults match the dev-sizing table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestConfig {
    /// Number of shard actors. Immutable per (tenant, signal) in production
    /// (docs/catalog-and-mvcc.md); changing it after data has landed is a
    /// data-loss operation.
    pub shard_count: u32,
    /// Bounded mpsc channel depth per shard.
    pub channel_depth: usize,
    /// Flush a tenant's buffer once the object that flush would write is
    /// estimated to reach this many bytes.
    ///
    /// Estimated in object bytes, not in buffered memory (issue #1305): the
    /// size trigger reads the per-signal flush-size estimator, while the
    /// process-wide memory ceiling (ADR-0069) keeps charging its own
    /// conservative figure. The two are no longer one number, so raising this
    /// to get larger objects no longer weakens the memory ceiling.
    /// [`buffer_memory_backstop_bytes`] is what bounds the memory a single
    /// buffer may hold while filling toward this target.
    pub target_bytes: usize,
    /// Flush a tenant's buffer once its oldest point is at least this old.
    pub max_flush_delay: Duration,
    /// Interval on which each shard actor checks buffered ages against
    /// `max_flush_delay`.
    pub flush_tick: Duration,
    /// Age threshold applied instead of `max_flush_delay` when a buffer has
    /// no strict-mode waiter and holds fewer than `min_flush_bytes`
    /// (ADR-0051 section 7). Keeps the fast age trigger for
    /// buffers a caller is blocked on or that are already worth a PUT, while
    /// an idle, low-volume buffered-mode tenant waits this long instead of
    /// paying a PUT every `max_flush_delay` regardless of how little data it
    /// holds.
    pub max_flush_delay_idle: Duration,
    /// A buffer whose flush would write at least this many object bytes is
    /// never treated as idle for the age trigger, even with no strict-mode
    /// waiter: it is already worth the PUT cost `max_flush_delay` pays for.
    /// Same estimator and same units as `target_bytes` (issue #1305): "worth a
    /// PUT" is a statement about the object, not about the RAM the buffer
    /// occupies while building it.
    pub min_flush_bytes: usize,
    /// Opt-in third age tier (ADR-1737). When non-zero, a buffer with no
    /// strict-mode waiter whose flush would write fewer than this many object
    /// bytes waits for the sub-floor hold, `max_flush_lifetime` less one
    /// `flush_tick`, instead of `max_flush_delay_idle` before its age trigger
    /// fires, and that flush is counted as [`FlushTrigger::AgeFloor`]. Read by
    /// the metrics, log, and span actors against the same object-bytes
    /// estimate as `min_flush_bytes`. The tick the hold gives up is the one
    /// the age check may take to notice the threshold, so the buffer is at
    /// most `max_flush_lifetime` old when its flush opens.
    ///
    /// 0, the default, disables the tier, and every actor keeps the two-clock
    /// predicate. A non-zero value widens the buffered-mode loss window that
    /// docs/consistency-model.md states, for a buffer below the floor, from
    /// `max_flush_delay_idle` to `max_flush_lifetime`. It must be below
    /// `min_flush_bytes`;
    /// [`IngestConfig::validate`] refuses anything else.
    ///
    /// [`FlushTrigger::AgeFloor`]: crate::FlushTrigger::AgeFloor
    pub idle_flush_byte_floor: usize,
    /// Retries after the first attempt for the data-object PUT (total
    /// attempts = this + 1). Also bounds retries of the commit-record PUT.
    /// This matches `ravel_commit::publish::RetryPolicy::max_attempts`'s own
    /// "retries after the first attempt" convention, so both retry budgets in
    /// the flush path count the same way.
    pub put_retry_max_attempts: u32,
    pub put_retry_base_delay: Duration,
    pub put_retry_max_delay: Duration,
    /// A flush that cannot complete within this long after it opened is
    /// abandoned: never published, waiters errored (ADR-0010 §1/§11).
    pub max_flush_lifetime: Duration,
    /// Window width of the flush-scoped exemplar admission cap (ADR-0047
    /// decision 2): at most one exemplar per series per window reaches the
    /// EXEMPLARS section of one object. A security control, not a tuning
    /// knob: a trace id is high-entropy, so an uncapped path lets a client
    /// multiply object size at will.
    ///
    /// The shard builds a fresh [`ravel_types::ExemplarCap`] per flush rather
    /// than holding one for its lifetime: the cap's per-series map is
    /// unbounded, so a shard-lived cap grows with the shard's series
    /// cardinality forever. The wire-side cap (`ravel_otlp`'s, which does
    /// outlive a request) already carries the cross-request window; this one
    /// bounds what one object can hold.
    pub exemplar_cap_window_ns: i64,
    /// Upper bound on concurrently in-flight flush tasks per shard
    /// (ADR-0067 decision 2). The shard actor pins a flush's identity and
    /// moves its buffer into a spawned task while continuing to drain its
    /// channel; this semaphore is the only thing that can make a flush
    /// trigger block. Default 1 reproduces today's one-flush-at-a-time
    /// behavior bit for bit; raising it is a measured decision, not a routine tuning
    /// change. Must be at least 1: a
    /// value of 0 deadlocks every flush (`services/ravel-server`'s
    /// `Cli::validate` rejects it at the edge).
    pub max_inflight_flushes: u32,
    /// Upper bound on flush tasks one shard may have spawned and not yet
    /// reaped, counting both the flushes executing against the object store
    /// and those parked waiting for a `max_inflight_flushes` permit
    /// (ADR-1642 amendment, issue #1740). At the bound a size or age trigger
    /// is refused: the tenant's rows stay in its buffer with their arrival
    /// bookkeeping intact, so the next tick re-fires the same trigger once a
    /// flush has finished. Drain triggers ([`FlushTrigger::Manual`]: an
    /// explicit flush-all, shutdown, channel close) are never refused, since
    /// nothing would retry them.
    ///
    /// Neither is a trigger on a buffer that has crossed its memory backstop
    /// ([`memory_backstop_crossed`]), so the queue CAN exceed this bound.
    /// That backstop is the only bound on one buffer's resident memory, and
    /// under [`IngestByteBudgetLimit::Unlimited`] nothing sheds behind it, so
    /// refusing a crossing would trade a bounded queue of flush tasks for an
    /// unbounded buffer. No count bounds that overshoot: an exempt spawn
    /// consumes the whole buffer it fires on and the only re-insert path is
    /// the ordinary one, so the same tenant crosses again after buffering
    /// another backstop's worth and adds a window rather than replacing one.
    /// Counting the buffers currently over their backstop describes one
    /// instant, not the queue.
    ///
    /// So this is the count bound on the ORDINARY triggers, and it is the
    /// only bound they have under [`IngestByteBudgetLimit::Unlimited`], where
    /// `try_charge` never sheds: resident flush memory per shard is then this
    /// many flush windows, plus the tenant buffers themselves, plus however
    /// many exempt windows the stall has accumulated, which only its length
    /// bounds. Under [`IngestByteBudgetLimit::Bounded`] the budget bounds the
    /// exempt windows instead, since a queued flush stays charged until its
    /// PUTs complete and admission sheds once the charges reach the ceiling,
    /// which stops the refill that would spawn the next one.
    ///
    /// Read through [`IngestConfig::queued_flush_cap`], which floors it at 1;
    /// 0 would refuse every trigger and never flush.
    ///
    /// [`FlushTrigger::Manual`]: crate::FlushTrigger::Manual
    pub max_queued_flushes: usize,
    /// Enables the per-(shard, tenant) adaptive age trigger (ADR-0067
    /// decision 3): the fast age threshold moves within
    /// `[max_flush_delay, ceiling]` based on observed arrival rate and PUT
    /// RTT, instead of always using the fixed `max_flush_delay`. `false`
    /// (the default) keeps today's fixed-delay behavior for a clean A/B
    /// against the adaptive corridor in the ingest bench.
    pub adaptive_flush_delay: bool,
    /// Strict-mode visibility budget (ADR-0067 decision 3's corridor
    /// ceiling, ADR-0076 decision 4): `visibility_ceiling_ns` subtracts two
    /// PUT round trips plus retry headroom from this to get the adaptive
    /// corridor's cap. Metrics-only; the log and span actors do not read
    /// this field. Must follow whatever `max_flush_delay` is actually
    /// configured to, or the adaptive corridor contradicts the operator's
    /// chosen visibility budget (a 2s cadence next to a stale 1s corridor
    /// ceiling being exactly the contradiction the ADR warns against); the
    /// default therefore matches the new `max_flush_delay` default (2s)
    /// rather than retaining the old hard-coded 1s value.
    pub strict_visibility_budget_ns: i64,
    /// The zstd level of every page, the four compressed sections and the
    /// POSTINGS term blocks of each RLOG object a log flush writes. Metrics and
    /// span flushes do not read it.
    pub rlog_zstd_level: RlogZstdLevel,
}

impl Default for IngestConfig {
    fn default() -> Self {
        // ADR-0076 decision 4: the three flush-cadence knobs move as a
        // set, scaled 4x together (500ms/10s/64KiB -> 2s/40s/256KiB). At
        // the ~9.6 KB/s buffer fill rate the ADR measures, 256KiB is
        // reached in ~27s, comfortably under the new 40s idle ceiling,
        // so buffered-mode tenants keep the same "size trigger fires
        // before the idle timer" property they had before the bump.
        let max_flush_delay = Duration::from_secs(2);
        IngestConfig {
            shard_count: 4,
            channel_depth: 256,
            target_bytes: 8 * 1024 * 1024,
            max_flush_delay,
            flush_tick: Duration::from_millis(200),
            max_flush_delay_idle: Duration::from_secs(40),
            min_flush_bytes: 256 * 1024,
            idle_flush_byte_floor: 0,
            put_retry_max_attempts: 4,
            put_retry_base_delay: Duration::from_millis(100),
            put_retry_max_delay: Duration::from_secs(2),
            max_flush_lifetime: Duration::from_secs(3600),
            exemplar_cap_window_ns: ravel_types::ExemplarCap::DEFAULT_WINDOW_NS,
            max_inflight_flushes: 1,
            // Issue #1740: eight flush windows per shard is deep enough that a
            // healthy shard never reaches it (a flush that is not stalled is
            // reaped within one PUT round trip, and the default single permit
            // admits one at a time), and shallow enough that a stalled prefix
            // holds a bounded number of windows rather than an unbounded queue.
            // What a window holds is BUFFERED MEMORY, bounded per buffer by
            // `buffer_memory_backstop_bytes` (64 MiB at the default 512 MiB
            // ceiling, since a buffer that crosses it spawns exempt from this
            // cap), so the ordinary queue holds at most eight of those. It does
            // NOT bound the size of the objects those windows write:
            // `target_bytes` gates `flush_est_bytes`, a deferred trigger keeps
            // merging into the same buffer and spawns with whatever
            // `flush_est_bytes` has reached by then, and on the native-histogram
            // path a point charges a flat 16 bytes to `est_bytes` against up to
            // `32 + 8 * (buckets + spans + custom_values)` object bytes (see
            // `size_trigger_fires`), so such a buffer can stay under the
            // backstop while its object grows past the 8 MiB `target_bytes`
            // default.
            max_queued_flushes: 8,
            adaptive_flush_delay: false,
            // Must exceed max_flush_delay by STRICT_VISIBILITY_RESERVE_NS, not
            // equal it: equal leaves visibility_ceiling_ns's subtraction with
            // no headroom, collapsing the adaptive corridor to the floor
            // unconditionally.
            strict_visibility_budget_ns: max_flush_delay.as_nanos() as i64
                + STRICT_VISIBILITY_RESERVE_NS,
            rlog_zstd_level: RlogZstdLevel::DEFAULT,
        }
    }
}

/// A cross-field [`IngestConfig`] constraint that [`IngestConfig::validate`]
/// refuses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IngestConfigError {
    /// A non-zero `idle_flush_byte_floor` at or above `min_flush_bytes`
    /// (ADR-1737 decision 1). Such a floor has no idle tier left between it
    /// and the fast clock, so every buffer below `min_flush_bytes` would wait
    /// for the sub-floor hold.
    #[error(
        "idle_flush_byte_floor ({floor} bytes) must be below min_flush_bytes \
         ({min_flush_bytes} bytes), or 0 to disable it"
    )]
    IdleFlushByteFloorNotBelowMinFlushBytes {
        floor: usize,
        min_flush_bytes: usize,
    },
}

/// A level [`RlogZstdLevel::new`] refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RlogZstdLevelError {
    /// The level is outside the range zstd accepts.
    #[error("RLOG zstd level {level} is outside zstd's accepted range {min}..={max}")]
    OutOfRange { level: i32, min: i32, max: i32 },
}

impl IngestConfig {
    /// Checks the cross-field constraints a caller building an `IngestConfig`
    /// from operator input must refuse before constructing a router.
    pub fn validate(&self) -> Result<(), IngestConfigError> {
        if self.idle_flush_byte_floor != 0 && self.idle_flush_byte_floor >= self.min_flush_bytes {
            return Err(IngestConfigError::IdleFlushByteFloorNotBelowMinFlushBytes {
                floor: self.idle_flush_byte_floor,
                min_flush_bytes: self.min_flush_bytes,
            });
        }
        Ok(())
    }

    /// The per-shard queued-flush cap as the shard actors enforce it: at least
    /// 1, whatever [`IngestConfig::max_queued_flushes`] holds. This is where
    /// the "at least 1" rule is applied, rather than in a validator every
    /// construction site would have to remember to call: `IngestConfig` is a
    /// plain struct literal at a dozen call sites and its other bounds
    /// (`max_inflight_flushes`) are checked only at the `ravel-server` CLI
    /// edge, which leaves a library caller free to build a 0. A 0 cap refuses
    /// every size and age trigger, so a shard would buffer forever and flush
    /// only on a drain; flooring here makes that unreachable.
    pub(crate) fn queued_flush_cap(&self) -> usize {
        self.max_queued_flushes.max(1)
    }

    /// The largest age any buffer reaches before its flush trigger fires when
    /// no deferral intervenes, leaving out the sub-floor hold, plus one
    /// `flush_tick`: the largest of `max_flush_delay` (the fast clock),
    /// `max_flush_delay_idle` (the idle clock of a buffered-mode buffer with no
    /// strict waiter), and, with `adaptive_flush_delay` on, the metrics actor's
    /// adaptive corridor ceiling at its widest (`strict_visibility_budget_ns`
    /// less one `put_retry_base_delay`, the ceiling at a zero PUT round trip).
    /// The tick is added because the age check runs on a tick rather than at
    /// the instant the threshold is crossed. The sub-floor hold of ADR-1737 is
    /// left out on purpose: it already spends `max_flush_lifetime`, so counting
    /// it would drive [`Self::flush_deferral_cap_ns`] to 0 for any non-zero
    /// `idle_flush_byte_floor`, and a buffer held under that floor is not
    /// bounded by the cap.
    pub fn flush_trigger_age_bound_ns(&self) -> i64 {
        let fast_ns = self.max_flush_delay.as_nanos() as i64;
        let idle_ns = self.max_flush_delay_idle.as_nanos() as i64;
        let mut threshold_ns = fast_ns.max(idle_ns);
        if self.adaptive_flush_delay {
            let widest_ns = self
                .strict_visibility_budget_ns
                .saturating_sub(self.put_retry_base_delay.as_nanos() as i64);
            threshold_ns = threshold_ns.max(widest_ns);
        }
        threshold_ns.saturating_add(self.flush_tick.as_nanos() as i64)
    }

    /// How long a refused flush trigger may stay deferred before its shard
    /// refuses new appends (ADR-1642 deferral cap amendment). It is what the
    /// read-side slack `ravel_catalog::FLUSH_BOUND_SLACK_HOURS` has left for a
    /// row once the rest of its span is paid for: the slack less
    /// `max_flush_lifetime` (the term the frozen derivation reserves for the
    /// flush itself) less [`Self::flush_trigger_age_bound_ns`]. A row has
    /// waited at most that bound when its buffer's deferral starts, unless it
    /// is held under a non-zero `idle_flush_byte_floor`. A strict waiter is
    /// only acknowledged from a flush that opens inside the cap (a deferred
    /// buffer, and one whose generation-mismatch hand-back was retried for
    /// the cap, answer theirs `Abandoned` and are then written), so an
    /// acknowledged strict row's routing-to-pin span plus the flush lifetime
    /// stays inside the slack, and the shard starts refusing new writes while
    /// a flush opening then would still fit every buffered row outside the
    /// sub-floor hold inside it. Rows held under the floor, and rows of a
    /// strict write that already timed out, are not bounded by the cap.
    /// 3559.8 s at the shipped defaults, whatever `idle_flush_byte_floor`
    /// holds; it saturates at 0 for a configuration whose own bounds already
    /// spend the slack.
    pub fn flush_deferral_cap_ns(&self) -> i64 {
        let slack_ns = i64::from(ravel_catalog::FLUSH_BOUND_SLACK_HOURS) * NS_PER_HOUR;
        slack_ns
            .saturating_sub(self.max_flush_lifetime.as_nanos() as i64)
            .saturating_sub(self.flush_trigger_age_bound_ns())
            .max(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ADR-1685 lag check is one-sided and inclusive at the allowance, and
    /// an extreme observation saturates rather than overflowing into a pass.
    #[test]
    fn store_clock_lag_bounds() {
        let allowance = ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
        let raw = MIN_PLAUSIBLE_INGEST_CLOCK_NS + NS_PER_HOUR;
        assert!(matches!(
            store_clock_lag(raw, None),
            StoreClockLag::Unobserved
        ));
        assert!(matches!(
            store_clock_lag(raw, Some(raw + allowance)),
            StoreClockLag::WithinAllowance
        ));
        assert!(matches!(
            store_clock_lag(raw, Some(raw + allowance + 1)),
            StoreClockLag::Refused { lag_ns, .. } if lag_ns == allowance + 1
        ));
        assert!(matches!(
            store_clock_lag(raw, Some(raw - 10 * NS_PER_HOUR)),
            StoreClockLag::WithinAllowance
        ));
        assert!(matches!(
            store_clock_lag(raw, Some(i64::MAX)),
            StoreClockLag::Refused { .. }
        ));
        assert!(matches!(
            store_clock_lag(raw, Some(i64::MIN)),
            StoreClockLag::WithinAllowance
        ));
    }

    /// The trigger reads the object-bytes estimate, and the memory backstop is
    /// the bound on the other side: a buffer whose struct headers dwarf its
    /// payload still flushes before it can hold an unbounded amount of RAM.
    #[test]
    fn size_trigger_reads_object_bytes_with_a_memory_backstop() {
        let cfg = IngestConfig {
            target_bytes: 8 * 1024 * 1024,
            ..IngestConfig::default()
        };
        let default_ceiling = IngestByteBudgetLimit::Bounded(512 * 1024 * 1024);
        let backstop = buffer_memory_backstop_bytes(&cfg, default_ceiling);
        assert_eq!(backstop, 64 * 1024 * 1024);

        // Object bytes decide, whatever the buffer holds.
        assert!(!size_trigger_fires(
            cfg.target_bytes - 1,
            0,
            &cfg,
            default_ceiling
        ));
        assert!(size_trigger_fires(
            cfg.target_bytes,
            0,
            &cfg,
            default_ceiling
        ));
        assert!(
            !size_trigger_fires(0, cfg.target_bytes, &cfg, default_ceiling),
            "buffered bytes at target_bytes no longer fire the trigger"
        );

        // Backstop decides when the buffer runs far past the payload it holds.
        assert!(!size_trigger_fires(0, backstop - 1, &cfg, default_ceiling));
        assert!(size_trigger_fires(0, backstop, &cfg, default_ceiling));

        // A target above the cap raises the backstop with it, so the
        // backstop can never pre-empt the trigger it backs.
        let wide = IngestConfig {
            target_bytes: 256 * 1024 * 1024,
            ..IngestConfig::default()
        };
        assert_eq!(
            buffer_memory_backstop_bytes(&wide, default_ceiling),
            256 * 1024 * 1024
        );
    }

    /// The backstop is a share of the ceiling an operator configured, not of
    /// the default one. A replica sized with `--max-ingest-buffer-bytes
    /// 67108864` must not let one (shard, tenant) buffer hold the whole process
    /// budget and shed every other tenant's write.
    #[test]
    fn memory_backstop_is_a_share_of_the_configured_ceiling() {
        let cfg = IngestConfig {
            target_bytes: 8 * 1024 * 1024,
            ..IngestConfig::default()
        };
        let small = 64 * 1024 * 1024_u64;
        let backstop = buffer_memory_backstop_bytes(&cfg, IngestByteBudgetLimit::Bounded(small));
        assert_eq!(
            backstop,
            8 * 1024 * 1024,
            "an eighth of the configured ceiling, not the 64 MiB the default ceiling earns"
        );
        assert_eq!(
            backstop as u64 * BUFFER_MEMORY_BACKSTOP_BUDGET_DIVISOR,
            small,
            "one buffer holds an eighth of the budget, leaving seven eighths"
        );
        assert!(
            size_trigger_fires(0, backstop, &cfg, IngestByteBudgetLimit::Bounded(small)),
            "the buffer flushes at an eighth of the budget"
        );
        assert!(
            size_trigger_fires(
                0,
                small as usize - 1,
                &cfg,
                IngestByteBudgetLimit::Bounded(small)
            ),
            "a buffer one byte short of the whole ceiling has long since flushed"
        );

        // Unlimited has no ceiling to take a share of: the cap alone bounds a
        // buffer's resident memory, and nothing sheds under it.
        assert_eq!(
            buffer_memory_backstop_bytes(&cfg, IngestByteBudgetLimit::Unlimited),
            64 * 1024 * 1024
        );
        // A ceiling far above the default does not raise the backstop with it.
        assert_eq!(
            buffer_memory_backstop_bytes(
                &cfg,
                IngestByteBudgetLimit::Bounded(64 * 1024 * 1024 * 1024)
            ),
            64 * 1024 * 1024
        );
        // `Bounded(0)` sheds every non-empty write, so nothing can buffer to a
        // backstop at all; `target_bytes` is the floor that remains.
        assert_eq!(
            buffer_memory_backstop_bytes(&cfg, IngestByteBudgetLimit::Bounded(0)),
            8 * 1024 * 1024
        );
    }

    #[test]
    fn defaults_match_sizing_table() {
        let cfg = IngestConfig::default();
        assert_eq!(cfg.shard_count, 4);
        assert_eq!(cfg.channel_depth, 256);
        assert_eq!(cfg.target_bytes, 8 * 1024 * 1024);
        assert_eq!(cfg.max_flush_delay, Duration::from_secs(2));
        assert_eq!(cfg.flush_tick, Duration::from_millis(200));
        assert_eq!(cfg.max_flush_delay_idle, Duration::from_secs(40));
        assert_eq!(cfg.min_flush_bytes, 256 * 1024);
        assert_eq!(cfg.idle_flush_byte_floor, 0);
        assert_eq!(cfg.put_retry_max_attempts, 4);
        assert_eq!(cfg.put_retry_base_delay, Duration::from_millis(100));
        assert_eq!(cfg.put_retry_max_delay, Duration::from_secs(2));
        assert_eq!(cfg.max_flush_lifetime, Duration::from_secs(3600));
        // Asserted against the shared cap's own constant, never a literal:
        // ADR-0047's default window lives in ravel-types.
        assert_eq!(
            cfg.exemplar_cap_window_ns,
            ravel_types::ExemplarCap::DEFAULT_WINDOW_NS
        );
        // ADR-0067 decision 2: default reproduces today's one-flush-at-a-time
        // behavior bit for bit; the flip to 3 is a later measured decision.
        assert_eq!(cfg.max_inflight_flushes, 1);
        // Issue #1740: the count bound on spawned-but-unreaped flush tasks per
        // shard, the one that still holds under an Unlimited byte budget.
        assert_eq!(cfg.max_queued_flushes, 8);
        assert_eq!(cfg.queued_flush_cap(), 8);
        assert!(!cfg.adaptive_flush_delay);
        // ADR-0076 decision 4: must exceed the max_flush_delay default (2s)
        // by STRICT_VISIBILITY_RESERVE_NS, not equal it -- equal collapses
        // the adaptive corridor to the floor unconditionally.
        assert_eq!(
            cfg.strict_visibility_budget_ns,
            cfg.max_flush_delay.as_nanos() as i64 + STRICT_VISIBILITY_RESERVE_NS
        );
    }

    /// ADR-1737 decision 1: 0 disables the floor and is always accepted, a
    /// floor below `min_flush_bytes` is accepted, and a floor at or above it
    /// is refused with the typed error naming both values.
    #[test]
    fn idle_flush_byte_floor_must_be_below_min_flush_bytes() {
        let min_flush_bytes = 256 * 1024;
        let with_floor = |floor| IngestConfig {
            min_flush_bytes,
            idle_flush_byte_floor: floor,
            ..IngestConfig::default()
        };
        assert_eq!(IngestConfig::default().validate(), Ok(()));
        assert_eq!(with_floor(0).validate(), Ok(()));
        assert_eq!(with_floor(min_flush_bytes - 1).validate(), Ok(()));
        assert_eq!(
            with_floor(min_flush_bytes).validate(),
            Err(IngestConfigError::IdleFlushByteFloorNotBelowMinFlushBytes {
                floor: min_flush_bytes,
                min_flush_bytes,
            })
        );
        assert_eq!(
            with_floor(min_flush_bytes + 1).validate(),
            Err(IngestConfigError::IdleFlushByteFloorNotBelowMinFlushBytes {
                floor: min_flush_bytes + 1,
                min_flush_bytes,
            })
        );
    }

    /// A 0 cap would refuse every size and age trigger, leaving a shard to
    /// buffer until a drain. The accessor the actors read floors it at 1, so a
    /// library caller that builds a 0 gets one queued flush, not none.
    #[test]
    fn queued_flush_cap_floors_at_one() {
        let zero = IngestConfig {
            max_queued_flushes: 0,
            ..IngestConfig::default()
        };
        assert_eq!(zero.queued_flush_cap(), 1);
        let three = IngestConfig {
            max_queued_flushes: 3,
            ..IngestConfig::default()
        };
        assert_eq!(three.queued_flush_cap(), 3);
    }

    /// The deferral cap is what the slack leaves a row: at the shipped
    /// defaults 7200 s less the 3600 s lifetime less the 40 s
    /// `max_flush_delay_idle` and one 200 ms tick. A non-zero idle flush byte
    /// floor leaves it unchanged, since the sub-floor hold is not counted. The
    /// adaptive corridor's 2.4 s ceiling sits under the idle clock at the
    /// defaults, so it changes the bound only once its budget passes 40 s.
    #[test]
    fn flush_deferral_cap_is_what_the_slack_leaves_a_row() {
        let shipped = IngestConfig::default();
        assert_eq!(shipped.flush_trigger_age_bound_ns(), 40_200_000_000);
        assert_eq!(shipped.flush_deferral_cap_ns(), 3_559_800_000_000);
        assert_eq!(
            shipped.flush_deferral_cap_ns()
                + shipped.flush_trigger_age_bound_ns()
                + shipped.max_flush_lifetime.as_nanos() as i64,
            i64::from(ravel_catalog::FLUSH_BOUND_SLACK_HOURS) * NS_PER_HOUR
        );
        let floored = IngestConfig {
            idle_flush_byte_floor: 64 * 1024,
            ..shipped
        };
        assert_eq!(floored.validate(), Ok(()));
        assert_eq!(floored.flush_trigger_age_bound_ns(), 40_200_000_000);
        assert_eq!(floored.flush_deferral_cap_ns(), 3_559_800_000_000);
        let adaptive = IngestConfig {
            adaptive_flush_delay: true,
            ..shipped
        };
        assert_eq!(adaptive.flush_trigger_age_bound_ns(), 40_200_000_000);
        assert_eq!(adaptive.flush_deferral_cap_ns(), 3_559_800_000_000);
        let wide_adaptive = IngestConfig {
            strict_visibility_budget_ns: 60_000_000_000,
            ..adaptive
        };
        assert_eq!(wide_adaptive.flush_trigger_age_bound_ns(), 60_100_000_000);
        assert_eq!(wide_adaptive.flush_deferral_cap_ns(), 3_539_900_000_000);
        let slow_fast_clock = IngestConfig {
            max_flush_delay: Duration::from_secs(50),
            ..shipped
        };
        assert_eq!(slow_fast_clock.flush_trigger_age_bound_ns(), 50_200_000_000);
        let spent = IngestConfig {
            max_flush_lifetime: Duration::from_secs(7200),
            ..shipped
        };
        assert_eq!(spent.flush_deferral_cap_ns(), 0);
    }

    #[test]
    fn segment_format_version_tracks_the_rseg_trailer() {
        // Asserted against the format's own constant, never a literal. A
        // literal here is exactly what let the RSPAN v2 bump ship a
        // version-1 claim in every span commit record while this style of
        // test stayed green (see the same fix for spans and logs).
        assert_eq!(SEGMENT_FORMAT_VERSION, ravel_segment::VERSION_V7);
    }

    #[test]
    fn span_segment_format_version_tracks_the_rspan_trailer() {
        // Asserted against the format's own constant, not a literal. A literal
        // here is what let the v2 bump ship a version-1 claim in every span
        // commit record while this test stayed green.
        assert_eq!(SPAN_SEGMENT_FORMAT_VERSION, ravel_rspan::footer::VERSION);
    }

    #[test]
    fn log_segment_format_version_tracks_the_rlog_trailer() {
        assert_eq!(LOG_SEGMENT_FORMAT_VERSION, ravel_logseg::footer::VERSION);
    }

    #[test]
    fn checked_ingest_hour_bucket_rejects_zero() {
        assert!(checked_ingest_hour_bucket(0).is_err());
    }

    #[test]
    fn checked_ingest_hour_bucket_rejects_negative() {
        assert!(checked_ingest_hour_bucket(-1).is_err());
    }

    #[test]
    fn checked_ingest_hour_bucket_accepts_a_normal_reading() {
        // A reading three hours past the 2020 plausibility floor buckets into
        // that floor's hour plus three. `MIN_PLAUSIBLE_INGEST_CLOCK_NS` is an
        // exact multiple of `NS_PER_HOUR` (2020-01-01T00:00:00Z is hour
        // 438288), so the arithmetic is exact.
        let floor_bucket = (MIN_PLAUSIBLE_INGEST_CLOCK_NS / NS_PER_HOUR) as u32;
        assert_eq!(
            checked_ingest_hour_bucket(MIN_PLAUSIBLE_INGEST_CLOCK_NS + NS_PER_HOUR * 3 + 1),
            Ok(floor_bucket + 3)
        );
    }

    #[test]
    fn checked_ingest_hour_bucket_rejects_a_positive_sub_floor_reading() {
        // A positive reading below the 2020 floor (a host whose RTC reset to
        // shortly after the epoch): previously this passed and bucketed into a
        // far-past hour; the amendment fails it loud. `NS_PER_HOUR * 3` is such
        // a reading (1970, three hours after the epoch).
        assert!(checked_ingest_hour_bucket(NS_PER_HOUR * 3 + 1).is_err());
    }

    #[test]
    fn plausible_ingest_clock_accepts_a_post_floor_reading() {
        assert!(plausible_ingest_clock(MIN_PLAUSIBLE_INGEST_CLOCK_NS).is_ok());
        assert!(plausible_ingest_clock(MIN_PLAUSIBLE_INGEST_CLOCK_NS + NS_PER_HOUR).is_ok());
    }

    #[test]
    fn plausible_ingest_clock_rejects_a_sub_floor_reading() {
        assert!(plausible_ingest_clock(MIN_PLAUSIBLE_INGEST_CLOCK_NS - 1).is_err());
        assert!(plausible_ingest_clock(NS_PER_HOUR * 3).is_err());
        assert!(plausible_ingest_clock(0).is_err());
        assert!(plausible_ingest_clock(-1).is_err());
    }

    #[test]
    fn rlog_zstd_level_refuses_a_level_outside_zstds_range() {
        for level in [23, i32::MAX, -131_073, i32::MIN] {
            assert_eq!(
                RlogZstdLevel::new(level),
                Err(RlogZstdLevelError::OutOfRange {
                    level,
                    min: -131_072,
                    max: 22,
                }),
                "level {level}"
            );
        }
        for level in [-131_072, -1, 0, 1, 3, 19, 22] {
            assert_eq!(RlogZstdLevel::new(level).map(RlogZstdLevel::get), Ok(level));
        }
        assert_eq!(IngestConfig::default().rlog_zstd_level.get(), 3);
        assert_eq!(RlogZstdLevel::default(), RlogZstdLevel::DEFAULT);
    }
}
