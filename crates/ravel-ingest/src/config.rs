//! Sizing knobs for the ingest pipeline (docs/ingest.md "Sizing defaults").

use std::time::Duration;

/// RSEG trailer version every flush emits. ADR-0027 leaves v7 the only
/// writable version (ADR-0092 bumped it from v6), so this is no longer a
/// configurable knob; it mirrors `ravel_segment`'s `VERSION_V7` constant, and
/// is stamped verbatim into the commit record's `segment_format_version`.
/// Changing it is a format-level ADR, not a routine edit.
pub const SEGMENT_FORMAT_VERSION: u16 = ravel_segment::VERSION_V7;

/// RLOG trailer version every log flush emits. Mirrors `ravel_logseg`'s own
/// object trailer version (`docs/log-segment-format.md`, ADR-0029); like
/// [`SEGMENT_FORMAT_VERSION`] it is not a configurable knob, and is stamped
/// verbatim into the commit record's `segment_format_version`. Changing it is
/// a format-level ADR, not a routine edit.
pub const LOG_SEGMENT_FORMAT_VERSION: u16 = 2;

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

/// All fields are overridable; defaults match the dev-sizing table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestConfig {
    /// Number of shard actors. Immutable per (tenant, signal) in production
    /// (docs/catalog-and-mvcc.md); changing it after data has landed is a
    /// data-loss operation.
    pub shard_count: u32,
    /// Bounded mpsc channel depth per shard.
    pub channel_depth: usize,
    /// Flush a tenant's buffer once its estimated size reaches this many bytes.
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
    /// A buffer at or above this many estimated bytes is never treated as
    /// idle for the age trigger, even with no strict-mode waiter: it is
    /// already worth the PUT cost `max_flush_delay` pays for.
    pub min_flush_bytes: usize,
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
            put_retry_max_attempts: 4,
            put_retry_base_delay: Duration::from_millis(100),
            put_retry_max_delay: Duration::from_secs(2),
            max_flush_lifetime: Duration::from_secs(3600),
            exemplar_cap_window_ns: ravel_types::ExemplarCap::DEFAULT_WINDOW_NS,
            max_inflight_flushes: 1,
            adaptive_flush_delay: false,
            // Must exceed max_flush_delay by STRICT_VISIBILITY_RESERVE_NS, not
            // equal it: equal leaves visibility_ceiling_ns's subtraction with
            // no headroom, collapsing the adaptive corridor to the floor
            // unconditionally.
            strict_visibility_budget_ns: max_flush_delay.as_nanos() as i64
                + STRICT_VISIBILITY_RESERVE_NS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!cfg.adaptive_flush_delay);
        // ADR-0076 decision 4: must exceed the max_flush_delay default (2s)
        // by STRICT_VISIBILITY_RESERVE_NS, not equal it -- equal collapses
        // the adaptive corridor to the floor unconditionally.
        assert_eq!(
            cfg.strict_visibility_budget_ns,
            cfg.max_flush_delay.as_nanos() as i64 + STRICT_VISIBILITY_RESERVE_NS
        );
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
    fn log_segment_format_version_is_v2() {
        assert_eq!(LOG_SEGMENT_FORMAT_VERSION, 2);
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
}
