//! Sizing knobs for the ingest pipeline (docs/ingest.md "Sizing defaults").

use std::time::Duration;

/// RSEG trailer version every flush emits. ADR-0027 leaves v5 the only
/// writable version, so this is no longer a configurable knob; it mirrors
/// `ravel_segment`'s private `format::VERSION_V5` constant (that crate keeps
/// its `format` module private), and is stamped verbatim into the commit
/// record's `segment_format_version`. Changing it is a format-level ADR, not
/// a routine edit.
pub const SEGMENT_FORMAT_VERSION: u16 = ravel_segment::VERSION_V6;

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

/// Derives the commit record's `ingest_hour_bucket` from a flush-open clock
/// reading, fail-loud (ADR-0051 section 7, S1-12). A non-positive reading
/// (a clock that reports zero or has gone backwards past the epoch) can
/// never be a valid hour bucket, and silently mapping it to bucket 0
/// (the previous `unwrap_or(0)` behavior) wrote data into an undiscoverable
/// bucket instead of surfacing the bad reading to the caller. Every one of
/// the three shard actors calls this at the same point `flush_open_ns` is
/// read, so the check runs once per flush, not per retry.
pub(crate) fn checked_ingest_hour_bucket(flush_open_ns: i64) -> Result<u32, String> {
    if flush_open_ns <= 0 {
        return Err(format!(
            "flush clock produced a non-positive reading: flush_open_ns={flush_open_ns}"
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
    /// (ADR-0051 section 7, S2-06/S3-05). Keeps the fast age trigger for
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
}

impl Default for IngestConfig {
    fn default() -> Self {
        IngestConfig {
            shard_count: 4,
            channel_depth: 256,
            target_bytes: 8 * 1024 * 1024,
            max_flush_delay: Duration::from_millis(500),
            flush_tick: Duration::from_millis(200),
            max_flush_delay_idle: Duration::from_secs(10),
            min_flush_bytes: 64 * 1024,
            put_retry_max_attempts: 4,
            put_retry_base_delay: Duration::from_millis(100),
            put_retry_max_delay: Duration::from_secs(2),
            max_flush_lifetime: Duration::from_secs(3600),
            exemplar_cap_window_ns: ravel_types::ExemplarCap::DEFAULT_WINDOW_NS,
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
        assert_eq!(cfg.max_flush_delay, Duration::from_millis(500));
        assert_eq!(cfg.flush_tick, Duration::from_millis(200));
        assert_eq!(cfg.max_flush_delay_idle, Duration::from_secs(10));
        assert_eq!(cfg.min_flush_bytes, 64 * 1024);
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
    }

    #[test]
    fn segment_format_version_tracks_the_rseg_trailer() {
        // Asserted against the format's own constant, never a literal. A
        // literal here is exactly what let the RSPAN v2 bump ship a
        // version-1 claim in every span commit record while this style of
        // test stayed green (see the same fix for spans and logs).
        assert_eq!(SEGMENT_FORMAT_VERSION, ravel_segment::VERSION_V6);
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
        assert_eq!(checked_ingest_hour_bucket(NS_PER_HOUR * 3 + 1), Ok(3));
    }
}
