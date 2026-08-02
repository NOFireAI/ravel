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
}

impl Default for IngestConfig {
    fn default() -> Self {
        IngestConfig {
            shard_count: 4,
            channel_depth: 256,
            target_bytes: 8 * 1024 * 1024,
            max_flush_delay: Duration::from_millis(500),
            flush_tick: Duration::from_millis(200),
            put_retry_max_attempts: 4,
            put_retry_base_delay: Duration::from_millis(100),
            put_retry_max_delay: Duration::from_secs(2),
            max_flush_lifetime: Duration::from_secs(3600),
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
        assert_eq!(cfg.put_retry_max_attempts, 4);
        assert_eq!(cfg.put_retry_base_delay, Duration::from_millis(100));
        assert_eq!(cfg.put_retry_max_delay, Duration::from_secs(2));
        assert_eq!(cfg.max_flush_lifetime, Duration::from_secs(3600));
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
}
