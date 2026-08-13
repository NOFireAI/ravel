/// Default per-entry max-age: 23 hours, in nanoseconds. Chosen by ADR-0064
/// (issue #753): raw bytes of an erased subject must not outlive the erasure
/// sweep on any query node by more than the ADR's 24 h bound. The worst-case
/// residue of an idle entry is `max_entry_age_ns + sweep_interval_ns` (see
/// [`DEFAULT_SWEEP_INTERVAL_NS`]), so the default max-age is set one sweep
/// interval below 24 h (23 h + a 1 h sweep = 24 h exactly) rather than at 24 h,
/// so the default actually meets the ADR bound instead of overshooting it by
/// the sweep period. Both cache tiers share it: the disk tier bounds bytes on a
/// node's disposable local disk (issue #753), the RAM tier bounds the same
/// bytes in a query node's memory (issue #988). The erased data is identical
/// PII either way, and the erasure sweep -- running on the maintain node --
/// can reach neither a query node's disk nor its RAM, so both tiers enforce the
/// same bound locally.
pub const DEFAULT_MAX_ENTRY_AGE_NS: u64 = 23 * 60 * 60 * 1_000_000_000;

/// Default period of a tier's background age sweep: 1 hour, in nanoseconds. A
/// per-`get` (and, on disk, startup-scan) age check only reaches an entry that
/// is read or that a fresh process scans; an entry that is never re-read and
/// sees no eviction pressure needs a periodic pass to physically drop its bytes
/// (ADR-0064, issue #753 finding F1 for disk, issue #988 for RAM). An idle
/// entry therefore ages out within at most one sweep interval past its
/// max-age, so the true worst-case residue is
/// `max_entry_age_ns + sweep_interval_ns`, not `max_entry_age_ns` alone. The
/// defaults are tuned so that sum meets ADR-0064's 24 h bound exactly:
/// [`DEFAULT_MAX_ENTRY_AGE_NS`] (23 h) + this (1 h) = 24 h. Shared by both
/// tiers for the same reason the max-age is.
pub const DEFAULT_SWEEP_INTERVAL_NS: u64 = 60 * 60 * 1_000_000_000;

/// Bounds on a cache tier. The first three are enforced independently by both
/// tiers: an entry that would only blow `max_entry_bytes` is rejected even if
/// `max_bytes` has room left, and an insert that fits `max_entry_bytes` can
/// still trigger eviction of other entries to stay under `max_bytes` or
/// `max_entries`.
///
/// `max_entry_age_ns` and `sweep_interval_ns` bound *both* tiers (see
/// [`crate::DiskCache`] and [`crate::Cache`]): `max_entry_age_ns` is the
/// maximum wall-clock age an entry is served at before it is treated as a miss
/// and dropped, and `sweep_interval_ns` is the period of the background pass
/// that drops over-age idle entries. Together they bound how long raw bytes of
/// an erased subject persist on a query node -- on local disk (ADR-0064, issue
/// #753) and in RAM (issue #988) alike.
#[derive(Debug, Clone, Copy)]
pub struct CacheLimits {
    /// Total bytes across every resident entry.
    pub max_bytes: u64,
    /// Total number of resident entries.
    pub max_entries: usize,
    /// Largest single entry the tier will admit. Exceeding this is not an
    /// error: the entry is simply not cached, and the caller still gets
    /// its bytes from whatever fetched them.
    pub max_entry_bytes: u64,
    /// Both tiers: the maximum wall-clock age, in nanoseconds since the Unix
    /// epoch, an entry is served at. A hit whose stamped `written_at_ns` is
    /// older than this is treated as a miss and the stale bytes are dropped
    /// (from disk for [`crate::DiskCache`], from memory for [`crate::Cache`]).
    /// Defaults to [`DEFAULT_MAX_ENTRY_AGE_NS`] (23 h).
    pub max_entry_age_ns: u64,
    /// Both tiers: the period, in nanoseconds, of the background sweep that
    /// drops entries past `max_entry_age_ns` even when they are never re-read
    /// (ADR-0064, issue #753 finding F1 for disk, issue #988 for RAM). An idle
    /// entry ages out within at most this interval past `max_entry_age_ns`.
    /// Defaults to [`DEFAULT_SWEEP_INTERVAL_NS`] (1 h).
    pub sweep_interval_ns: u64,
}

impl CacheLimits {
    /// Constructs limits with the default disk-tier max-age
    /// ([`DEFAULT_MAX_ENTRY_AGE_NS`], 23 h). Use
    /// [`CacheLimits::with_max_entry_age_ns`] to override it.
    pub fn new(max_bytes: u64, max_entries: usize, max_entry_bytes: u64) -> Self {
        CacheLimits {
            max_bytes,
            max_entries,
            max_entry_bytes,
            max_entry_age_ns: DEFAULT_MAX_ENTRY_AGE_NS,
            sweep_interval_ns: DEFAULT_SWEEP_INTERVAL_NS,
        }
    }

    /// Overrides the per-entry max-age (nanoseconds), for either tier.
    /// Builder-style so the common `new(..)` call sites stay unchanged and only
    /// a caller (or test) that needs a non-default age names it.
    pub fn with_max_entry_age_ns(mut self, max_entry_age_ns: u64) -> Self {
        self.max_entry_age_ns = max_entry_age_ns;
        self
    }

    /// Overrides the background sweep period (nanoseconds), for either tier.
    /// Builder-style, like [`CacheLimits::with_max_entry_age_ns`]: the common
    /// `new(..)` call sites keep the [`DEFAULT_SWEEP_INTERVAL_NS`] default, and
    /// only a caller (or a test) that needs a different cadence names it.
    pub fn with_sweep_interval_ns(mut self, sweep_interval_ns: u64) -> Self {
        self.sweep_interval_ns = sweep_interval_ns;
        self
    }
}
