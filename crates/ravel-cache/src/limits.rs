/// Default disk-tier per-entry max-age: 24 hours, in nanoseconds. Chosen by
/// ADR-0064 (issue #753): raw bytes of an erased subject must not outlive the
/// erasure sweep on any query node's disposable local disk by more than this.
pub const DEFAULT_MAX_ENTRY_AGE_NS: u64 = 24 * 60 * 60 * 1_000_000_000;

/// Default period of the disk tier's background age sweep: 1 hour, in
/// nanoseconds. The per-`get` and startup age checks only reach an entry that
/// is read or that a fresh process scans; an entry that is never re-read and
/// sees no eviction pressure needs a periodic pass to physically drop its
/// bytes (ADR-0064, issue #753, finding F1). An idle entry therefore ages out
/// within at most this interval past [`DEFAULT_MAX_ENTRY_AGE_NS`], well inside
/// the 24 h bound with room to spare.
pub const DEFAULT_SWEEP_INTERVAL_NS: u64 = 60 * 60 * 1_000_000_000;

/// Bounds on a cache tier. The first three are enforced independently by both
/// tiers: an entry that would only blow `max_entry_bytes` is rejected even if
/// `max_bytes` has room left, and an insert that fits `max_entry_bytes` can
/// still trigger eviction of other entries to stay under `max_bytes` or
/// `max_entries`.
///
/// `max_entry_age_ns` bounds *only the disk tier* (see [`crate::DiskCache`]):
/// it is the maximum wall-clock age a disk entry is served at before it is
/// treated as a miss and dropped, so that raw bytes of an erased subject
/// persist on local disk at most that long past the sweep (ADR-0064, issue
/// #753). The RAM tier is bounded by eviction alone and ignores this field.
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
    /// Disk tier only: the maximum wall-clock age, in nanoseconds since the
    /// Unix epoch, a disk entry is served at. A disk hit whose stamped
    /// `written_at_ns` is older than this is treated as a miss and the stale
    /// bytes are dropped. Defaults to [`DEFAULT_MAX_ENTRY_AGE_NS`] (24 h).
    pub max_entry_age_ns: u64,
    /// Disk tier only: the period, in nanoseconds, of the background sweep
    /// that drops entries past `max_entry_age_ns` even when they are never
    /// re-read (ADR-0064, issue #753, finding F1). An idle entry ages out
    /// within at most this interval past `max_entry_age_ns`. Defaults to
    /// [`DEFAULT_SWEEP_INTERVAL_NS`] (1 h). The RAM tier ignores this field.
    pub sweep_interval_ns: u64,
}

impl CacheLimits {
    /// Constructs limits with the default disk-tier max-age
    /// ([`DEFAULT_MAX_ENTRY_AGE_NS`], 24 h). Use
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

    /// Overrides the disk-tier per-entry max-age (nanoseconds). Builder-style
    /// so the common `new(..)` call sites stay unchanged and only a disk tier
    /// that needs a non-default age names it.
    pub fn with_max_entry_age_ns(mut self, max_entry_age_ns: u64) -> Self {
        self.max_entry_age_ns = max_entry_age_ns;
        self
    }

    /// Overrides the disk-tier background sweep period (nanoseconds).
    /// Builder-style, like [`CacheLimits::with_max_entry_age_ns`]: the common
    /// `new(..)` call sites keep the [`DEFAULT_SWEEP_INTERVAL_NS`] default, and
    /// only a disk tier (or a test) that needs a different cadence names it.
    pub fn with_sweep_interval_ns(mut self, sweep_interval_ns: u64) -> Self {
        self.sweep_interval_ns = sweep_interval_ns;
        self
    }
}
