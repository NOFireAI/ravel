//! Compactor configuration: the seal margin, the trigger threshold, the part
//! size cap, and the abandonment deadline (docs/compaction-retention-plan.md
//! §3.2, §3.3, §3.4). All durations are nanoseconds to match the injected
//! [`crate::clock::Clock`].

use uuid::Uuid;

/// Nanoseconds in one hour; an ingest-hour bucket spans exactly this.
pub const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Default `max_flush_lifetime`: 1 hour (matches ravel-ingest and
/// ravel-catalog, ADR-0010 §11).
pub const DEFAULT_MAX_FLUSH_LIFETIME_NS: i64 = NS_PER_HOUR;
/// Default `clock_skew_allowance`: 5 minutes (matches ravel-catalog).
pub const DEFAULT_CLOCK_SKEW_ALLOWANCE_NS: i64 = 300_000_000_000;
/// Default `max_compaction_lifetime`: 1 hour (plan §3.4 point 4). Mirrors the
/// writer interlock so the sweeper's unreferenced-part rule is safe.
pub const DEFAULT_MAX_COMPACTION_LIFETIME_NS: i64 = NS_PER_HOUR;
/// Default `max_l1_part_bytes`: 256 MiB (plan §3.3 point 4).
pub const DEFAULT_MAX_L1_PART_BYTES: u64 = 256 * 1024 * 1024;
/// Default minimum L0 records for a bucket to be worth compacting (plan §3.2).
pub const DEFAULT_MIN_COMPACTION_INPUTS: usize = 2;
/// Default footer suffix-probe size. 64 KiB covers the footer + catalog of a
/// typical L0 flush in one GET (docs/segment-format.md reader protocol).
pub const DEFAULT_FOOTER_PROBE_BYTES: u64 = 64 * 1024;

/// Everything the compactor needs beyond the store and the clock.
#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// Longest a flush may stay open; a bucket is sealed only after its end
    /// plus this plus the skew allowance (plan §3.2).
    pub max_flush_lifetime_ns: i64,
    /// Extra seal margin for cross-host clock skew (plan §3.2).
    pub clock_skew_allowance_ns: i64,
    /// Deadline after which a compaction run must not publish its record
    /// (plan §3.4 point 4); measured from the run's start via the clock.
    pub max_compaction_lifetime_ns: i64,
    /// Split parts on series boundaries once accumulated verbatim page bytes
    /// reach this (plan §3.3 point 4).
    pub max_l1_part_bytes: u64,
    /// Buckets with fewer L0 records than this are left uncompacted; set 1 for
    /// v1-retirement campaigns (plan §3.2).
    pub min_compaction_inputs: usize,
    /// Suffix-probe size for the first footer GET of each input.
    pub footer_probe_bytes: u64,
    /// This compactor process's uuid. Informational only: it is recorded in
    /// each part's footer `writer_id` and never enters dedup priority
    /// (plan §4). Default is the nil uuid; the service sets a real one.
    pub compactor_writer_id: Uuid,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfig {
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            max_compaction_lifetime_ns: DEFAULT_MAX_COMPACTION_LIFETIME_NS,
            max_l1_part_bytes: DEFAULT_MAX_L1_PART_BYTES,
            min_compaction_inputs: DEFAULT_MIN_COMPACTION_INPUTS,
            footer_probe_bytes: DEFAULT_FOOTER_PROBE_BYTES,
            compactor_writer_id: Uuid::nil(),
        }
    }
}

impl CompactorConfig {
    /// The seal margin: a bucket ending at `bucket_end_ns` is sealed once
    /// `now_ns >= bucket_end_ns + this` (plan §3.2). No new commit record can
    /// appear in the bucket after that, so a single strongly consistent LIST
    /// is a complete, repeatable input set.
    pub fn seal_margin_ns(&self) -> i64 {
        self.max_flush_lifetime_ns
            .saturating_add(self.clock_skew_allowance_ns)
    }
}
