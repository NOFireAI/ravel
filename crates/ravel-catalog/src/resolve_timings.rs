//! Wall time of one snapshot resolve, split into its parts (issue #4634).

use std::time::Duration;

/// Where one resolve's wall time went, measured on the catalog's injected
/// monotonic clock ([`crate::Catalog::with_monotonic_clock`]). The four parts
/// are disjoint intervals of the resolve and sum to its whole duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResolveTimings {
    /// The provisioning reads at the start of the resolve: the once-per-pair
    /// `shard_count` check and the generation history read.
    pub generations: Duration,
    /// The joined LIST wave, from its start until its last LIST (the shard
    /// LISTs and the `del/` LIST) returns, plus each HEAD lookup that issued a
    /// store request. A resolve that lists no window counts its standalone
    /// `del/` LIST here.
    pub list_wave: Duration,
    /// From the end of the LIST wave to the sorted segment set: the record
    /// cache pass and bucket processing not already overlapped by the wave,
    /// then the `min_tokens` steps and the sort.
    pub records: Duration,
    /// Everything else inside the resolve, such as the snapshot parts' load
    /// and extraction.
    pub other: Duration,
}

impl ResolveTimings {
    /// The sum of the four parts: the resolve's whole duration.
    pub fn total(&self) -> Duration {
        self.generations
            .saturating_add(self.list_wave)
            .saturating_add(self.records)
            .saturating_add(self.other)
    }

    /// Field-wise sum, for a caller that runs several resolves in sequence.
    #[must_use]
    pub fn saturating_add(self, other: ResolveTimings) -> ResolveTimings {
        ResolveTimings {
            generations: self.generations.saturating_add(other.generations),
            list_wave: self.list_wave.saturating_add(other.list_wave),
            records: self.records.saturating_add(other.records),
            other: self.other.saturating_add(other.other),
        }
    }
}

/// The interval from `from_ns` to `to_ns` on one monotonic clock.
pub(crate) fn between(from_ns: u64, to_ns: u64) -> Duration {
    Duration::from_nanos(to_ns.saturating_sub(from_ns))
}
