//! Tunable thresholds for the P4 compactor (plan §3.2, §3.3, §3.4).

const NS_PER_SEC: i64 = 1_000_000_000;

/// Sealed-bucket scan, build, and publish thresholds. Defaults match plan
/// §3.2/§3.3/§3.4 exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintainConfig {
    /// Upper bound on how long a writer may keep a flush open (plan §3.2).
    pub max_flush_lifetime_ns: i64,
    /// Allowance for clock skew between writers and the compactor (plan
    /// §3.2).
    pub clock_skew_allowance_ns: i64,
    /// Minimum L0 record count in a sealed bucket to trigger compaction
    /// (plan §3.2). Set to 1 for v1-retirement campaigns.
    pub min_compaction_inputs: usize,
    /// Series-boundary part size cap (plan §3.3 step 4).
    pub max_l1_part_bytes: u64,
    /// A compaction run that cannot publish its record within this window
    /// abandons and must never publish afterward (plan §3.4 step 4).
    pub max_compaction_lifetime_ns: i64,
}

impl Default for MaintainConfig {
    fn default() -> Self {
        MaintainConfig {
            max_flush_lifetime_ns: NS_PER_SEC * 3600,
            clock_skew_allowance_ns: NS_PER_SEC * 300,
            min_compaction_inputs: 2,
            max_l1_part_bytes: 256 << 20,
            max_compaction_lifetime_ns: NS_PER_SEC * 3600,
        }
    }
}

impl MaintainConfig {
    /// `now_ns >= bucket_end_ns + max_flush_lifetime + clock_skew_allowance`
    /// (plan §3.2).
    pub fn is_sealed(&self, now_ns: i64, bucket_end_ns: i64) -> bool {
        now_ns >= bucket_end_ns + self.max_flush_lifetime_ns + self.clock_skew_allowance_ns
    }

    /// Whether a compaction run started at `started_ns` has exceeded
    /// `max_compaction_lifetime` as of `now_ns` (plan §3.4 step 4).
    pub fn is_abandoned(&self, started_ns: i64, now_ns: i64) -> bool {
        now_ns >= started_ns + self.max_compaction_lifetime_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_plan() {
        let cfg = MaintainConfig::default();
        assert_eq!(cfg.max_flush_lifetime_ns, 3600 * NS_PER_SEC);
        assert_eq!(cfg.clock_skew_allowance_ns, 300 * NS_PER_SEC);
        assert_eq!(cfg.min_compaction_inputs, 2);
        assert_eq!(cfg.max_l1_part_bytes, 256 << 20);
        assert_eq!(cfg.max_compaction_lifetime_ns, 3600 * NS_PER_SEC);
    }

    #[test]
    fn seal_rule_boundary_is_inclusive() {
        let cfg = MaintainConfig::default();
        let bucket_end_ns = 0;
        let threshold = cfg.max_flush_lifetime_ns + cfg.clock_skew_allowance_ns;
        assert!(!cfg.is_sealed(threshold - 1, bucket_end_ns));
        assert!(cfg.is_sealed(threshold, bucket_end_ns));
    }

    #[test]
    fn abandonment_boundary_is_inclusive() {
        let cfg = MaintainConfig::default();
        assert!(!cfg.is_abandoned(0, cfg.max_compaction_lifetime_ns - 1));
        assert!(cfg.is_abandoned(0, cfg.max_compaction_lifetime_ns));
    }
}
