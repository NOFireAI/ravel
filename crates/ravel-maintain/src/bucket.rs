//! The compaction unit: one `(tenant, signal, shard, ingest-hour)` bucket.

use ravel_types::{Signal, TenantHash};

use crate::config::{CompactorConfig, LATE_COMMIT_SETTLE_NS, NS_PER_HOUR};

/// One sealed-hour compaction unit. Identity is exactly the four fields the
/// commit-record key prefix is built from, so a `Bucket` maps to exactly one
/// `c/<shard>/<hour>/` listing prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bucket {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
}

impl Bucket {
    pub fn new(
        tenant_hash: TenantHash,
        signal: Signal,
        shard: u32,
        ingest_hour_bucket: u32,
    ) -> Self {
        Bucket {
            tenant_hash,
            signal,
            shard,
            ingest_hour_bucket,
        }
    }

    /// Exclusive end of this bucket's ingest hour, in unix nanoseconds.
    /// `ingest_hour_bucket` is unix hours, so the bucket covers
    /// `[hour * 3.6e12, (hour + 1) * 3.6e12)`.
    pub fn end_ns(&self) -> i64 {
        i64::from(self.ingest_hour_bucket)
            .saturating_add(1)
            .saturating_mul(NS_PER_HOUR)
    }

    /// Inclusive start of this bucket's ingest hour, in unix nanoseconds.
    pub fn start_ns(&self) -> i64 {
        i64::from(self.ingest_hour_bucket).saturating_mul(NS_PER_HOUR)
    }

    /// A bucket is sealed once `now_ns >= end_ns + seal_margin`: past that no
    /// writer issues a commit PUT for it. A PUT issued before can still land
    /// later, so a LIST of a sealed bucket is not a complete input set on its
    /// own; [`Self::is_plannable`] and the late-commit cutoff are what make the
    /// planned set repeatable (ADR-4561).
    pub fn is_sealed(&self, now_ns: i64, config: &CompactorConfig) -> bool {
        now_ns >= self.end_ns().saturating_add(config.seal_margin_ns())
    }

    /// The late-commit cutoff `C(H)` (ADR-4561 R1): the seal instant
    /// `end_ns + seal_margin`, truncated down to a whole second. A commit
    /// record the store stamped at or after it is late.
    pub fn late_commit_cutoff_ns(&self, config: &CompactorConfig) -> i64 {
        let seal = self.end_ns().saturating_add(config.seal_margin_ns());
        seal - seal.rem_euclid(NS_PER_SECOND)
    }

    /// Whether a pass may plan this bucket from raw L0 (ADR-4561 R3): its own
    /// clock `now_ns` and, when the store gave one, the store-clock observation
    /// `observed_store_ns` both read at least `C(H) + LATE_COMMIT_SETTLE_NS`.
    pub fn is_plannable(
        &self,
        now_ns: i64,
        observed_store_ns: Option<i64>,
        config: &CompactorConfig,
    ) -> bool {
        let settled = self
            .late_commit_cutoff_ns(config)
            .saturating_add(LATE_COMMIT_SETTLE_NS);
        now_ns >= settled && observed_store_ns.is_none_or(|observed| observed >= settled)
    }
}

const NS_PER_SECOND: i64 = 1_000_000_000;
