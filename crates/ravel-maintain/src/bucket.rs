//! The compaction unit: one `(tenant, signal, shard, ingest-hour)` bucket.

use ravel_types::{Signal, TenantHash};

use crate::config::{CompactorConfig, NS_PER_HOUR};

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

    /// A bucket is sealed once `now_ns >= end_ns + seal_margin`:
    /// the writer interlock then guarantees no further commit record can ever
    /// appear, making one strongly consistent LIST a complete input set.
    pub fn is_sealed(&self, now_ns: i64, config: &CompactorConfig) -> bool {
        now_ns >= self.end_ns().saturating_add(config.seal_margin_ns())
    }
}
