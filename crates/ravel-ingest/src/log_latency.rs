//! Fixed-bucket latency histograms for the logs strict-ack path (issue #3533):
//! one per flush phase and one per strict-ack phase, held in
//! [`crate::LogIngestMetrics`] and carried on its snapshot.
//!
//! Every observation is a difference of two injected-[`crate::Clock`]
//! readings, so a test that advances its clock pins each figure exactly. The
//! buckets are the object store's fixed latency bounds
//! ([`ravel_object_store::instrument::LATENCY_BUCKET_BOUNDS_MICROS`]), selected by the same
//! [`latency_bucket`], so these histograms and `ravel_store_latency_seconds`
//! share one `le` set.

use std::sync::atomic::{AtomicU64, Ordering};

use ravel_object_store::instrument::{LATENCY_BUCKET_COUNT, latency_bucket};

/// One step of a logs flush, in the order a flush reaches them, plus the
/// whole flush as `Total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlushPhase {
    /// The wait for the tenant's flush share and the shard's
    /// `max_inflight_flushes` permit.
    PermitWait,
    /// The tenant's indexed-field and storage-layout resolve, a config GET on
    /// a cache miss.
    Resolve,
    /// The encode on the ADR-1702 write gate: the gate wait and the RLOG
    /// encode, which are one future.
    Encode,
    /// The data object PUT, retries included.
    DataPut,
    /// The commit record build and its declared-column stamps.
    CommitBuild,
    /// The commit record publish, retries included.
    CommitPublish,
    /// From the flush task's spawn to the end of its ack fan-out, on every
    /// exit the task takes.
    Total,
}

impl FlushPhase {
    /// Every phase, in flush order with `Total` last.
    pub const ALL: [FlushPhase; 7] = [
        FlushPhase::PermitWait,
        FlushPhase::Resolve,
        FlushPhase::Encode,
        FlushPhase::DataPut,
        FlushPhase::CommitBuild,
        FlushPhase::CommitPublish,
        FlushPhase::Total,
    ];

    /// The `phase` label value.
    pub fn name(self) -> &'static str {
        match self {
            FlushPhase::PermitWait => "permit_wait",
            FlushPhase::Resolve => "resolve",
            FlushPhase::Encode => "encode",
            FlushPhase::DataPut => "data_put",
            FlushPhase::CommitBuild => "commit_build",
            FlushPhase::CommitPublish => "commit_publish",
            FlushPhase::Total => "total",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// One span of a strict logs write's acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrictAckPhase {
    /// Per strict waiter: from its registration on the shard actor to the
    /// spawn of the flush that carries it.
    Queued,
    /// Per strict request: from the router's submit to every shard's ack
    /// received, or to the ack deadline.
    Request,
    /// Per strict request that reached more than one shard: from the first
    /// shard ack received to the last.
    ShardSkew,
}

impl StrictAckPhase {
    pub const ALL: [StrictAckPhase; 3] = [
        StrictAckPhase::Queued,
        StrictAckPhase::Request,
        StrictAckPhase::ShardSkew,
    ];

    /// The `phase` label value.
    pub fn name(self) -> &'static str {
        match self {
            StrictAckPhase::Queued => "queued",
            StrictAckPhase::Request => "request",
            StrictAckPhase::ShardSkew => "shard_skew",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Per-bucket (not cumulative) counts on the store's latency bounds and
/// the summed nanoseconds of every observation.
#[derive(Debug, Default)]
struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
    sum_ns: AtomicU64,
}

impl LatencyHistogram {
    fn record(&self, elapsed_ns: u64) {
        self.buckets[latency_bucket(elapsed_ns)].fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LatencyHistogramSnapshot {
        let mut buckets = [0u64; LATENCY_BUCKET_COUNT];
        for (slot, counter) in buckets.iter_mut().zip(self.buckets.iter()) {
            *slot = counter.load(Ordering::Relaxed);
        }
        LatencyHistogramSnapshot {
            buckets,
            sum_ns: self.sum_ns.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of one histogram. `buckets[i]` counts observations in
/// bucket `i` only, as `OpMetricsSnapshot::latency_micros_buckets` does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatencyHistogramSnapshot {
    pub buckets: [u64; LATENCY_BUCKET_COUNT],
    pub sum_ns: u64,
}

impl LatencyHistogramSnapshot {
    /// Observations recorded, the sum of every bucket.
    pub fn count(&self) -> u64 {
        self.buckets.iter().sum()
    }

    /// Adds one observation, as the live histogram does; for callers that
    /// build a snapshot by hand.
    pub fn record(&mut self, elapsed_ns: u64) {
        self.buckets[latency_bucket(elapsed_ns)] += 1;
        self.sum_ns += elapsed_ns;
    }
}

/// The logs pipeline's phase histograms.
#[derive(Debug, Default)]
pub(crate) struct LogLatency {
    flush: [LatencyHistogram; FlushPhase::ALL.len()],
    strict_ack: [LatencyHistogram; StrictAckPhase::ALL.len()],
}

impl LogLatency {
    pub(crate) fn record_flush(&self, phase: FlushPhase, elapsed_ns: u64) {
        self.flush[phase.index()].record(elapsed_ns);
    }

    pub(crate) fn record_strict_ack(&self, phase: StrictAckPhase, elapsed_ns: u64) {
        self.strict_ack[phase.index()].record(elapsed_ns);
    }

    pub(crate) fn snapshot(&self) -> LogLatencySnapshot {
        LogLatencySnapshot {
            flush: std::array::from_fn(|i| self.flush[i].snapshot()),
            strict_ack: std::array::from_fn(|i| self.strict_ack[i].snapshot()),
        }
    }
}

/// Point-in-time copy of the logs phase histograms, read by phase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogLatencySnapshot {
    flush: [LatencyHistogramSnapshot; FlushPhase::ALL.len()],
    strict_ack: [LatencyHistogramSnapshot; StrictAckPhase::ALL.len()],
}

impl LogLatencySnapshot {
    pub fn flush(&self, phase: FlushPhase) -> &LatencyHistogramSnapshot {
        &self.flush[phase.index()]
    }

    pub fn flush_mut(&mut self, phase: FlushPhase) -> &mut LatencyHistogramSnapshot {
        &mut self.flush[phase.index()]
    }

    pub fn strict_ack(&self, phase: StrictAckPhase) -> &LatencyHistogramSnapshot {
        &self.strict_ack[phase.index()]
    }

    pub fn strict_ack_mut(&mut self, phase: StrictAckPhase) -> &mut LatencyHistogramSnapshot {
        &mut self.strict_ack[phase.index()]
    }
}

/// Elapsed nanoseconds between two injected-clock readings, floored at zero.
pub(crate) fn elapsed_ns(start_ns: i64, end_ns: i64) -> u64 {
    u64::try_from(end_ns.saturating_sub(start_ns)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use ravel_object_store::instrument::LATENCY_BUCKET_BOUNDS_MICROS;

    use super::*;

    #[test]
    fn phases_index_their_own_histogram() {
        let latency = LogLatency::default();
        for (i, phase) in FlushPhase::ALL.into_iter().enumerate() {
            for _ in 0..=i {
                latency.record_flush(phase, 1_000);
            }
        }
        latency.record_strict_ack(StrictAckPhase::ShardSkew, 7);
        let snap = latency.snapshot();
        for (i, phase) in FlushPhase::ALL.into_iter().enumerate() {
            assert_eq!(snap.flush(phase).count(), i as u64 + 1, "{}", phase.name());
            assert_eq!(snap.flush(phase).sum_ns, 1_000 * (i as u64 + 1));
        }
        assert_eq!(snap.strict_ack(StrictAckPhase::Queued).count(), 0);
        assert_eq!(snap.strict_ack(StrictAckPhase::Request).count(), 0);
        assert_eq!(snap.strict_ack(StrictAckPhase::ShardSkew).count(), 1);
        assert_eq!(snap.strict_ack(StrictAckPhase::ShardSkew).sum_ns, 7);
    }

    #[test]
    fn buckets_follow_the_store_bounds() {
        let latency = LogLatency::default();
        // 100us is bucket 0's inclusive bound; 6s is past the last bound.
        latency.record_flush(FlushPhase::Total, 100_000);
        latency.record_flush(FlushPhase::Total, 100_001_000);
        latency.record_flush(FlushPhase::Total, 6_000_000_000);
        let total = *latency.snapshot().flush(FlushPhase::Total);
        let mut expected = [0u64; LATENCY_BUCKET_COUNT];
        expected[0] = 1;
        let past_100ms = LATENCY_BUCKET_BOUNDS_MICROS
            .iter()
            .position(|bound| *bound == 250_000)
            .expect("250ms bound");
        expected[past_100ms] = 1;
        expected[LATENCY_BUCKET_COUNT - 1] = 1;
        assert_eq!(total.buckets, expected);
        assert_eq!(total.count(), 3);
    }

    #[test]
    fn elapsed_floors_a_backwards_reading_at_zero() {
        assert_eq!(elapsed_ns(10, 4), 0);
        assert_eq!(elapsed_ns(4, 10), 6);
    }
}
