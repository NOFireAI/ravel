//! Self-observability counters for the log pipeline, the log-side
//! counterpart of [`crate::IngestMetrics`].
//!
//! # Counting convention
//!
//! Identical to [`crate::metrics`]'s, and worth restating because mixing the
//! two timing conventions up misreads the numbers. Every counter here is a
//! monotonic process-global total with **no per-shard and no per-tenant
//! dimension**: a single [`LogIngestMetrics`] is constructed once by the log
//! router and shared by every log shard actor through an `Arc`, so a value is
//! the sum across all shards and all tenants of this process.
//!
//! - **Attempt-time.** [`record_flush`](LogIngestMetrics::record_flush) fires
//!   when a flush is *opened*, before the RLOG build, the data-object PUT, or
//!   the commit-record PUT. A flush later abandoned is counted in both
//!   `flushes_by_*` **and** one of the `abandoned_*` counters. Flushes that
//!   reached a durable commit are the three trigger counters summed, minus
//!   `abandoned_retry_exhausted` and `abandoned_input_rejected`; the bare
//!   trigger sum overcounts.
//! - **Success-time.** `acks_ok`/`acks_err` are recorded when a flush's
//!   strict waiters are acked, i.e. at the flush's terminal outcome. They
//!   count strict-mode waiters only: a buffered-mode flush, or an age/size
//!   flush with no strict waiter attached, records zero on both.
//!
//! `flushes_manual` covers every [`FlushTrigger::Manual`] flush: an explicit
//! flush request, the shutdown drain, and the channel-close drop-path drain.
//! It is not exclusively operator-requested flushes.
//!
//! Two field names differ from [`crate::IngestMetricsSnapshot`]'s, because
//! the unit differs: `buffered_records_total` (a log unit is a record, not a
//! sample) and `stream_id_collisions` (log identity is a stream, not a
//! series). Everything else matches name for name and semantics for
//! semantics.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::FlushTrigger;

#[derive(Debug, Default)]
pub struct LogIngestMetrics {
    /// Flushes opened because the tenant buffer reached `target_bytes`.
    /// Attempt-time: incremented at flush open, so it includes flushes later
    /// abandoned.
    flushes_by_size: AtomicU64,
    /// Flushes opened because the tenant buffer aged past `max_flush_delay`.
    /// Attempt-time, same as `flushes_by_size`.
    flushes_by_age: AtomicU64,
    /// Flushes opened by any [`FlushTrigger::Manual`] path. Attempt-time.
    flushes_manual: AtomicU64,
    /// Retried PUT attempts across both the data-object and commit-record
    /// paths. Excludes each path's first attempt.
    put_retries: AtomicU64,
    /// Flushes abandoned because a PUT exhausted its retry budget or
    /// `max_flush_lifetime` elapsed first ([`crate::LogWriteError::Abandoned`]).
    /// A durability signal: the input was fine, the object store did not
    /// accept it in time. Nothing was acknowledged and the whole write stays
    /// retryable.
    abandoned_retry_exhausted: AtomicU64,
    /// Flushes abandoned because the input could not be turned into a durable
    /// object at all: the RLOG build, data-key derivation, or commit-record
    /// build failed ([`crate::LogWriteError::SegmentBuild`]). A client
    /// signal: identical input will fail again, so the write is not
    /// retryable.
    abandoned_input_rejected: AtomicU64,
    /// Cumulative bytes admitted into shard buffers at enqueue time.
    buffered_bytes_total: AtomicU64,
    /// Cumulative log record count admitted into shard buffers at enqueue
    /// time.
    buffered_records_total: AtomicU64,
    /// Strict-mode waiters acked with a commit token (success-time). Zero for
    /// buffered-mode and for flushes with no strict waiter.
    acks_ok: AtomicU64,
    /// Strict-mode waiters acked with a [`crate::LogWriteError`]
    /// (success-time). Zero for buffered-mode and for flushes with no strict
    /// waiter.
    acks_err: AtomicU64,
    /// Batches rejected because two records shared a `stream_id` under
    /// different `stream_attrs` (the fail-loud collision check
    /// `RlogWriter::finish()` performs, issue #225).
    stream_id_collisions: AtomicU64,
    /// Distinct log shard actors observed dead by the router: its send half
    /// or a strict-mode ack found the shard channel closed, meaning the actor
    /// task ended (e.g. panicked) without the router shutting it down.
    /// Counted once per shard on the first observation, so it never exceeds
    /// `shard_count` and makes a permanently degraded process observable.
    shard_deaths: AtomicU64,
}

/// Point-in-time copy of [`LogIngestMetrics`] for scraping. See the
/// [module docs](self) for each field's timing convention.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogIngestMetricsSnapshot {
    pub flushes_by_size: u64,
    pub flushes_by_age: u64,
    pub flushes_manual: u64,
    pub put_retries: u64,
    pub abandoned_retry_exhausted: u64,
    pub abandoned_input_rejected: u64,
    pub buffered_bytes_total: u64,
    pub buffered_records_total: u64,
    pub acks_ok: u64,
    pub acks_err: u64,
    pub stream_id_collisions: u64,
    pub shard_deaths: u64,
}

// Every `record_*` method below is written by the log shard actor and the log
// router, which land in the next part of this phase. They are exercised by
// this module's tests today, so `dead_code` would fire on a non-test build
// until those callers exist.
#[allow(dead_code)]
impl LogIngestMetrics {
    pub(crate) fn record_flush(&self, trigger: FlushTrigger) {
        let counter = match trigger {
            FlushTrigger::Size => &self.flushes_by_size,
            FlushTrigger::Age => &self.flushes_by_age,
            FlushTrigger::Manual => &self.flushes_manual,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_put_retry(&self) {
        self.put_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// A flush abandoned by retry-budget or lifetime exhaustion
    /// ([`crate::LogWriteError::Abandoned`]): a durability signal, retryable.
    pub(crate) fn record_abandoned_retry_exhausted(&self) {
        self.abandoned_retry_exhausted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A flush abandoned because the input could not be built into a durable
    /// object ([`crate::LogWriteError::SegmentBuild`]): a client signal, not
    /// retryable.
    pub(crate) fn record_abandoned_input_rejected(&self) {
        self.abandoned_input_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_buffered(&self, bytes: u64, records: u64) {
        self.buffered_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
        self.buffered_records_total
            .fetch_add(records, Ordering::Relaxed);
    }

    pub(crate) fn record_acks(&self, count: usize, ok: bool) {
        let counter = if ok { &self.acks_ok } else { &self.acks_err };
        counter.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_stream_id_collision(&self) {
        self.stream_id_collisions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_shard_death(&self) {
        self.shard_deaths.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> LogIngestMetricsSnapshot {
        LogIngestMetricsSnapshot {
            flushes_by_size: self.flushes_by_size.load(Ordering::Relaxed),
            flushes_by_age: self.flushes_by_age.load(Ordering::Relaxed),
            flushes_manual: self.flushes_manual.load(Ordering::Relaxed),
            put_retries: self.put_retries.load(Ordering::Relaxed),
            abandoned_retry_exhausted: self.abandoned_retry_exhausted.load(Ordering::Relaxed),
            abandoned_input_rejected: self.abandoned_input_rejected.load(Ordering::Relaxed),
            buffered_bytes_total: self.buffered_bytes_total.load(Ordering::Relaxed),
            buffered_records_total: self.buffered_records_total.load(Ordering::Relaxed),
            acks_ok: self.acks_ok.load(Ordering::Relaxed),
            acks_err: self.acks_err.load(Ordering::Relaxed),
            stream_id_collisions: self.stream_id_collisions.load(Ordering::Relaxed),
            shard_deaths: self.shard_deaths.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_snapshot_is_all_zeros() {
        assert_eq!(
            LogIngestMetrics::default().snapshot(),
            LogIngestMetricsSnapshot::default()
        );
    }

    /// Each `record_*` call must move exactly one counter. The test records
    /// one call against a fresh instance and compares the whole snapshot with
    /// the expected one, so an increment leaking into a second counter fails
    /// here rather than being read as a plausible number later.
    fn assert_only(record: impl FnOnce(&LogIngestMetrics), expected: LogIngestMetricsSnapshot) {
        let metrics = LogIngestMetrics::default();
        record(&metrics);
        assert_eq!(metrics.snapshot(), expected);
    }

    #[test]
    fn each_record_method_increments_only_its_own_counter() {
        assert_only(
            |m| m.record_flush(FlushTrigger::Size),
            LogIngestMetricsSnapshot {
                flushes_by_size: 1,
                ..Default::default()
            },
        );
        assert_only(
            |m| m.record_flush(FlushTrigger::Age),
            LogIngestMetricsSnapshot {
                flushes_by_age: 1,
                ..Default::default()
            },
        );
        assert_only(
            |m| m.record_flush(FlushTrigger::Manual),
            LogIngestMetricsSnapshot {
                flushes_manual: 1,
                ..Default::default()
            },
        );
        assert_only(
            LogIngestMetrics::record_put_retry,
            LogIngestMetricsSnapshot {
                put_retries: 1,
                ..Default::default()
            },
        );
        assert_only(
            LogIngestMetrics::record_abandoned_retry_exhausted,
            LogIngestMetricsSnapshot {
                abandoned_retry_exhausted: 1,
                ..Default::default()
            },
        );
        assert_only(
            LogIngestMetrics::record_abandoned_input_rejected,
            LogIngestMetricsSnapshot {
                abandoned_input_rejected: 1,
                ..Default::default()
            },
        );
        assert_only(
            LogIngestMetrics::record_stream_id_collision,
            LogIngestMetricsSnapshot {
                stream_id_collisions: 1,
                ..Default::default()
            },
        );
        assert_only(
            LogIngestMetrics::record_shard_death,
            LogIngestMetricsSnapshot {
                shard_deaths: 1,
                ..Default::default()
            },
        );
    }

    #[test]
    fn buffered_and_acks_record_their_own_pairs() {
        // record_buffered moves both buffered counters and nothing else; the
        // ack counters split by outcome.
        assert_only(
            |m| m.record_buffered(100, 3),
            LogIngestMetricsSnapshot {
                buffered_bytes_total: 100,
                buffered_records_total: 3,
                ..Default::default()
            },
        );
        assert_only(
            |m| m.record_acks(2, true),
            LogIngestMetricsSnapshot {
                acks_ok: 2,
                ..Default::default()
            },
        );
        assert_only(
            |m| m.record_acks(1, false),
            LogIngestMetricsSnapshot {
                acks_err: 1,
                ..Default::default()
            },
        );
    }

    #[test]
    fn counters_accumulate_across_calls() {
        let metrics = LogIngestMetrics::default();
        metrics.record_flush(FlushTrigger::Age);
        metrics.record_flush(FlushTrigger::Age);
        metrics.record_buffered(10, 1);
        metrics.record_buffered(5, 2);

        let snap = metrics.snapshot();
        assert_eq!(snap.flushes_by_age, 2);
        assert_eq!(snap.buffered_bytes_total, 15);
        assert_eq!(snap.buffered_records_total, 3);
    }
}
