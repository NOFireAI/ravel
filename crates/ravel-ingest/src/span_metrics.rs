//! Self-observability counters for the span pipeline, the span-side
//! counterpart of [`crate::LogIngestMetrics`].
//!
//! # Counting convention
//!
//! Identical to [`crate::log_metrics`]'s, and worth restating because mixing
//! the timing conventions up misreads the numbers. Every counter here is a
//! monotonic process-global total with **no per-shard and no per-tenant
//! dimension**: a single [`SpanIngestMetrics`] is constructed once by the span
//! router and shared by every span shard actor through an `Arc`, so a value is
//! the sum across all shards and all tenants of this process.
//!
//! - **Attempt-time.** [`record_flush`](SpanIngestMetrics::record_flush) fires
//!   when a flush is *opened*, before the RSPAN build, the data-object PUT, or
//!   the commit-record PUT. A flush later abandoned is counted in both
//!   `flushes_by_*` **and** one of the `abandoned_*` counters.
//! - **Success-time.** `acks_ok`/`acks_err` are recorded when a flush's strict
//!   waiters are acked, i.e. at the flush's terminal outcome. They count
//!   strict-mode waiters only.
//!
//! One field name differs from [`crate::LogIngestMetricsSnapshot`]'s, because
//! the unit differs: `buffered_spans_total`. There is no
//! `stream_id_collisions` counterpart: spans derive no identity that could
//! collide (see [`crate::SpanWriteError`]).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::FlushTrigger;

#[derive(Debug, Default)]
pub struct SpanIngestMetrics {
    /// Flushes opened because the tenant buffer reached `target_bytes`.
    /// Attempt-time: incremented at flush open, so it includes flushes later
    /// abandoned.
    flushes_by_size: AtomicU64,
    /// Flushes opened because the tenant buffer aged past `max_flush_delay`.
    /// Attempt-time, same as `flushes_by_size`.
    flushes_by_age: AtomicU64,
    /// Flushes opened by any [`FlushTrigger::Manual`] path: an explicit flush
    /// request, the shutdown drain, and the channel-close drop-path drain.
    flushes_manual: AtomicU64,
    /// Retried PUT attempts across both the data-object and commit-record
    /// paths. Excludes each path's first attempt.
    put_retries: AtomicU64,
    /// Flushes abandoned because a PUT exhausted its retry budget or
    /// `max_flush_lifetime` elapsed first
    /// ([`crate::SpanWriteError::Abandoned`]). A durability signal: the input
    /// was fine, the object store did not accept it in time.
    abandoned_retry_exhausted: AtomicU64,
    /// Flushes abandoned because the input could not be turned into a durable
    /// object at all: the RSPAN build, data-key derivation, or commit-record
    /// build failed ([`crate::SpanWriteError::SegmentBuild`]). A client
    /// signal: identical input will fail again.
    abandoned_input_rejected: AtomicU64,
    /// Cumulative bytes admitted into shard buffers at enqueue time.
    buffered_bytes_total: AtomicU64,
    /// Cumulative span count admitted into shard buffers at enqueue time.
    buffered_spans_total: AtomicU64,
    /// Strict-mode waiters acked with a commit token (success-time).
    acks_ok: AtomicU64,
    /// Strict-mode waiters acked with a [`crate::SpanWriteError`]
    /// (success-time).
    acks_err: AtomicU64,
    /// Distinct span shard actors observed dead by the router. Counted once
    /// per shard on the first observation, so it never exceeds `shard_count`.
    shard_deaths: AtomicU64,
    /// Flushes failed closed on a stale provisioning view (ADR-0052 section 3),
    /// the span-pipeline counterpart of `IngestMetrics::stale_provisioning_flushes`.
    stale_provisioning_flushes: AtomicU64,
    /// Flushes routed on a last-known-good provisioning view inside the
    /// bounded NF-2 grace window (ADR-0052 degraded-safe fallback), the
    /// span-pipeline counterpart of `IngestMetrics::grace_extended_stale_flushes`.
    grace_extended_stale_flushes: AtomicU64,
}

/// Point-in-time copy of [`SpanIngestMetrics`] for scraping. See the
/// [module docs](self) for each field's timing convention.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpanIngestMetricsSnapshot {
    pub flushes_by_size: u64,
    pub flushes_by_age: u64,
    pub flushes_manual: u64,
    pub put_retries: u64,
    pub abandoned_retry_exhausted: u64,
    pub abandoned_input_rejected: u64,
    pub buffered_bytes_total: u64,
    pub buffered_spans_total: u64,
    pub acks_ok: u64,
    pub acks_err: u64,
    pub shard_deaths: u64,
    pub stale_provisioning_flushes: u64,
    pub grace_extended_stale_flushes: u64,
}

impl SpanIngestMetrics {
    pub(crate) fn record_flush(&self, trigger: FlushTrigger) {
        let counter = match trigger {
            FlushTrigger::Size => &self.flushes_by_size,
            // The span shard actor has no adaptive-delay trigger of its own
            // (ADR-0067 decisions 1-3 scope to the metrics pipeline only);
            // this arm exists only so the shared `FlushTrigger` enum stays
            // exhaustive here, and is never reached from this actor.
            FlushTrigger::Age | FlushTrigger::AgeAdaptive => &self.flushes_by_age,
            FlushTrigger::Manual => &self.flushes_manual,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_put_retry(&self) {
        self.put_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// A flush abandoned by retry-budget or lifetime exhaustion
    /// ([`crate::SpanWriteError::Abandoned`]): a durability signal, retryable.
    pub(crate) fn record_abandoned_retry_exhausted(&self) {
        self.abandoned_retry_exhausted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A flush abandoned because the input could not be built into a durable
    /// object ([`crate::SpanWriteError::SegmentBuild`]): a client signal, not
    /// retryable.
    pub(crate) fn record_abandoned_input_rejected(&self) {
        self.abandoned_input_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_buffered(&self, bytes: u64, spans: u64) {
        self.buffered_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
        self.buffered_spans_total
            .fetch_add(spans, Ordering::Relaxed);
    }

    pub(crate) fn record_acks(&self, count: usize, ok: bool) {
        let counter = if ok { &self.acks_ok } else { &self.acks_err };
        counter.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_shard_death(&self) {
        self.shard_deaths.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_stale_provisioning_flush(&self) {
        self.stale_provisioning_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One flush routed on a last-known-good provisioning view inside the
    /// bounded NF-2 grace window (ADR-0052 degraded-safe fallback).
    pub(crate) fn record_grace_extended_stale_flush(&self) {
        self.grace_extended_stale_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> SpanIngestMetricsSnapshot {
        SpanIngestMetricsSnapshot {
            flushes_by_size: self.flushes_by_size.load(Ordering::Relaxed),
            flushes_by_age: self.flushes_by_age.load(Ordering::Relaxed),
            flushes_manual: self.flushes_manual.load(Ordering::Relaxed),
            put_retries: self.put_retries.load(Ordering::Relaxed),
            abandoned_retry_exhausted: self.abandoned_retry_exhausted.load(Ordering::Relaxed),
            abandoned_input_rejected: self.abandoned_input_rejected.load(Ordering::Relaxed),
            buffered_bytes_total: self.buffered_bytes_total.load(Ordering::Relaxed),
            buffered_spans_total: self.buffered_spans_total.load(Ordering::Relaxed),
            acks_ok: self.acks_ok.load(Ordering::Relaxed),
            acks_err: self.acks_err.load(Ordering::Relaxed),
            shard_deaths: self.shard_deaths.load(Ordering::Relaxed),
            stale_provisioning_flushes: self.stale_provisioning_flushes.load(Ordering::Relaxed),
            grace_extended_stale_flushes: self.grace_extended_stale_flushes.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_snapshot_is_all_zeros() {
        assert_eq!(
            SpanIngestMetrics::default().snapshot(),
            SpanIngestMetricsSnapshot::default()
        );
    }

    /// Each `record_*` call must move exactly one counter. The test records
    /// one call against a fresh instance and compares the whole snapshot with
    /// the expected one, so an increment leaking into a second counter fails
    /// here rather than being read as a plausible number later.
    fn assert_only(record: impl FnOnce(&SpanIngestMetrics), expected: SpanIngestMetricsSnapshot) {
        let metrics = SpanIngestMetrics::default();
        record(&metrics);
        assert_eq!(metrics.snapshot(), expected);
    }

    #[test]
    fn each_record_method_increments_only_its_own_counter() {
        assert_only(
            |m| m.record_flush(FlushTrigger::Size),
            SpanIngestMetricsSnapshot {
                flushes_by_size: 1,
                ..Default::default()
            },
        );
        assert_only(
            |m| m.record_flush(FlushTrigger::Age),
            SpanIngestMetricsSnapshot {
                flushes_by_age: 1,
                ..Default::default()
            },
        );
        assert_only(
            |m| m.record_flush(FlushTrigger::Manual),
            SpanIngestMetricsSnapshot {
                flushes_manual: 1,
                ..Default::default()
            },
        );
        assert_only(
            SpanIngestMetrics::record_put_retry,
            SpanIngestMetricsSnapshot {
                put_retries: 1,
                ..Default::default()
            },
        );
        assert_only(
            SpanIngestMetrics::record_abandoned_retry_exhausted,
            SpanIngestMetricsSnapshot {
                abandoned_retry_exhausted: 1,
                ..Default::default()
            },
        );
        assert_only(
            SpanIngestMetrics::record_abandoned_input_rejected,
            SpanIngestMetricsSnapshot {
                abandoned_input_rejected: 1,
                ..Default::default()
            },
        );
        assert_only(
            SpanIngestMetrics::record_shard_death,
            SpanIngestMetricsSnapshot {
                shard_deaths: 1,
                ..Default::default()
            },
        );
    }

    #[test]
    fn buffered_and_acks_record_their_own_pairs() {
        assert_only(
            |m| m.record_buffered(100, 3),
            SpanIngestMetricsSnapshot {
                buffered_bytes_total: 100,
                buffered_spans_total: 3,
                ..Default::default()
            },
        );
        assert_only(
            |m| m.record_acks(2, true),
            SpanIngestMetricsSnapshot {
                acks_ok: 2,
                ..Default::default()
            },
        );
        assert_only(
            |m| m.record_acks(1, false),
            SpanIngestMetricsSnapshot {
                acks_err: 1,
                ..Default::default()
            },
        );
    }

    #[test]
    fn counters_accumulate_across_calls() {
        let metrics = SpanIngestMetrics::default();
        metrics.record_flush(FlushTrigger::Age);
        metrics.record_flush(FlushTrigger::Age);
        metrics.record_buffered(10, 1);
        metrics.record_buffered(5, 2);

        let snap = metrics.snapshot();
        assert_eq!(snap.flushes_by_age, 2);
        assert_eq!(snap.buffered_bytes_total, 15);
        assert_eq!(snap.buffered_spans_total, 3);
    }
}
