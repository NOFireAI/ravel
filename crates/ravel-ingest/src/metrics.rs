//! Self-observability counters (docs/ingest.md "Metrics"). Plain atomics for
//! now; scraping/otel export is a later task.
//!
//! # Counting convention
//!
//! Every counter here is a monotonic process-global total. There is **no
//! per-shard and no per-tenant dimension**: a single [`IngestMetrics`] is
//! constructed once by the router and shared by every shard actor through an
//! `Arc`, so a value is the sum across all shards and all tenants of this
//! process. The dimensioned model docs/ingest.md describes (per-shard buffered
//! bytes/points, per-shard latency histograms, per-tenant accepted/rejected
//! points) is not implemented here; that doc marks it as tracked future work.
//!
//! Two timing conventions coexist, and mixing them up misreads the numbers:
//!
//! - **Attempt-time.** [`record_flush`](IngestMetrics::record_flush) fires when
//!   a flush is *opened* (`shard.rs` `flush_tenant`), before the segment build,
//!   the data-object PUT, or the commit-record PUT. A flush that is later
//!   abandoned is therefore counted in both `flushes_by_*` **and** one of the
//!   `abandoned_*` counters. Flushes that actually reached a durable commit
//!   are the three trigger counters summed, minus `abandoned_retry_exhausted`
//!   and `abandoned_input_rejected`; the bare trigger sum overcounts.
//! - **Success-time.** `acks_ok`/`acks_err` are recorded when a flush's strict
//!   waiters are acked, i.e. at the flush's terminal outcome. They count
//!   strict-mode waiters only: a buffered-mode flush, or an age/size flush with
//!   no strict waiter attached, records zero on both. They are an ack-outcome
//!   counter, not a flush-outcome counter, despite sitting next to one.
//!
//! `flushes_manual` covers every `FlushTrigger::Manual` flush: an explicit
//! `FlushNow`, the `Shutdown` drain, and the channel-close drop-path drain
//! (`shard.rs` `run`). It is not exclusively operator-requested flushes.
//!
//! `put_retries` counts every retried PUT attempt on both the data-object and
//! the commit-record path (`put_data_object_with_retry` and
//! `publish_with_retry`); the first attempt of each is not a retry and is not
//! counted.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushTrigger {
    Size,
    Age,
    Manual,
}

#[derive(Debug, Default)]
pub struct IngestMetrics {
    /// Flushes opened because the tenant buffer reached `target_bytes`.
    /// Attempt-time: incremented at flush open, so it includes flushes later
    /// abandoned.
    flushes_by_size: AtomicU64,
    /// Flushes opened because the tenant buffer aged past `max_flush_delay`.
    /// Attempt-time, same as `flushes_by_size`.
    flushes_by_age: AtomicU64,
    /// Flushes opened by any `FlushTrigger::Manual` path: an explicit
    /// `FlushNow`, the `Shutdown` drain, or the channel-close drop-path drain.
    /// Attempt-time.
    flushes_manual: AtomicU64,
    /// Retried PUT attempts across both the data-object and commit-record
    /// paths. Excludes each path's first attempt.
    put_retries: AtomicU64,
    /// Flushes abandoned because a PUT exhausted its retry budget or
    /// `max_flush_lifetime` elapsed first (`WriteError::Abandoned`). A
    /// durability signal: the input was fine, the object store did not accept
    /// it in time. Nothing was acknowledged and the whole write stays
    /// retryable.
    abandoned_retry_exhausted: AtomicU64,
    /// Flushes abandoned because the input could not be turned into a durable
    /// object at all: the segment build, data-key derivation, or commit-record
    /// build failed (`WriteError::SegmentBuild`). A client signal: identical
    /// input will fail again, so the write is not retryable. Split out from
    /// `abandoned_retry_exhausted` because `error.rs` already treats the two
    /// causes differently (a8-F05).
    abandoned_input_rejected: AtomicU64,
    /// Cumulative bytes admitted into shard buffers at enqueue time.
    buffered_bytes_total: AtomicU64,
    /// Cumulative sample count admitted into shard buffers at enqueue time.
    buffered_points_total: AtomicU64,
    /// Strict-mode waiters acked with a commit token (success-time). Zero for
    /// buffered-mode and for flushes with no strict waiter.
    acks_ok: AtomicU64,
    /// Strict-mode waiters acked with a `WriteError` (success-time). Zero for
    /// buffered-mode and for flushes with no strict waiter.
    acks_err: AtomicU64,
    /// Batches rejected because two points shared a `series_id` under
    /// distinct canonical label sets (ADR-0005 fail-loud collision check).
    series_id_collisions: AtomicU64,
    /// Exemplars written into a flushed object's EXEMPLARS section
    /// (ADR-0047). Attempt-time, like `flushes_by_*`: counted when the flush
    /// is built, so a flush later abandoned counts here too.
    exemplars_written_total: AtomicU64,
    /// Exemplars discarded at flush rather than written: either their parent
    /// sample was not in this flush (so the object carries no measurement for
    /// them, and the writer would reject them) or they lost the flush-scoped
    /// per-series window cap. Keeps the drop visible now that some exemplars
    /// are kept (ADR-0047 decision 2), alongside the wire-side drop counters
    /// the normalize paths already report.
    exemplars_dropped_total: AtomicU64,
    /// Distinct shard actors observed dead by the router: its send half or a
    /// strict-mode ack found the shard channel closed, meaning the actor task
    /// ended (e.g. panicked) without the router shutting it down. Counted
    /// once per shard on the first observation, so it never exceeds
    /// `shard_count` and makes a permanently degraded process observable
    /// (docs/ingest.md "Metrics (self-observability)", a8-F03).
    shard_deaths: AtomicU64,
    /// Flushes failed closed because the router's cached provisioning view for
    /// the tenant was older than the refresh interval `C` (ADR-0052 section 3).
    /// The load-bearing staleness signal: a nonzero, growing value means the
    /// background refresher is not keeping views current and writes are being
    /// refused rather than routed on a possibly-missed activation.
    stale_provisioning_flushes: AtomicU64,
}

/// Point-in-time copy of [`IngestMetrics`] for scraping. See the
/// [module docs](self) for each field's timing convention.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestMetricsSnapshot {
    pub flushes_by_size: u64,
    pub flushes_by_age: u64,
    pub flushes_manual: u64,
    pub put_retries: u64,
    pub abandoned_retry_exhausted: u64,
    pub abandoned_input_rejected: u64,
    pub buffered_bytes_total: u64,
    pub buffered_points_total: u64,
    pub acks_ok: u64,
    pub acks_err: u64,
    pub series_id_collisions: u64,
    pub shard_deaths: u64,
    pub exemplars_written_total: u64,
    pub exemplars_dropped_total: u64,
    pub stale_provisioning_flushes: u64,
}

impl IngestMetrics {
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
    /// (`WriteError::Abandoned`): a durability signal, retryable.
    pub(crate) fn record_abandoned_retry_exhausted(&self) {
        self.abandoned_retry_exhausted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A flush abandoned because the input could not be built into a durable
    /// object (`WriteError::SegmentBuild`): a client signal, not retryable.
    pub(crate) fn record_abandoned_input_rejected(&self) {
        self.abandoned_input_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_buffered(&self, bytes: u64, points: u64) {
        self.buffered_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
        self.buffered_points_total
            .fetch_add(points, Ordering::Relaxed);
    }

    pub(crate) fn record_acks(&self, count: usize, ok: bool) {
        let counter = if ok { &self.acks_ok } else { &self.acks_err };
        counter.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_series_id_collision(&self) {
        self.series_id_collisions.fetch_add(1, Ordering::Relaxed);
    }

    /// One flush's exemplar outcome: `written` reached the object's EXEMPLARS
    /// section, `dropped` did not (no parent sample in the flush, or lost the
    /// flush-scoped window cap).
    pub(crate) fn record_exemplars(&self, written: u64, dropped: u64) {
        self.exemplars_written_total
            .fetch_add(written, Ordering::Relaxed);
        self.exemplars_dropped_total
            .fetch_add(dropped, Ordering::Relaxed);
    }

    pub(crate) fn record_shard_death(&self) {
        self.shard_deaths.fetch_add(1, Ordering::Relaxed);
    }

    /// One flush refused because the router's cached provisioning view for the
    /// tenant exceeded the refresh interval `C` (ADR-0052 section 3, fail
    /// closed).
    pub(crate) fn record_stale_provisioning_flush(&self) {
        self.stale_provisioning_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> IngestMetricsSnapshot {
        IngestMetricsSnapshot {
            flushes_by_size: self.flushes_by_size.load(Ordering::Relaxed),
            flushes_by_age: self.flushes_by_age.load(Ordering::Relaxed),
            flushes_manual: self.flushes_manual.load(Ordering::Relaxed),
            put_retries: self.put_retries.load(Ordering::Relaxed),
            abandoned_retry_exhausted: self.abandoned_retry_exhausted.load(Ordering::Relaxed),
            abandoned_input_rejected: self.abandoned_input_rejected.load(Ordering::Relaxed),
            buffered_bytes_total: self.buffered_bytes_total.load(Ordering::Relaxed),
            buffered_points_total: self.buffered_points_total.load(Ordering::Relaxed),
            acks_ok: self.acks_ok.load(Ordering::Relaxed),
            acks_err: self.acks_err.load(Ordering::Relaxed),
            series_id_collisions: self.series_id_collisions.load(Ordering::Relaxed),
            shard_deaths: self.shard_deaths.load(Ordering::Relaxed),
            exemplars_written_total: self.exemplars_written_total.load(Ordering::Relaxed),
            exemplars_dropped_total: self.exemplars_dropped_total.load(Ordering::Relaxed),
            stale_provisioning_flushes: self.stale_provisioning_flushes.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_recorded_counters() {
        let metrics = IngestMetrics::default();
        metrics.record_flush(FlushTrigger::Size);
        metrics.record_flush(FlushTrigger::Age);
        metrics.record_flush(FlushTrigger::Age);
        metrics.record_flush(FlushTrigger::Manual);
        metrics.record_put_retry();
        metrics.record_abandoned_retry_exhausted();
        metrics.record_buffered(100, 3);
        metrics.record_acks(2, true);
        metrics.record_acks(1, false);
        metrics.record_series_id_collision();
        metrics.record_shard_death();
        metrics.record_exemplars(2, 5);

        let snap = metrics.snapshot();
        assert_eq!(snap.exemplars_written_total, 2);
        assert_eq!(snap.exemplars_dropped_total, 5);
        assert_eq!(snap.flushes_by_size, 1);
        assert_eq!(snap.flushes_by_age, 2);
        assert_eq!(snap.flushes_manual, 1);
        assert_eq!(snap.put_retries, 1);
        assert_eq!(snap.abandoned_retry_exhausted, 1);
        assert_eq!(snap.abandoned_input_rejected, 0);
        assert_eq!(snap.buffered_bytes_total, 100);
        assert_eq!(snap.buffered_points_total, 3);
        assert_eq!(snap.acks_ok, 2);
        assert_eq!(snap.acks_err, 1);
        assert_eq!(snap.series_id_collisions, 1);
        assert_eq!(snap.shard_deaths, 1);
    }

    #[test]
    fn abandoned_causes_are_counted_separately() {
        let metrics = IngestMetrics::default();
        // Two durability abandonments (retry/lifetime exhaustion) and one
        // input rejection (segment build failed). The split lets an operator
        // tell a store problem from a bad-input problem by counter alone
        // (a8-F05), which the old single `abandoned_flushes` could not.
        metrics.record_abandoned_retry_exhausted();
        metrics.record_abandoned_retry_exhausted();
        metrics.record_abandoned_input_rejected();

        let snap = metrics.snapshot();
        assert_eq!(snap.abandoned_retry_exhausted, 2);
        assert_eq!(snap.abandoned_input_rejected, 1);
    }
}
