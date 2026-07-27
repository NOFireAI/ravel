//! Self-observability counters (docs/ingest.md "Metrics"). Plain atomics for
//! now; scraping/otel export is a later task.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushTrigger {
    Size,
    Age,
    Manual,
}

#[derive(Debug, Default)]
pub struct IngestMetrics {
    flushes_by_size: AtomicU64,
    flushes_by_age: AtomicU64,
    flushes_manual: AtomicU64,
    put_retries: AtomicU64,
    abandoned_flushes: AtomicU64,
    buffered_bytes_total: AtomicU64,
    buffered_points_total: AtomicU64,
    acks_ok: AtomicU64,
    acks_err: AtomicU64,
    /// Batches rejected because two points shared a `series_id` under
    /// distinct canonical label sets (ADR-0005 fail-loud collision check).
    series_id_collisions: AtomicU64,
}

/// Point-in-time copy of [`IngestMetrics`] for scraping.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestMetricsSnapshot {
    pub flushes_by_size: u64,
    pub flushes_by_age: u64,
    pub flushes_manual: u64,
    pub put_retries: u64,
    pub abandoned_flushes: u64,
    pub buffered_bytes_total: u64,
    pub buffered_points_total: u64,
    pub acks_ok: u64,
    pub acks_err: u64,
    pub series_id_collisions: u64,
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

    pub(crate) fn record_abandoned(&self) {
        self.abandoned_flushes.fetch_add(1, Ordering::Relaxed);
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

    pub fn snapshot(&self) -> IngestMetricsSnapshot {
        IngestMetricsSnapshot {
            flushes_by_size: self.flushes_by_size.load(Ordering::Relaxed),
            flushes_by_age: self.flushes_by_age.load(Ordering::Relaxed),
            flushes_manual: self.flushes_manual.load(Ordering::Relaxed),
            put_retries: self.put_retries.load(Ordering::Relaxed),
            abandoned_flushes: self.abandoned_flushes.load(Ordering::Relaxed),
            buffered_bytes_total: self.buffered_bytes_total.load(Ordering::Relaxed),
            buffered_points_total: self.buffered_points_total.load(Ordering::Relaxed),
            acks_ok: self.acks_ok.load(Ordering::Relaxed),
            acks_err: self.acks_err.load(Ordering::Relaxed),
            series_id_collisions: self.series_id_collisions.load(Ordering::Relaxed),
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
        metrics.record_abandoned();
        metrics.record_buffered(100, 3);
        metrics.record_acks(2, true);
        metrics.record_acks(1, false);
        metrics.record_series_id_collision();

        let snap = metrics.snapshot();
        assert_eq!(snap.flushes_by_size, 1);
        assert_eq!(snap.flushes_by_age, 2);
        assert_eq!(snap.flushes_manual, 1);
        assert_eq!(snap.put_retries, 1);
        assert_eq!(snap.abandoned_flushes, 1);
        assert_eq!(snap.buffered_bytes_total, 100);
        assert_eq!(snap.buffered_points_total, 3);
        assert_eq!(snap.acks_ok, 2);
        assert_eq!(snap.acks_err, 1);
        assert_eq!(snap.series_id_collisions, 1);
    }
}
