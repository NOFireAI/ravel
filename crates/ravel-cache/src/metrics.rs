//! Process-global counters for one [`crate::Cache`], flat `AtomicU64`s
//! behind an `Arc`, in the shape `StoreMetrics`/`StoreMetrics::snapshot`
//! use in `ravel-object-store::instrument`: no per-key dimension, a
//! [`CacheMetrics::snapshot`] that is a scrape rather than a consistent
//! cut across fields, and no dependency on `ravel-types` or
//! `QueryAccounting` (a later task wires this crate's counters into
//! `QueryAccounting`; this crate does not reference it).

use std::sync::atomic::{AtomicU64, Ordering};

/// Shared counters for one `Cache`, held behind an `Arc` and cloned out
/// with [`crate::Cache::metrics`] so a scrape path can read them without
/// touching the cache itself.
#[derive(Debug, Default)]
pub struct CacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    bytes_served: AtomicU64,
    bytes_admitted: AtomicU64,
    admissions_rejected_size: AtomicU64,
    evictions: AtomicU64,
    single_flight_collapses: AtomicU64,
    disk_errors_degraded_to_misses: AtomicU64,
    disk_entries_expired_max_age: AtomicU64,
}

impl CacheMetrics {
    pub(crate) fn record_hit(&self, bytes: u64) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.bytes_served.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_admission(&self, bytes: u64) {
        self.bytes_admitted.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_rejected_size(&self) {
        self.admissions_rejected_size
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_collapse(&self) {
        self.single_flight_collapses.fetch_add(1, Ordering::Relaxed);
    }

    /// A disk-tier read found an entry at its canonical path but discarded it
    /// (short read, bad header, key mismatch, oversize, truncated payload, or
    /// a failed crc32c check) rather than a clean "nothing at this path"
    /// miss. Distinct from [`Self::record_miss`], which every discarded read
    /// still also records: this counter exists to separate "the cache never
    /// had it" from "the disk tier is unhealthy," the latter being operable.
    pub(crate) fn record_disk_error(&self) {
        self.disk_errors_degraded_to_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A disk-tier read (or the startup scan) found a well-formed entry whose
    /// stamped write time is older than the configured max-age and dropped it
    /// rather than serving it (ADR-0064). Distinct from
    /// [`Self::record_disk_error`]: the entry was not corrupt or foreign, it
    /// simply aged out. The dropped read still records a plain miss.
    pub(crate) fn record_expired_max_age(&self) {
        self.disk_entries_expired_max_age
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Point-in-time copy of every counter. Not atomic across fields:
    /// concurrent calls may land between two loads, so a snapshot can show
    /// `hits` from a hair later than `misses`. It is a scrape, not a
    /// consistent cut, and nothing correctness-bearing reads it.
    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bytes_served: self.bytes_served.load(Ordering::Relaxed),
            bytes_admitted: self.bytes_admitted.load(Ordering::Relaxed),
            admissions_rejected_size: self.admissions_rejected_size.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            single_flight_collapses: self.single_flight_collapses.load(Ordering::Relaxed),
            disk_errors_degraded_to_misses: self
                .disk_errors_degraded_to_misses
                .load(Ordering::Relaxed),
            disk_entries_expired_max_age: self.disk_entries_expired_max_age.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of a whole [`CacheMetrics`]. Plain `u64`s, so it is
/// `Copy` and needs no allocation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub bytes_served: u64,
    pub bytes_admitted: u64,
    pub admissions_rejected_size: u64,
    pub evictions: u64,
    pub single_flight_collapses: u64,
    pub disk_errors_degraded_to_misses: u64,
    pub disk_entries_expired_max_age: u64,
}
