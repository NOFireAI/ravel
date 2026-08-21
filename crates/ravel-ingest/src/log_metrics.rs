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

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use ravel_types::TenantHash;

use crate::attribution::TenantPutAttribution;
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
    /// `RlogWriter::finish()` performs).
    stream_id_collisions: AtomicU64,
    /// Distinct log shard actors observed dead by the router: its send half
    /// or a strict-mode ack found the shard channel closed, meaning the actor
    /// task ended (e.g. panicked) without the router shutting it down.
    /// Counted once per shard on the first observation, so it never exceeds
    /// `shard_count` and makes a permanently degraded process observable.
    shard_deaths: AtomicU64,
    /// Flushes failed closed on a stale provisioning view (ADR-0052 section 3),
    /// the log-pipeline counterpart of `IngestMetrics::stale_provisioning_flushes`.
    stale_provisioning_flushes: AtomicU64,
    /// Flushes routed on a last-known-good provisioning view past the refresh
    /// interval `C`, inside the bounded grace window, because the
    /// provisioning re-read could not complete but the cached view's validity
    /// horizon had not been crossed (`GenerationSwitch::try_grace_extend`).
    /// Degraded, not failed: distinct from `stale_provisioning_flushes`, which
    /// counts a flush that failed closed outright. A sustained rise here means
    /// the store is slow/throttled and this router is degraded-but-available
    /// rather than fleet-wide-outed; the log-pipeline counterpart of
    /// `IngestMetrics::grace_extended_stale_flushes`.
    grace_extended_stale_flushes: AtomicU64,
    /// Flushes whose per-tenant indexed-field list resolved from a stale cached
    /// value or a failed-re-read/validation fallback rather than a fresh durable
    /// `TenantConfig` read this tick (ADR-0079 deliverable 6). Degraded, not
    /// failed: the flush still indexes on the last-known-good or CLI-only list.
    /// A sustained rise means a tenant's durable override is unreadable or
    /// malformed and its override is silently not applying -- the exact
    /// silent-gap class ADR-0079 exists to close, so it must be visible. Mirrors
    /// `grace_extended_stale_flushes` (a snapshot-only counter, read via
    /// [`LogIngestMetrics::snapshot`]).
    indexed_fields_stale_fallbacks: AtomicU64,
    /// Objects flushed carrying a non-empty POSTINGS section (ADR-0049). The denominator for average section bytes per indexed object; an
    /// object whose resolved indexed-field list produced no section is not
    /// counted here.
    postings_objects: AtomicU64,
    /// Cumulative encoded POSTINGS section bytes across every flushed object.
    postings_bytes_total: AtomicU64,
    /// Cumulative count of indexed fields that emitted a posting list, summed
    /// over objects. The denominator for a mean distinct-per-field
    /// (`postings_distinct_values_total / postings_indexed_fields_total`); no
    /// per-field label is kept, which the ADR-0044 allowlist forbids.
    postings_indexed_fields_total: AtomicU64,
    /// Cumulative distinct-value count across every non-capped indexed field,
    /// summed over objects.
    postings_distinct_values_total: AtomicU64,
    /// Indexed fields dropped from POSTINGS for exceeding the per-field
    /// distinct-value cap (ADR-0049 decision 4), summed over objects.
    postings_capped_fields_total: AtomicU64,
    /// Distinct `(name, type)` pairs that received a real dynamic column,
    /// summed over flushed objects (ADR-0100 decision 1).
    dynamic_columns_used_total: AtomicU64,
    /// Distinct `(name, type)` pairs that overflowed the `max_dynamic_columns`
    /// budget and folded into `attrs_raw`, summed over flushed objects
    /// (ADR-0100 decision 1). A sustained rise means loads are silently losing
    /// columnar access to attributes past the budget.
    dynamic_columns_overflowed_total: AtomicU64,
    /// Largest `dynamic_columns_used` of any single flushed object seen so far
    /// (a running maximum, not a sum). A total cannot show budget pressure
    /// before the cap is crossed; this maximum can, so an operator sees a load
    /// approaching `max_dynamic_columns` before any object overflows it
    /// (ADR-0100 decision 1).
    dynamic_columns_used_max: AtomicU64,
    /// Per-shard count of flushes whose flush task has been spawned but has
    /// not yet acked its waiters (ADR-0067 decisions 1-2, the log-pipeline
    /// counterpart of [`crate::IngestMetrics`]'s own gauge). Keyed by shard
    /// index; a shard with no flush in flight has no entry, equivalent to 0.
    /// Not part of [`LogIngestMetricsSnapshot`]'s flat counters because it is a
    /// gauge with a per-shard dimension, unlike everything else here; read it
    /// via [`LogIngestMetrics::in_flight_flushes_by_shard`].
    in_flight_flushes: Mutex<HashMap<u32, i64>>,
    /// Bounded-cardinality per-tenant PUT attribution (ADR-0076 decision 2),
    /// the log-pipeline counterpart of [`crate::IngestMetrics`]'s own. Carries
    /// a per-tenant dimension, so it stays bounded by a top-K cap rather than
    /// an unbounded label, and is read via
    /// [`LogIngestMetrics::tenant_put_attribution`] rather than folded into the
    /// `Copy` snapshot. See [`crate::attribution`] for the policy.
    put_attribution: TenantPutAttribution,
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
    pub stale_provisioning_flushes: u64,
    pub grace_extended_stale_flushes: u64,
    pub indexed_fields_stale_fallbacks: u64,
    pub postings_objects: u64,
    pub postings_bytes_total: u64,
    pub postings_indexed_fields_total: u64,
    pub postings_distinct_values_total: u64,
    pub postings_capped_fields_total: u64,
    pub dynamic_columns_used_total: u64,
    pub dynamic_columns_overflowed_total: u64,
    pub dynamic_columns_used_max: u64,
}

impl LogIngestMetrics {
    pub(crate) fn record_flush(&self, trigger: FlushTrigger) {
        let counter = match trigger {
            FlushTrigger::Size => &self.flushes_by_size,
            // The log shard actor has no adaptive-delay trigger of its own
            // (ADR-0067 decisions 1-3 scope to the metrics pipeline only);
            // this arm exists only so the shared `FlushTrigger` enum stays
            // exhaustive here, and is never reached from this actor.
            FlushTrigger::Age | FlushTrigger::AgeAdaptive => &self.flushes_by_age,
            FlushTrigger::Manual => &self.flushes_manual,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Attribute one completed flush's PUTs to `tenant` (success-time,
    /// ADR-0076 decision 2). Called from the terminal success path of
    /// `log_shard.rs`'s `run_flush`, after both PUTs have landed.
    pub(crate) fn record_flush_puts(&self, tenant: TenantHash) {
        self.put_attribution.record_flush(tenant);
    }

    /// The bounded-cardinality per-tenant PUT attribution (ADR-0076 decision
    /// 2). Reachable from `LogIngestRouter` through
    /// [`crate::LogIngestRouter::metrics`].
    pub fn tenant_put_attribution(&self) -> &TenantPutAttribution {
        &self.put_attribution
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

    /// Adjusts shard `shard`'s in-flight-flush gauge by `delta` (+1 when a
    /// flush task is spawned, -1 when it ends, including on panic via
    /// `log_shard`'s `InFlightFlushGuard`). Poison recovery rather than a panic
    /// on a poisoned lock: a gauge is best-effort self-observability, not a
    /// durability path, so a prior panicked holder must not take this one down
    /// with it.
    pub(crate) fn record_inflight_flush_delta(&self, shard: u32, delta: i64) {
        let mut map = self
            .in_flight_flushes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = map.entry(shard).or_insert(0);
        *entry += delta;
    }

    /// Point-in-time per-shard in-flight-flush counts, sorted by shard index.
    /// A shard with none in flight is simply absent, equivalent to 0.
    pub fn in_flight_flushes_by_shard(&self) -> Vec<(u32, u64)> {
        let map = self
            .in_flight_flushes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut counts: Vec<(u32, u64)> = map
            .iter()
            .map(|(&shard, &count)| (shard, count.max(0) as u64))
            .collect();
        counts.sort_unstable_by_key(|&(shard, _)| shard);
        counts
    }

    pub(crate) fn record_stale_provisioning_flush(&self) {
        self.stale_provisioning_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_grace_extended_stale_flush(&self) {
        self.grace_extended_stale_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One flush resolved its indexed-field list from a stale cached value or a
    /// failed-re-read/validation fallback rather than a fresh durable read
    /// (ADR-0079 deliverable 6). Called from `run_flush` on the overlay's
    /// fallback arm, mirroring [`Self::record_grace_extended_stale_flush`].
    pub(crate) fn record_indexed_fields_stale_fallback(&self) {
        self.indexed_fields_stale_fallbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Fold one flushed object's write-side POSTINGS counters
    /// ([`ravel_logseg::writer::WriteStats`]) into the cumulative totals
    /// (ADR-0049). An object with no POSTINGS section
    /// (`postings_bytes == 0`) still records here: it moves no counter, so an
    /// unindexed tenant leaves every total untouched. Unlike the single-counter
    /// `record_*` methods this moves several counters at once, one per field of
    /// the stats.
    pub(crate) fn record_postings(&self, stats: ravel_logseg::writer::WriteStats) {
        if stats.postings_bytes > 0 {
            self.postings_objects.fetch_add(1, Ordering::Relaxed);
        }
        self.postings_bytes_total
            .fetch_add(stats.postings_bytes, Ordering::Relaxed);
        self.postings_indexed_fields_total
            .fetch_add(u64::from(stats.postings_indexed_fields), Ordering::Relaxed);
        self.postings_distinct_values_total
            .fetch_add(stats.postings_distinct_total, Ordering::Relaxed);
        self.postings_capped_fields_total
            .fetch_add(u64::from(stats.postings_capped_fields), Ordering::Relaxed);
        // Dynamic-column budget (ADR-0100 decision 1). Folded for every object,
        // not only POSTINGS-carrying ones: an object with no indexed field
        // still assigns dynamic columns and can overflow the budget. The used
        // count also feeds a running maximum, which shows budget pressure
        // before any object crosses the cap.
        self.dynamic_columns_used_total
            .fetch_add(u64::from(stats.dynamic_columns_used), Ordering::Relaxed);
        self.dynamic_columns_overflowed_total.fetch_add(
            u64::from(stats.dynamic_columns_overflowed),
            Ordering::Relaxed,
        );
        self.dynamic_columns_used_max
            .fetch_max(u64::from(stats.dynamic_columns_used), Ordering::Relaxed);
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
            stale_provisioning_flushes: self.stale_provisioning_flushes.load(Ordering::Relaxed),
            grace_extended_stale_flushes: self.grace_extended_stale_flushes.load(Ordering::Relaxed),
            indexed_fields_stale_fallbacks: self
                .indexed_fields_stale_fallbacks
                .load(Ordering::Relaxed),
            postings_objects: self.postings_objects.load(Ordering::Relaxed),
            postings_bytes_total: self.postings_bytes_total.load(Ordering::Relaxed),
            postings_indexed_fields_total: self
                .postings_indexed_fields_total
                .load(Ordering::Relaxed),
            postings_distinct_values_total: self
                .postings_distinct_values_total
                .load(Ordering::Relaxed),
            postings_capped_fields_total: self.postings_capped_fields_total.load(Ordering::Relaxed),
            dynamic_columns_used_total: self.dynamic_columns_used_total.load(Ordering::Relaxed),
            dynamic_columns_overflowed_total: self
                .dynamic_columns_overflowed_total
                .load(Ordering::Relaxed),
            dynamic_columns_used_max: self.dynamic_columns_used_max.load(Ordering::Relaxed),
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
        assert_only(
            LogIngestMetrics::record_indexed_fields_stale_fallback,
            LogIngestMetricsSnapshot {
                indexed_fields_stale_fallbacks: 1,
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
        // record_postings folds a whole WriteStats into the postings totals and
        // touches nothing else. An object carrying a section increments the
        // object count; its distinct counts and capped-field count add through.
        assert_only(
            |m| {
                m.record_postings(ravel_logseg::writer::WriteStats {
                    postings_capped_fields: 1,
                    postings_bytes: 512,
                    postings_indexed_fields: 3,
                    postings_distinct_total: 40,
                    postings_distinct_max: 25,
                    dynamic_columns_used: 7,
                    dynamic_columns_overflowed: 2,
                })
            },
            LogIngestMetricsSnapshot {
                postings_objects: 1,
                postings_bytes_total: 512,
                postings_indexed_fields_total: 3,
                postings_distinct_values_total: 40,
                postings_capped_fields_total: 1,
                dynamic_columns_used_total: 7,
                dynamic_columns_overflowed_total: 2,
                dynamic_columns_used_max: 7,
                ..Default::default()
            },
        );
        // An object with no section (bytes 0) moves no counter, not even the
        // object count.
        assert_only(
            |m| m.record_postings(ravel_logseg::writer::WriteStats::default()),
            LogIngestMetricsSnapshot::default(),
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

    #[test]
    fn dynamic_column_counters_and_used_max_fold_into_snapshot() {
        // Two objects, the wider one first (used 8) then a narrower one (used
        // 5). The used and overflowed counts sum; the used_max keeps the larger
        // 8. A sum would report 13 and a store-latest would report 5, so the
        // fixture forces the maximum fold to be the real thing that shows budget
        // pressure a total cannot (ADR-0100 decision 1).
        let metrics = LogIngestMetrics::default();
        metrics.record_postings(ravel_logseg::writer::WriteStats {
            dynamic_columns_used: 8,
            dynamic_columns_overflowed: 3,
            ..Default::default()
        });
        metrics.record_postings(ravel_logseg::writer::WriteStats {
            dynamic_columns_used: 5,
            dynamic_columns_overflowed: 2,
            ..Default::default()
        });

        let snap = metrics.snapshot();
        assert_eq!(snap.dynamic_columns_used_total, 13, "8 + 5");
        // Both writes overflow, and by different amounts, so this sum
        // distinguishes a `fetch_add` fold from a `fetch_max` one: a maximum
        // would report 3 here.
        assert_eq!(snap.dynamic_columns_overflowed_total, 5, "3 + 2");
        assert_eq!(
            snap.dynamic_columns_used_max, 8,
            "running maximum keeps the wider object's used count"
        );
    }
}
