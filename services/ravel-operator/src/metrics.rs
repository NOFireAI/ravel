//! The operator's own four metrics and their hand-written Prometheus text
//! encoder (ADR-1731 decision 3).
//!
//! Four fixed samples need no registry and no label combinatorics, so this
//! does not share code with `services/ravel-server`'s encoder (whose helpers
//! are private to that crate; ADR-1731 rejected alternatives). It does reuse
//! `ravel_object_store::instrument::LATENCY_BUCKET_BOUNDS_MICROS`, the same
//! bucket bounds the server's own store-latency histogram uses, since that
//! constant is already public API of a crate this operator depends on for
//! `sys/auth` reconciliation, not server-private code.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_object_store::instrument::LATENCY_BUCKET_BOUNDS_MICROS;

/// Histogram width: one bucket per bound plus a final overflow bucket for
/// anything slower than the largest bound.
const BUCKET_COUNT: usize = LATENCY_BUCKET_BOUNDS_MICROS.len() + 1;

/// Whether a reconcile pass succeeded, for the `result` label on
/// `ravel_operator_reconciles_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileResult {
    Ok,
    Error,
}

impl ReconcileResult {
    fn label(self) -> &'static str {
        match self {
            ReconcileResult::Ok => "ok",
            ReconcileResult::Error => "error",
        }
    }
}

/// Process-lifetime reconcile counters (ADR-1731 decision 3), updated once
/// per reconcile pass by [`crate::controller::reconcile`] and rendered fresh
/// on every `/metrics` scrape. All fields are relaxed atomics: this is a
/// scrape counter, not a consistency boundary, matching
/// `services/ravel-server`'s own metrics.
#[derive(Debug, Default)]
pub struct ReconcileMetrics {
    reconciles_ok: AtomicU64,
    reconciles_error: AtomicU64,
    /// Unix seconds of the last successful reconcile, zero until the first
    /// one (ADR-1731 decision 3).
    last_success_unix_seconds: AtomicU64,
    duration_buckets: [AtomicU64; BUCKET_COUNT],
    duration_nanos_total: AtomicU64,
}

impl ReconcileMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one completed reconcile pass: bumps the `result`-labelled
    /// counter, folds `elapsed` into the duration histogram, and on success
    /// advances the last-success timestamp to `now_unix_seconds`.
    pub fn record(&self, result: ReconcileResult, elapsed: Duration, now_unix_seconds: u64) {
        match result {
            ReconcileResult::Ok => {
                self.reconciles_ok.fetch_add(1, Ordering::Relaxed);
                self.last_success_unix_seconds
                    .store(now_unix_seconds, Ordering::Relaxed);
            }
            ReconcileResult::Error => {
                self.reconciles_error.fetch_add(1, Ordering::Relaxed);
            }
        }
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let bucket = LATENCY_BUCKET_BOUNDS_MICROS
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(BUCKET_COUNT - 1);
        self.duration_buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.duration_nanos_total.fetch_add(
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn cumulative_duration_buckets(&self) -> [u64; BUCKET_COUNT] {
        let mut cumulative = [0u64; BUCKET_COUNT];
        let mut running = 0u64;
        for (i, bucket) in self.duration_buckets.iter().enumerate() {
            running += bucket.load(Ordering::Relaxed);
            cumulative[i] = running;
        }
        cumulative
    }
}

/// One histogram bucket's `le` value: the bound in seconds for a real bound,
/// `+Inf` for the overflow bucket.
fn bucket_le(index: usize) -> String {
    match LATENCY_BUCKET_BOUNDS_MICROS.get(index) {
        Some(bound_micros) => format!("{}", *bound_micros as f64 / 1_000_000.0),
        None => "+Inf".to_string(),
    }
}

fn write_header(out: &mut String, name: &str, help: &str, kind: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(kind);
    out.push('\n');
}

/// Render the operator's four metrics as a Prometheus text-format exposition
/// (`text/plain; version=0.0.4`), the body [`crate::health`] serves on
/// `/metrics`.
pub fn render(metrics: &ReconcileMetrics, watched_clusters: usize) -> String {
    let mut out = String::new();

    write_header(
        &mut out,
        "ravel_operator_reconciles_total",
        "Completed reconcile passes, by result.",
        "counter",
    );
    for result in [ReconcileResult::Ok, ReconcileResult::Error] {
        let count = match result {
            ReconcileResult::Ok => metrics.reconciles_ok.load(Ordering::Relaxed),
            ReconcileResult::Error => metrics.reconciles_error.load(Ordering::Relaxed),
        };
        out.push_str(&format!(
            "ravel_operator_reconciles_total{{result=\"{}\"}} {count}\n",
            result.label()
        ));
    }

    write_header(
        &mut out,
        "ravel_operator_reconcile_duration_seconds",
        "Reconcile pass duration.",
        "histogram",
    );
    let cumulative = metrics.cumulative_duration_buckets();
    for (i, count) in cumulative.iter().enumerate() {
        out.push_str(&format!(
            "ravel_operator_reconcile_duration_seconds_bucket{{le=\"{}\"}} {count}\n",
            bucket_le(i)
        ));
    }
    out.push_str(&format!(
        "ravel_operator_reconcile_duration_seconds_sum {}\n",
        metrics.duration_nanos_total.load(Ordering::Relaxed) as f64 / 1_000_000_000.0
    ));
    out.push_str(&format!(
        "ravel_operator_reconcile_duration_seconds_count {}\n",
        cumulative[BUCKET_COUNT - 1]
    ));

    write_header(
        &mut out,
        "ravel_operator_last_successful_reconcile_timestamp_seconds",
        "Unix time of the last successful reconcile, zero until the first one.",
        "gauge",
    );
    out.push_str(&format!(
        "ravel_operator_last_successful_reconcile_timestamp_seconds {}\n",
        metrics.last_success_unix_seconds.load(Ordering::Relaxed)
    ));

    write_header(
        &mut out,
        "ravel_operator_watched_clusters",
        "RavelCluster objects in the operator's reflector store.",
        "gauge",
    );
    out.push_str(&format!(
        "ravel_operator_watched_clusters {watched_clusters}\n"
    ));

    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn record_ok_bumps_ok_counter_and_last_success() {
        let metrics = ReconcileMetrics::new();
        metrics.record(ReconcileResult::Ok, Duration::from_millis(5), 1_000);
        let body = render(&metrics, 0);
        assert!(body.contains("ravel_operator_reconciles_total{result=\"ok\"} 1"));
        assert!(body.contains("ravel_operator_reconciles_total{result=\"error\"} 0"));
        assert!(body.contains("ravel_operator_last_successful_reconcile_timestamp_seconds 1000"));
    }

    #[test]
    fn record_error_does_not_advance_last_success() {
        let metrics = ReconcileMetrics::new();
        metrics.record(ReconcileResult::Ok, Duration::from_millis(1), 500);
        metrics.record(ReconcileResult::Error, Duration::from_millis(1), 999);
        let body = render(&metrics, 0);
        assert!(body.contains("ravel_operator_reconciles_total{result=\"error\"} 1"));
        assert!(body.contains("ravel_operator_last_successful_reconcile_timestamp_seconds 500"));
    }

    #[test]
    fn duration_count_matches_total_reconciles() {
        let metrics = ReconcileMetrics::new();
        metrics.record(ReconcileResult::Ok, Duration::from_secs(10), 1);
        metrics.record(ReconcileResult::Error, Duration::from_millis(1), 2);
        let body = render(&metrics, 3);
        assert!(body.contains("ravel_operator_reconcile_duration_seconds_count 2"));
        assert!(body.contains("ravel_operator_watched_clusters 3"));
    }
}
