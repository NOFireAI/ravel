//! Regression tests for Prometheus staleness-marker handling at the PromQL
//! selector surface. A NaN-valued row must not be surfaced when the newest
//! in-window sample is a staleness marker; the series is reported absent
//! instead.
#![allow(clippy::expect_used)]

use ravel_promql::{Evaluator, ms_to_ns, testsource::TestSource};

/// Prometheus staleness marker: a NaN with this exact payload marks the end
/// of a series. When it is the most recent sample in the lookback window,
/// Prometheus reports the series as ABSENT at that instant, not as a NaN
/// value.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// An ordinary NaN payload (not the staleness marker). It is a live,
/// observable value and must pass through the evaluator bit-for-bit.
const LIVE_NAN_BITS: u64 = 0x7ff0_0000_0000_0001;

fn minutes(m: i64) -> i64 {
    m * 60_000
}

/// Instant: a series whose newest in-window sample is the staleness marker
/// returns no row for that series.
#[test]
fn stale_marker_makes_series_absent_instant() {
    let t_ms = minutes(10);
    let stale = f64::from_bits(STALE_NAN_BITS);
    // Sample sits one nanosecond inside the lookback window, so lookback
    // alone would surface it; only staleness handling should drop it.
    let ts_ns = ms_to_ns(t_ms - minutes(5)).expect("no overflow") + 1;
    let source = TestSource::new()
        .with_series(&[("__name__", "up")], &[(ts_ns, stale)])
        .expect("valid series");

    let result = Evaluator::new()
        .instant(&source, "up", t_ms)
        .expect("evaluates");

    assert!(
        result.is_empty(),
        "series whose latest in-window sample is a staleness marker must be \
         absent, but the evaluator surfaced {} row(s) with value bits {:#018x}",
        result.len(),
        result.first().map(|s| s.value.to_bits()).unwrap_or(0),
    );
}

/// A real sample newer than an earlier staleness marker supersedes it: the
/// series is present again with that live value.
#[test]
fn real_sample_after_marker_supersedes_it() {
    let t_ms = minutes(10);
    let stale = f64::from_bits(STALE_NAN_BITS);
    let stale_ts = ms_to_ns(t_ms - minutes(3)).expect("no overflow");
    let live_ts = ms_to_ns(t_ms - minutes(1)).expect("no overflow");
    let source = TestSource::new()
        .with_series(&[("__name__", "up")], &[(stale_ts, stale), (live_ts, 7.0)])
        .expect("valid series");

    let result = Evaluator::new()
        .instant(&source, "up", t_ms)
        .expect("evaluates");

    assert_eq!(result.len(), 1, "live sample after a marker must surface");
    assert_eq!(result[0].value, 7.0);
}

/// Non-goal guard: every other NaN payload is a live value and must pass
/// through bit-exactly. Detection is an exact-bits check, never `is_nan()`.
#[test]
fn non_marker_nan_passes_through_bit_exactly() {
    let t_ms = minutes(10);
    let live_nan = f64::from_bits(LIVE_NAN_BITS);
    let ts_ns = ms_to_ns(t_ms - minutes(1)).expect("no overflow");
    let source = TestSource::new()
        .with_series(&[("__name__", "up")], &[(ts_ns, live_nan)])
        .expect("valid series");

    let result = Evaluator::new()
        .instant(&source, "up", t_ms)
        .expect("evaluates");

    assert_eq!(result.len(), 1, "a non-marker NaN is a live value");
    assert_eq!(
        result[0].value.to_bits(),
        LIVE_NAN_BITS,
        "non-marker NaN payload must pass through bit-for-bit",
    );
}

/// Range: a range query excludes marker samples from its per-step
/// windows. Steps whose newest in-window sample is a marker yield no point;
/// steps that reach a live sample do.
#[test]
fn range_query_excludes_marker_steps() {
    let stale = f64::from_bits(STALE_NAN_BITS);
    // Live sample at t=1m, staleness marker at t=6m. Query every minute from
    // 0m..=10m with a 5m lookback.
    let live_ts = ms_to_ns(minutes(1)).expect("no overflow");
    let marker_ts = ms_to_ns(minutes(6)).expect("no overflow");
    let source = TestSource::new()
        .with_series(&[("__name__", "up")], &[(live_ts, 5.0), (marker_ts, stale)])
        .expect("valid series");

    let result = Evaluator::new()
        .range(&source, "up", minutes(0), minutes(10), minutes(1))
        .expect("evaluates");

    assert_eq!(result.len(), 1, "one matched series");
    let (_, samples) = &result[0];

    // Steps 1m..=5m see the live sample (5m lookback keeps 1m in window at
    // step 5m: 5m - 5m = 0m < 1m). Steps 6m..=10m select the marker and are
    // dropped; step 0m predates the live sample. So exactly steps 1..=5.
    let step_minutes: Vec<i64> = samples
        .iter()
        .map(|s| s.ts_ns / (60 * 1_000_000_000))
        .collect();
    assert_eq!(step_minutes, vec![1, 2, 3, 4, 5]);
    assert!(
        samples.iter().all(|s| s.value == 5.0),
        "no surfaced sample may carry the marker value",
    );
}
