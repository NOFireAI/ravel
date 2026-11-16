//! Property gate for the analytics stage (ADR-0028 decision 6): no panics on
//! adversarial input, determinism, the invariances, and a bit-identical
//! summary against an independent naive reference over the ADR-0022
//! adversarial value pool.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::prelude::*;

use ravel_analytics::{
    ChangeKind, ChangePointParams, ChangePointResult, SummaryParams, SummaryResult, change_point,
    summary,
};

/// The ADR-0022 adversarial value pool (mirrors ravel-sql's
/// `interesting_value`): NaN with distinct payloads and both signs, both
/// infinities, both signed zeros, a denormal, and ordinary values.
fn interesting_value() -> BoxedStrategy<f64> {
    prop_oneof![
        Just(0.0f64),
        Just(-0.0f64),
        Just(1.0f64),
        Just(-1.0f64),
        Just(2.5f64),
        Just(-3.25f64),
        Just(1.0e16f64),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        Just(f64::from_bits(0x7ff8_0000_0000_0001)),
        Just(f64::from_bits(0x7ff8_0000_0000_00aa)),
        Just(f64::from_bits(0xfff8_0000_0000_0001)),
        Just(f64::from_bits(0x0000_0000_0000_0001)),
        Just(f64::MIN_POSITIVE),
    ]
    .boxed()
}

fn with_timestamps(values: &[f64]) -> Vec<(i64, f64)> {
    values
        .iter()
        .enumerate()
        .map(|(i, &v)| (i as i64 * 60_000_000_000, v))
        .collect()
}

/// Bit-level equality of a change-point result (NaN-safe: compares `score` by
/// bits, not value).
fn cp_bits_eq(a: &ChangePointResult, b: &ChangePointResult) -> bool {
    a.kind == b.kind
        && a.ts_ns == b.ts_ns
        && a.score.to_bits() == b.score.to_bits()
        && a.downsampled == b.downsampled
        && a.original_points == b.original_points
        && a.nan_excluded == b.nan_excluded
}

fn summary_bits_eq(a: &SummaryResult, b: &SummaryResult) -> bool {
    a.median.to_bits() == b.median.to_bits()
        && a.mad.to_bits() == b.mad.to_bits()
        && a.stddev.to_bits() == b.stddev.to_bits()
        && a.variance.to_bits() == b.variance.to_bits()
        && a.nan_excluded == b.nan_excluded
        && a.percentiles.len() == b.percentiles.len()
        && a.percentiles
            .iter()
            .zip(&b.percentiles)
            .all(|(x, y)| x.0.to_bits() == y.0.to_bits() && x.1.to_bits() == y.1.to_bits())
}

// A deterministic step series with small pseudo-random noise, std-only.
fn step_series(n: usize, level_a: f64, level_b: f64, change_idx: usize) -> Vec<f64> {
    let mut state: u64 = 0x1234_5678_9abc_def1;
    (0..n)
        .map(|i| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let u = (state >> 11) as f64 / (1u64 << 53) as f64;
            let noise = (u * 2.0 - 1.0) * 0.3;
            let base = if i < change_idx { level_a } else { level_b };
            base + noise
        })
        .collect()
}

proptest! {
    // No panic on adversarial input of any shape or size.
    #[test]
    fn change_point_never_panics(values in proptest::collection::vec(interesting_value(), 0..80)) {
        let series = with_timestamps(&values);
        let _ = change_point(&series, &ChangePointParams { downsample: false });
        let _ = change_point(&series, &ChangePointParams { downsample: true });
    }

    #[test]
    fn summary_never_panics(
        values in proptest::collection::vec(interesting_value(), 0..80),
        pcts in proptest::collection::vec(0.0f64..=1.0, 0..5),
    ) {
        let series = with_timestamps(&values);
        let _ = summary(&series, &SummaryParams { percentiles: pcts });
    }

    // Determinism: same input, bit-identical output.
    #[test]
    fn change_point_is_deterministic(
        values in proptest::collection::vec(interesting_value(), 0..80),
    ) {
        let series = with_timestamps(&values);
        let params = ChangePointParams { downsample: true };
        let a = change_point(&series, &params);
        let b = change_point(&series, &params);
        match (a, b) {
            (Ok(a), Ok(b)) => prop_assert!(cp_bits_eq(&a, &b)),
            (Err(a), Err(b)) => prop_assert_eq!(a, b),
            _ => prop_assert!(false, "one call errored and the other did not"),
        }
    }

    #[test]
    fn summary_is_deterministic(
        values in proptest::collection::vec(interesting_value(), 0..80),
        pcts in proptest::collection::vec(0.0f64..=1.0, 0..5),
    ) {
        let series = with_timestamps(&values);
        let params = SummaryParams { percentiles: pcts };
        let a = summary(&series, &params);
        let b = summary(&series, &params);
        match (a, b) {
            (Ok(a), Ok(b)) => prop_assert!(summary_bits_eq(&a, &b)),
            (Err(a), Err(b)) => prop_assert_eq!(a, b),
            _ => prop_assert!(false, "one call errored and the other did not"),
        }
    }

    // Permutation invariance of the summary's selection statistics (median,
    // MAD, percentiles) over the input multiset. The moment statistics are
    // grid-order dependent by ADR-0028 decision 3 and are not asserted here.
    #[test]
    fn summary_selection_stats_are_permutation_invariant(
        values in proptest::collection::vec(interesting_value(), 1..60),
        seed in any::<u64>(),
    ) {
        let params = SummaryParams { percentiles: vec![0.0, 0.25, 0.5, 0.75, 1.0] };
        let original = summary(&with_timestamps(&values), &params).expect("non-empty");

        // Fisher-Yates shuffle with a std-only PRNG seeded by `seed`.
        let mut shuffled = values.clone();
        let mut state = seed | 1;
        for i in (1..shuffled.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let j = (state % (i as u64 + 1)) as usize;
            shuffled.swap(i, j);
        }
        let permuted = summary(&with_timestamps(&shuffled), &params).expect("non-empty");

        prop_assert_eq!(original.median.to_bits(), permuted.median.to_bits());
        prop_assert_eq!(original.mad.to_bits(), permuted.mad.to_bits());
        prop_assert_eq!(original.nan_excluded, permuted.nan_excluded);
        for (a, b) in original.percentiles.iter().zip(&permuted.percentiles) {
            prop_assert_eq!(a.1.to_bits(), b.1.to_bits());
        }
    }

    // Time-translation invariance: shifting every timestamp by a constant
    // shifts the detected location by that constant and changes nothing else.
    #[test]
    fn change_point_location_is_time_translation_invariant(
        change_idx in 40usize..160,
        offset in -1_000_000_000i64..1_000_000_000,
    ) {
        let values = step_series(200, 5.0, 25.0, change_idx);
        let base = with_timestamps(&values);
        let shifted: Vec<(i64, f64)> =
            base.iter().map(|&(t, v)| (t + offset, v)).collect();
        let params = ChangePointParams { downsample: false };
        let a = change_point(&base, &params).expect("ok");
        let b = change_point(&shifted, &params).expect("ok");
        prop_assert_eq!(a.kind, b.kind);
        prop_assert_eq!(a.score.to_bits(), b.score.to_bits());
        match (a.ts_ns, b.ts_ns) {
            (Some(x), Some(y)) => prop_assert_eq!(x + offset, y),
            (None, None) => {},
            _ => prop_assert!(false, "location presence changed under translation"),
        }
    }

    // Value-offset invariance of step-change detection: adding a constant to
    // every value keeps the kind StepChange and the location unchanged.
    #[test]
    fn step_change_is_value_offset_invariant(
        change_idx in 40usize..160,
        offset in -1.0e6f64..1.0e6,
    ) {
        let values = step_series(200, 5.0, 25.0, change_idx);
        let base = with_timestamps(&values);
        let shifted: Vec<(i64, f64)> =
            values.iter().enumerate().map(|(i, &v)| (i as i64 * 60_000_000_000, v + offset)).collect();
        let params = ChangePointParams { downsample: false };
        let a = change_point(&base, &params).expect("ok");
        let b = change_point(&shifted, &params).expect("ok");
        prop_assert_eq!(a.kind, ChangeKind::StepChange);
        prop_assert_eq!(b.kind, ChangeKind::StepChange);
        prop_assert_eq!(a.ts_ns, b.ts_ns);
    }

    // The summary matches an independent naive reference bit-for-bit over the
    // adversarial pool (ADR-0022 exactness, ADR-0028 decision 6).
    #[test]
    fn summary_matches_naive_reference(
        values in proptest::collection::vec(interesting_value(), 1..60),
        pcts in proptest::collection::vec(0.0f64..=1.0, 0..5),
    ) {
        let series = with_timestamps(&values);
        let params = SummaryParams { percentiles: pcts.clone() };
        let got = summary(&series, &params).expect("non-empty");
        let want = naive_summary(&values, &pcts);
        prop_assert!(summary_bits_eq(&got, &want), "got {:?} want {:?}", got, want);
    }
}

/// An independent, deliberately straightforward reference implementation of
/// the summary, used only by the gate. It excludes NaN, sorts by `total_cmp`
/// for the selection statistics, and runs the same two-pass fold for the
/// moments; it exists to catch a wiring bug in the library (wrong NaN
/// handling, wrong ordering, a transposed pass), not to test floating
/// associativity, so it performs the identical arithmetic.
fn naive_summary(values: &[f64], pcts: &[f64]) -> SummaryResult {
    let mut kept = Vec::new();
    let mut nan_excluded = 0usize;
    for &v in values {
        if v.is_nan() {
            nan_excluded += 1;
        } else {
            kept.push(v);
        }
    }
    if kept.is_empty() {
        return SummaryResult {
            median: f64::NAN,
            mad: f64::NAN,
            percentiles: pcts.iter().map(|&p| (p, f64::NAN)).collect(),
            stddev: f64::NAN,
            variance: f64::NAN,
            nan_excluded,
        };
    }

    let mut ordered = kept.clone();
    ordered.sort_by(f64::total_cmp);
    let median = mid(&ordered);

    let mut devs: Vec<f64> = ordered.iter().map(|&x| (x - median).abs()).collect();
    devs.sort_by(f64::total_cmp);
    let mad = mid(&devs);

    let percentiles = pcts.iter().map(|&p| (p, interp(&ordered, p))).collect();

    // Two-pass population variance in grid order.
    let mut total = 0.0;
    for &v in &kept {
        total += v;
    }
    let mean = total / kept.len() as f64;
    let mut ss = 0.0;
    for &v in &kept {
        ss += (v - mean) * (v - mean);
    }
    let variance = ss / kept.len() as f64;

    SummaryResult {
        median,
        mad,
        percentiles,
        stddev: variance.sqrt(),
        variance,
        nan_excluded,
    }
}

fn mid(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

fn interp(sorted: &[f64], q: f64) -> f64 {
    let n = sorted.len() as f64;
    let rank = q * (n - 1.0);
    let lo_i = rank.floor();
    let hi_i = (lo_i + 1.0).min(n - 1.0);
    let w = rank - lo_i;
    sorted[lo_i as usize] * (1.0 - w) + sorted[hi_i as usize] * w
}
