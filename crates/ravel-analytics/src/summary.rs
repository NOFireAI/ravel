//! Robust per-series summary statistics (ADR-0028 decision 3).
//!
//! Selection statistics (median, MAD, percentiles) sort a copy of the
//! non-NaN values by [`f64::total_cmp`], which is a total order over all
//! f64 bit patterns: `-0.0` sorts below `0.0`, and the infinities sort at the
//! ends. Because they operate on the sorted multiset, they are invariant to
//! the input order (a permutation of the same values yields bit-identical
//! results).
//!
//! Moment statistics (variance, stddev) use a sequential two-pass fold in the
//! evaluation-grid order: pass one sums the values and divides by the count
//! to get the mean; pass two folds the squared deviations. This is naive IEEE
//! addition, not Kahan compensation, so an independent reference running the
//! identical two-pass fold is bit-identical by construction (the ADR-0028
//! decision 6 gate over the ADR-0022 adversarial value pool). Unlike the
//! selection statistics, the moments depend on the grid order, since f64
//! addition is not associative; this is intentional and documented in
//! docs/analytics.md.

use crate::{AnalyticsError, SummaryParams, SummaryResult};

pub(crate) fn summary(
    series: &[(i64, f64)],
    params: &SummaryParams,
) -> Result<SummaryResult, AnalyticsError> {
    if series.is_empty() {
        return Err(AnalyticsError::EmptySeries);
    }
    for &p in &params.percentiles {
        // NaN fails the range check (every comparison with NaN is false), so
        // a NaN percentile is rejected here rather than propagating.
        if !(0.0..=1.0).contains(&p) {
            return Err(AnalyticsError::InvalidPercentile { got: p });
        }
    }

    // Exclude NaN by IEEE class (never by an equality comparison) and count
    // the exclusions (ADR-0028 decision 5).
    let mut values: Vec<f64> = Vec::with_capacity(series.len());
    let mut nan_excluded = 0usize;
    for &(_, v) in series {
        if v.is_nan() {
            nan_excluded += 1;
        } else {
            values.push(v);
        }
    }

    if values.is_empty() {
        // Every point was NaN. There is nothing to summarize; return NaN
        // statistics rather than panicking (ADR-0028 decision 6).
        let percentiles = params.percentiles.iter().map(|&p| (p, f64::NAN)).collect();
        return Ok(SummaryResult {
            median: f64::NAN,
            mad: f64::NAN,
            percentiles,
            stddev: f64::NAN,
            variance: f64::NAN,
            nan_excluded,
        });
    }

    let mut sorted = values.clone();
    sorted.sort_by(f64::total_cmp);

    let median = median_of_sorted(&sorted);

    // MAD: median of the absolute deviations from the median.
    let mut deviations: Vec<f64> = sorted.iter().map(|&x| (x - median).abs()).collect();
    deviations.sort_by(f64::total_cmp);
    let mad = median_of_sorted(&deviations);

    let percentiles = params
        .percentiles
        .iter()
        .map(|&p| (p, percentile_of_sorted(&sorted, p)))
        .collect();

    let variance = population_variance(&values);
    let stddev = variance.sqrt();

    Ok(SummaryResult {
        median,
        mad,
        percentiles,
        stddev,
        variance,
        nan_excluded,
    })
}

/// The exact median of an already-sorted, non-empty slice: the central order
/// statistic for an odd count, the average of the two central ones for an
/// even count.
fn median_of_sorted(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

/// Prometheus' quantile interpolation over an already-sorted, non-empty
/// slice: linear interpolation between the two ranks straddling `q * (n - 1)`.
/// `q` is assumed to be in `[0, 1]` (validated by the caller). Mirrors
/// `ravel-promql`'s `quantile`, minus the NaN-sorts-smallest and out-of-range
/// clamping that this call site has already ruled out (NaN excluded upstream,
/// `q` range-checked upstream).
fn percentile_of_sorted(sorted: &[f64], q: f64) -> f64 {
    let n = sorted.len() as f64;
    let rank = q * (n - 1.0);
    let lower_index = rank.floor();
    let upper_index = (lower_index + 1.0).min(n - 1.0);
    let weight = rank - lower_index;
    let lo = sorted[lower_index as usize];
    let hi = sorted[upper_index as usize];
    lo * (1.0 - weight) + hi * weight
}

/// Population variance by a sequential two-pass fold over the grid order (see
/// the module doc). The values slice is non-empty.
fn population_variance(values: &[f64]) -> f64 {
    let n = values.len() as f64;
    let mut sum = 0.0_f64;
    for &v in values {
        sum += v;
    }
    let mean = sum / n;
    let mut m2 = 0.0_f64;
    for &v in values {
        let d = v - mean;
        m2 += d * d;
    }
    m2 / n
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn s(values: &[f64]) -> Vec<(i64, f64)> {
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i as i64 * 1_000_000, v))
            .collect()
    }

    fn params(percentiles: &[f64]) -> SummaryParams {
        SummaryParams {
            percentiles: percentiles.to_vec(),
        }
    }

    #[test]
    fn median_odd_and_even() {
        let odd = summary(&s(&[3.0, 1.0, 2.0]), &params(&[])).expect("ok");
        assert_eq!(odd.median, 2.0);
        let even = summary(&s(&[4.0, 1.0, 2.0, 3.0]), &params(&[])).expect("ok");
        assert_eq!(even.median, 2.5);
    }

    #[test]
    fn mad_of_a_known_series() {
        // values 1,2,3,4,5 -> median 3 -> deviations 2,1,0,1,2 -> median 1.
        let r = summary(&s(&[1.0, 2.0, 3.0, 4.0, 5.0]), &params(&[])).expect("ok");
        assert_eq!(r.mad, 1.0);
    }

    #[test]
    fn percentiles_interpolate_like_prometheus() {
        let r = summary(&s(&[1.0, 2.0, 3.0, 4.0]), &params(&[0.0, 0.5, 1.0])).expect("ok");
        assert_eq!(r.percentiles[0], (0.0, 1.0));
        assert_eq!(r.percentiles[1], (0.5, 2.5));
        assert_eq!(r.percentiles[2], (1.0, 4.0));
    }

    #[test]
    fn variance_and_stddev_population() {
        // 2,4,4,4,5,5,7,9: population variance 4, stddev 2.
        let r = summary(&s(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]), &params(&[])).expect("ok");
        assert_eq!(r.variance, 4.0);
        assert_eq!(r.stddev, 2.0);
    }

    #[test]
    fn empty_series_is_an_error() {
        assert_eq!(summary(&[], &params(&[])), Err(AnalyticsError::EmptySeries));
    }

    #[test]
    fn invalid_percentile_is_an_error() {
        assert_eq!(
            summary(&s(&[1.0]), &params(&[1.5])),
            Err(AnalyticsError::InvalidPercentile { got: 1.5 })
        );
        assert_eq!(
            summary(&s(&[1.0]), &params(&[-0.1])),
            Err(AnalyticsError::InvalidPercentile { got: -0.1 })
        );
    }

    #[test]
    fn nan_percentile_is_rejected() {
        let err = summary(&s(&[1.0]), &params(&[f64::NAN])).expect_err("nan rejected");
        assert!(matches!(err, AnalyticsError::InvalidPercentile { got } if got.is_nan()));
    }

    #[test]
    fn nan_values_are_excluded_and_counted() {
        let r = summary(&s(&[1.0, f64::NAN, 3.0, f64::NAN, 5.0]), &params(&[0.5])).expect("ok");
        assert_eq!(r.nan_excluded, 2);
        assert_eq!(r.median, 3.0);
    }

    #[test]
    fn all_nan_yields_nan_statistics_without_panic() {
        let r = summary(&s(&[f64::NAN, f64::NAN]), &params(&[0.5])).expect("ok");
        assert_eq!(r.nan_excluded, 2);
        assert!(r.median.is_nan());
        assert!(r.mad.is_nan());
        assert!(r.variance.is_nan());
        assert!(r.stddev.is_nan());
        assert!(r.percentiles[0].1.is_nan());
    }

    #[test]
    fn single_value_has_zero_dispersion() {
        let r = summary(&s(&[42.0]), &params(&[0.0, 0.5, 1.0])).expect("ok");
        assert_eq!(r.median, 42.0);
        assert_eq!(r.mad, 0.0);
        assert_eq!(r.variance, 0.0);
        assert_eq!(r.stddev, 0.0);
        for &(_, v) in &r.percentiles {
            assert_eq!(v, 42.0);
        }
    }

    #[test]
    fn median_averages_negative_and_positive_zero_to_positive_zero() {
        // total_cmp sorts -0.0 below 0.0; the even-count median averages them,
        // and -0.0 + 0.0 is +0.0 in IEEE.
        let r = summary(&s(&[0.0, -0.0]), &params(&[])).expect("ok");
        assert_eq!(r.median.to_bits(), 0.0_f64.to_bits());
    }
}
