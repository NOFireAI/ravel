//! Change point detection (ADR-0028 decisions 3, 4, 5).
//!
//! # Segmentation
//!
//! PELT (Pruned Exact Linear Time) partitions the series into contiguous
//! segments so as to minimize a penalized cost
//!
//! ```text
//!   sum over segments of C(segment)  +  k * beta
//! ```
//!
//! where `k` is the number of change points and `beta` is the BIC penalty.
//! The per-segment cost is the Gaussian negative log-likelihood with each
//! segment's own maximum-likelihood mean and variance, dropping the additive
//! constants that are identical for every segmentation of a fixed-length
//! series:
//!
//! ```text
//!   C(segment) = n * ln(variance_MLE)
//! ```
//!
//! The full Gaussian cost is `n*ln(2*pi) + n + n*ln(var)`; the first two
//! terms sum to `N*ln(2*pi) + N` over any segmentation of `N` points, a
//! constant that cannot change the arg-min, so only `n*ln(var)` is retained.
//! Each additional segment estimates two parameters (a mean and a variance),
//! so the BIC penalty per change point is `beta = 2 * ln(N)`. PELT's pruning
//! is exact for this cost with pruning constant `K = 0`.
//!
//! Values are centered on their mean before the prefix sums are formed: this
//! is exact under a value offset (adding a constant to every value leaves the
//! centered values unchanged) and keeps the `sum_sq - sum^2/n` variance form
//! well conditioned. A tiny variance floor keeps `ln(var)` finite for a
//! constant segment.
//!
//! # Classification
//!
//! From the segmentation the most significant change point is the boundary
//! with the largest cost reduction (the drop in `C` from splitting its two
//! adjacent segments). The transition there is classified by ordered
//! hypothesis tests, all of which are invariant to a time translation (they
//! use sample indices, not timestamp values) and, for the level tests, to a
//! value offset (they use mean differences and variances):
//!
//! 1. Spike / Dip: a short interior segment whose mean is far (by the pooled
//!    within-segment noise) from both neighbors in the same direction, with
//!    the neighbors returning to a common baseline.
//! 2. Trend change: a broken-line model fits far better than a
//!    piecewise-constant one, so the slope changes.
//! 3. Distribution change: the variance ratio across the boundary is large
//!    while the means stay close.
//! 4. Step change: the means differ across the boundary.
//!
//! A series with no change point is `Stationary`; a series with fewer than
//! [`MIN_POINTS`] evaluated points is `Indeterminable`.
//!
//! # Budget
//!
//! Detection runs on at most [`MAX_POINTS`] points. A longer series is a
//! [`AnalyticsError::SeriesTooLong`] error unless the caller sets
//! `downsample`, which reduces it by deterministic fixed-stride bucket
//! averaging (ADR-0028 decision 4).

use crate::{AnalyticsError, ChangeKind, ChangePointParams, ChangePointResult};

/// The maximum number of points `change_point` will run PELT over
/// (ADR-0028 decision 4).
pub(crate) const MAX_POINTS: usize = 2000;

/// The Elastic-matching floor: fewer evaluated points than this yields
/// [`ChangeKind::Indeterminable`] (ADR-0028 decision 3).
pub(crate) const MIN_POINTS: usize = 22;

/// An absolute floor on a segment's variance so `ln(var)` stays finite for a
/// constant segment. Offset-invariant; well below any realistic dispersion.
const VAR_FLOOR_ABS: f64 = 1e-12;

/// A segment's variance is also floored at this fraction of the whole
/// series' variance. Without a relative floor the Gaussian cost rewards
/// isolating a short low-variance run far too strongly (a lucky calm stretch
/// of noise), which over-segments an otherwise stationary series. Flooring
/// relative to the global spread bounds that reward.
const VAR_FLOOR_REL: f64 = 0.10;

/// The minimum segment length PELT will produce. Short segments are what let
/// the Gaussian cost chase noise; a floor of 15 samples keeps the mean/
/// variance estimates meaningful. Spikes and dips (shorter than this) are
/// detected separately, before PELT.
const MIN_SEG_LEN: usize = 15;

/// Number of pooled-noise standard deviations a level shift must exceed to
/// count as a step.
const STEP_Z: f64 = 4.0;

/// The robust (median absolute deviation) z-score a single point must exceed
/// to read as a spike or dip.
const SPIKE_Z: f64 = 6.0;

/// The variance ratio across a boundary above which the change reads as a
/// distribution change (given the means stay close).
const VAR_RATIO: f64 = 4.0;

/// A broken-line model is accepted as a trend change when its residual sum of
/// squares is below this fraction of both the piecewise-constant and the
/// single-line residuals.
const TREND_RATIO: f64 = 0.5;

/// Scale factor converting a median absolute deviation to a standard-deviation
/// estimate for a normal distribution (`1 / Phi^-1(0.75)`).
const MAD_TO_SIGMA: f64 = 1.482_602_218_505_602;

pub(crate) fn change_point(
    series: &[(i64, f64)],
    params: &ChangePointParams,
) -> Result<ChangePointResult, AnalyticsError> {
    if series.is_empty() {
        return Err(AnalyticsError::EmptySeries);
    }

    // Exclude NaN by IEEE class (never by an equality comparison) and count
    // the exclusions (ADR-0028 decision 5).
    let mut valid: Vec<(i64, f64)> = Vec::with_capacity(series.len());
    let mut nan_excluded = 0usize;
    for &(t, v) in series {
        if v.is_nan() {
            nan_excluded += 1;
        } else {
            valid.push((t, v));
        }
    }

    let original_points = valid.len();

    // Cap and optional downsampling (ADR-0028 decision 4).
    let mut downsampled = false;
    let work = if valid.len() > MAX_POINTS {
        if !params.downsample {
            return Err(AnalyticsError::SeriesTooLong {
                max: MAX_POINTS,
                got: valid.len(),
            });
        }
        downsampled = true;
        bucket_average(&valid, MAX_POINTS)
    } else {
        valid
    };

    if work.len() < MIN_POINTS {
        return Ok(ChangePointResult {
            kind: ChangeKind::Indeterminable,
            ts_ns: None,
            score: 0.0,
            downsampled,
            original_points,
            nan_excluded,
        });
    }

    let (kind, ts_ns, score) = detect(&work);
    Ok(ChangePointResult {
        kind,
        ts_ns,
        score,
        downsampled,
        original_points,
        nan_excluded,
    })
}

/// Deterministic fixed-stride bucket averaging to at most `buckets` points.
/// Bucket `k` spans the half-open index range
/// `[floor(k*n/buckets), floor((k+1)*n/buckets))`; its value is the mean of
/// the values in that range and its timestamp is the first timestamp in it.
/// Using the first timestamp keeps the output strictly monotonic and lets a
/// reported location map back to a real evaluation instant.
fn bucket_average(valid: &[(i64, f64)], buckets: usize) -> Vec<(i64, f64)> {
    let n = valid.len();
    let mut out = Vec::with_capacity(buckets);
    for k in 0..buckets {
        let start = k * n / buckets;
        let end = (k + 1) * n / buckets;
        if start >= end {
            continue;
        }
        let slice = &valid[start..end];
        let mut sum = 0.0_f64;
        for &(_, v) in slice {
            sum += v;
        }
        out.push((slice[0].0, sum / slice.len() as f64));
    }
    out
}

/// O(1)-segment-cost prefix sums over the centered values.
struct Cost {
    /// Prefix sums of values; `s[i]` is the sum of the first `i` values.
    s: Vec<f64>,
    /// Prefix sums of squared values.
    ss: Vec<f64>,
    /// The variance floor (absolute and relative components resolved).
    floor: f64,
}

impl Cost {
    fn new(values: &[f64]) -> Self {
        let mut s = Vec::with_capacity(values.len() + 1);
        let mut ss = Vec::with_capacity(values.len() + 1);
        s.push(0.0);
        ss.push(0.0);
        let mut acc = 0.0_f64;
        let mut acc_sq = 0.0_f64;
        for &v in values {
            acc += v;
            acc_sq += v * v;
            s.push(acc);
            ss.push(acc_sq);
        }
        let global_var = population_variance(values);
        let floor = (global_var * VAR_FLOOR_REL).max(VAR_FLOOR_ABS);
        Cost { s, ss, floor }
    }

    /// The Gaussian cost `n * ln(var)` of the segment `[a, b)`, with the
    /// variance floored (absolute and relative to the global spread).
    fn cost(&self, a: usize, b: usize) -> f64 {
        let n = (b - a) as f64;
        let sum = self.s[b] - self.s[a];
        let sum_sq = self.ss[b] - self.ss[a];
        let mean = sum / n;
        let var = (sum_sq / n - mean * mean).max(0.0).max(self.floor);
        n * var.ln()
    }
}

/// PELT segmentation with a minimum segment length. Returns the interior
/// segment-start indices (the change points) in ascending order; an empty
/// vector means one segment. Every produced segment has length at least
/// [`MIN_SEG_LEN`].
fn pelt(cost: &Cost, n: usize) -> Vec<usize> {
    let l = MIN_SEG_LEN;
    let beta = 2.0 * (n as f64).ln();
    // `f[t]` is the optimal penalized cost of segmenting the first `t` points;
    // +inf marks a `t` with no valid segmentation (every segment >= l).
    let mut f = vec![f64::INFINITY; n + 1];
    f[0] = -beta;
    // `last[t]` is the start index of the last segment in that segmentation.
    let mut last = vec![0usize; n + 1];
    // Candidate starts whose `f` is finite and that are far enough back to
    // begin a length->=l segment ending at the current `t`.
    let mut cands: Vec<usize> = Vec::new();

    for t in l..=n {
        // `t - l` becomes eligible now: a segment [t-l, t) has length exactly
        // l. Only add it if it is a valid segmentation boundary.
        let new_s = t - l;
        if f[new_s].is_finite() {
            cands.push(new_s);
        }

        let mut best = f64::INFINITY;
        let mut best_s = 0usize;
        for &s in &cands {
            let c = f[s] + cost.cost(s, t) + beta;
            // Strict `<` keeps the earliest candidate on ties, for a
            // deterministic segmentation.
            if c < best {
                best = c;
                best_s = s;
            }
        }
        f[t] = best;
        last[t] = best_s;

        let ft = f[t];
        cands.retain(|&s| f[s] + cost.cost(s, t) <= ft);
    }

    let mut bounds = Vec::new();
    let mut t = n;
    while t > 0 {
        let s = last[t];
        if s > 0 {
            bounds.push(s);
        }
        t = s;
    }
    bounds.reverse();
    bounds
}

/// Classify the series and locate its most significant change.
fn detect(work: &[(i64, f64)]) -> (ChangeKind, Option<i64>, f64) {
    let n = work.len();
    let ts: Vec<i64> = work.iter().map(|&(t, _)| t).collect();
    let raw: Vec<f64> = work.iter().map(|&(_, v)| v).collect();

    // Center on the mean: offset-invariant, and it conditions the variance.
    let mean_all = mean(&raw);
    let v: Vec<f64> = raw.iter().map(|x| x - mean_all).collect();

    // 1. Spike / dip: a robust (median/MAD) check, independent of the
    //    segmentation, so it catches excursions shorter than a PELT segment.
    if let Some((kind, idx, score)) = detect_spike(&v) {
        return (kind, Some(ts[idx]), score);
    }

    let cost = Cost::new(&v);
    let bounds = pelt(&cost, n);
    if bounds.is_empty() {
        return (ChangeKind::Stationary, None, 0.0);
    }

    // Segment boundaries as start indices: [0, b1, ..., bk, n].
    let mut starts = vec![0usize];
    starts.extend_from_slice(&bounds);
    starts.push(n);

    // Significance of the detected change, and the dominant PELT boundary:
    // the largest cost reduction from splitting the two segments adjacent to
    // a boundary. The Gaussian cost is sensitive to both mean and variance,
    // so this boundary tracks a variance shift (whose location a
    // piecewise-constant residual cannot see, the means being equal).
    let mut best_b = bounds[0];
    let mut best_score = f64::NEG_INFINITY;
    for w in 1..starts.len() - 1 {
        let a = starts[w - 1];
        let b = starts[w];
        let c = starts[w + 1];
        let sc = cost.cost(a, c) - cost.cost(a, b) - cost.cost(b, c);
        if sc > best_score {
            best_score = sc;
            best_b = b;
        }
    }

    // Refine the dominant breakpoint for classification and location. PELT
    // boundaries are quantized by the minimum segment length, and the
    // piecewise-constant residual is very sensitive to a boundary that sits a
    // few points off a level shift (misassigned points inflate a segment's
    // variance). So the best single-break piecewise-constant model and the
    // best single-break broken-line model are each optimized over every
    // admissible index. Comparing their residuals distinguishes a level shift
    // (both fit equally well) from a slope change (the line fits far better),
    // and the argmin indices give precise locations.
    let lin = LinRss::new(&v);
    let rss_single_line = lin.rss(0, n);
    let mut best_step_b = bounds[0];
    let mut rss_step_best = f64::INFINITY;
    let mut best_line_b = bounds[0];
    let mut best_line_rss = f64::INFINITY;
    for b in MIN_SEG_LEN..=n - MIN_SEG_LEN {
        let step_rss = lin.rss_const(0, b) + lin.rss_const(b, n);
        if step_rss < rss_step_best {
            rss_step_best = step_rss;
            best_step_b = b;
        }
        let line_rss = lin.rss(0, b) + lin.rss(b, n);
        if line_rss < best_line_rss {
            best_line_rss = line_rss;
            best_line_b = b;
        }
    }

    // Noise floor from the refined piecewise-constant split (not the PELT
    // segments, whose quantized boundary can contaminate a segment).
    let noise = (rss_step_best / n as f64).max(cost.floor).sqrt();

    // 2. Trend change: the best broken-line fit beats both the best
    //    piecewise-constant split and the single-line fit by TREND_RATIO.
    if best_line_rss < TREND_RATIO * rss_step_best && best_line_rss < TREND_RATIO * rss_single_line
    {
        return (ChangeKind::TrendChange, Some(ts[best_line_b]), best_score);
    }

    // 3. Distribution change: across the dominant PELT boundary the variance
    //    ratio is large while the means stay close. Located at that boundary,
    //    since a piecewise-constant residual is blind to a variance shift.
    let dvar_l = population_variance(&v[..best_b]);
    let dvar_r = population_variance(&v[best_b..]);
    let dmean_l = mean(&v[..best_b]);
    let dmean_r = mean(&v[best_b..]);
    let vmax = dvar_l.max(dvar_r);
    let vmin = dvar_l.min(dvar_r).max(cost.floor);
    if (dmean_l - dmean_r).abs() < STEP_Z * noise && vmax > VAR_RATIO * vmin {
        return (ChangeKind::DistributionChange, Some(ts[best_b]), best_score);
    }

    // 4. Step change: a significant level shift across the refined mean-split
    //    boundary (the piecewise-constant argmin, which lands on a level
    //    shift precisely because the means differ there).
    let mean_l = mean(&v[..best_step_b]);
    let mean_r = mean(&v[best_step_b..]);
    if (mean_l - mean_r).abs() > STEP_Z * noise {
        return (ChangeKind::StepChange, Some(ts[best_step_b]), best_score);
    }

    // A boundary was found but no hypothesis is significant: not a change.
    (ChangeKind::Stationary, None, 0.0)
}

/// Detect a spike or dip: one point (or a few) whose deviation from the
/// robust center exceeds [`SPIKE_Z`] median-absolute-deviation z-scores,
/// while such extremes stay rare enough to be an isolated excursion rather
/// than a whole regime. Returns `(kind, index, z_score)` for the strongest
/// excursion; the score is the robust z-score of the extreme point.
fn detect_spike(v: &[f64]) -> Option<(ChangeKind, usize, f64)> {
    let n = v.len();
    let mut sorted = v.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = median_of_sorted(&sorted);

    let mut abs_dev: Vec<f64> = v.iter().map(|&x| (x - median).abs()).collect();
    abs_dev.sort_by(f64::total_cmp);
    let mad = median_of_sorted(&abs_dev);

    // Fall back to the population standard deviation when the MAD is zero
    // (more than half the points share the median), so a spike above an
    // otherwise-constant baseline is still scored.
    let mut sigma = mad * MAD_TO_SIGMA;
    if sigma <= VAR_FLOOR_ABS {
        sigma = population_variance(v).sqrt();
    }
    if sigma <= VAR_FLOOR_ABS {
        return None; // a truly constant series has no spike.
    }

    let threshold = SPIKE_Z * sigma;
    // At most this many points may exceed the threshold for the excursion to
    // read as a spike rather than a sustained regime.
    let cap = (n / 50).max(1);

    let mut extreme_count = 0usize;
    let mut peak_idx = 0usize;
    let mut peak_dev = 0.0_f64;
    for (i, &x) in v.iter().enumerate() {
        let dev = (x - median).abs();
        if dev > threshold {
            extreme_count += 1;
        }
        if dev > peak_dev {
            peak_dev = dev;
            peak_idx = i;
        }
    }

    if extreme_count == 0 || extreme_count > cap {
        return None;
    }

    let kind = if v[peak_idx] > median {
        ChangeKind::Spike
    } else {
        ChangeKind::Dip
    };
    Some((kind, peak_idx, peak_dev / sigma))
}

/// The median of an already-sorted, non-empty slice.
fn median_of_sorted(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

/// Arithmetic mean of a non-empty slice.
fn mean(values: &[f64]) -> f64 {
    let mut sum = 0.0_f64;
    for &v in values {
        sum += v;
    }
    sum / values.len() as f64
}

/// Population variance of a non-empty slice.
fn population_variance(values: &[f64]) -> f64 {
    let m = mean(values);
    let mut acc = 0.0_f64;
    for &v in values {
        let d = v - m;
        acc += d * d;
    }
    acc / values.len() as f64
}

/// Prefix sums for the O(1) residual sum of squares of an ordinary-least-
/// squares line fit over any index range, with `x` the global sample index.
/// (Shifting `x` by a constant does not change a line fit's residuals, so a
/// global `x` gives the same RSS as a segment-local one.)
struct LinRss {
    sx: Vec<f64>,
    sy: Vec<f64>,
    sxy: Vec<f64>,
    sxx: Vec<f64>,
    syy: Vec<f64>,
}

impl LinRss {
    fn new(values: &[f64]) -> Self {
        let n = values.len();
        let mut sx = Vec::with_capacity(n + 1);
        let mut sy = Vec::with_capacity(n + 1);
        let mut sxy = Vec::with_capacity(n + 1);
        let mut sxx = Vec::with_capacity(n + 1);
        let mut syy = Vec::with_capacity(n + 1);
        let (mut ax, mut ay, mut axy, mut axx, mut ayy) = (0.0, 0.0, 0.0, 0.0, 0.0);
        sx.push(0.0);
        sy.push(0.0);
        sxy.push(0.0);
        sxx.push(0.0);
        syy.push(0.0);
        for (i, &y) in values.iter().enumerate() {
            let x = i as f64;
            ax += x;
            ay += y;
            axy += x * y;
            axx += x * x;
            ayy += y * y;
            sx.push(ax);
            sy.push(ay);
            sxy.push(axy);
            sxx.push(axx);
            syy.push(ayy);
        }
        LinRss {
            sx,
            sy,
            sxy,
            sxx,
            syy,
        }
    }

    /// Residual sum of squares of the best constant fit over `[a, b)`: the
    /// sum of squared deviations from the range mean. A single point fits
    /// exactly (`0.0`).
    fn rss_const(&self, a: usize, b: usize) -> f64 {
        let n = (b - a) as f64;
        let sy = self.sy[b] - self.sy[a];
        let syy = self.syy[b] - self.syy[a];
        (syy - sy * sy / n).max(0.0)
    }

    /// Residual sum of squares of the least-squares line over `[a, b)`. A
    /// range shorter than two points fits exactly (`0.0`).
    fn rss(&self, a: usize, b: usize) -> f64 {
        let n = (b - a) as f64;
        if b - a < 2 {
            return 0.0;
        }
        let sx = self.sx[b] - self.sx[a];
        let sy = self.sy[b] - self.sy[a];
        let sxy = self.sxy[b] - self.sxy[a];
        let sxx = self.sxx[b] - self.sxx[a];
        let syy = self.syy[b] - self.syy[a];
        let var_x = sxx - sx * sx / n;
        let syy_centered = syy - sy * sy / n;
        if var_x <= 0.0 {
            return syy_centered.max(0.0);
        }
        let cov_xy = sxy - sx * sy / n;
        (syy_centered - cov_xy * cov_xy / var_x).max(0.0)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn params(downsample: bool) -> ChangePointParams {
        ChangePointParams { downsample }
    }

    fn series_from(values: &[f64]) -> Vec<(i64, f64)> {
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i as i64 * 60_000_000_000, v))
            .collect()
    }

    #[test]
    fn empty_series_is_an_error() {
        assert_eq!(
            change_point(&[], &params(false)),
            Err(AnalyticsError::EmptySeries)
        );
    }

    #[test]
    fn under_the_floor_is_indeterminable() {
        let series = series_from(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let r = change_point(&series, &params(false)).expect("ok");
        assert_eq!(r.kind, ChangeKind::Indeterminable);
        assert_eq!(r.ts_ns, None);
        assert_eq!(r.original_points, 5);
    }

    #[test]
    fn over_cap_without_downsample_is_an_error() {
        let series = series_from(&vec![1.0; MAX_POINTS + 1]);
        assert_eq!(
            change_point(&series, &params(false)),
            Err(AnalyticsError::SeriesTooLong {
                max: MAX_POINTS,
                got: MAX_POINTS + 1
            })
        );
    }

    #[test]
    fn over_cap_with_downsample_reports_downsampled_and_original_points() {
        // A constant series of 2500 points: over the cap, so downsampling
        // reduces it, original_points is preserved, and a constant series is
        // Stationary.
        let series = series_from(&vec![7.0; 2500]);
        let r = change_point(&series, &params(true)).expect("ok");
        assert!(r.downsampled);
        assert_eq!(r.original_points, 2500);
        assert_eq!(r.kind, ChangeKind::Stationary);
    }

    #[test]
    fn constant_series_is_stationary() {
        let series = series_from(&vec![3.5; 100]);
        let r = change_point(&series, &params(false)).expect("ok");
        assert_eq!(r.kind, ChangeKind::Stationary);
        assert_eq!(r.ts_ns, None);
        assert!(!r.downsampled);
        assert_eq!(r.original_points, 100);
    }

    #[test]
    fn nan_values_are_excluded_and_counted() {
        let mut values = vec![5.0; 60];
        values[10] = f64::NAN;
        values[20] = f64::NAN;
        let series = series_from(&values);
        let r = change_point(&series, &params(false)).expect("ok");
        assert_eq!(r.nan_excluded, 2);
        assert_eq!(r.original_points, 58);
    }

    #[test]
    fn all_nan_is_indeterminable_without_panic() {
        let series = series_from(&vec![f64::NAN; 50]);
        let r = change_point(&series, &params(false)).expect("ok");
        assert_eq!(r.kind, ChangeKind::Indeterminable);
        assert_eq!(r.nan_excluded, 50);
        assert_eq!(r.original_points, 0);
    }

    #[test]
    fn single_point_does_not_panic() {
        let series = series_from(&[42.0]);
        let r = change_point(&series, &params(false)).expect("ok");
        assert_eq!(r.kind, ChangeKind::Indeterminable);
    }

    #[test]
    fn bucket_average_preserves_bounds_and_monotonic_time() {
        let valid: Vec<(i64, f64)> = (0..100).map(|i| (i as i64 * 10, i as f64)).collect();
        let out = bucket_average(&valid, 10);
        assert_eq!(out.len(), 10);
        // First bucket averages indices 0..10 -> mean 4.5, ts of index 0.
        assert_eq!(out[0], (0, 4.5));
        for pair in out.windows(2) {
            assert!(pair[0].0 < pair[1].0);
        }
    }
}
