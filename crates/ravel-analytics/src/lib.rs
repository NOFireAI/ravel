//! Post-evaluation analytics stage (ADR-0028): change point detection and
//! robust summary statistics as pure per-series functions over
//! `(timestamp_ns, f64)` slices.
//!
//! Every function here is a deterministic function of its input slice and
//! parameters: no clock, no IO, no object-store or catalog access. The crate
//! depends on std only. It exists because neither existing query surface can
//! host these operations (ADR-0028 context): PromQL's parser table is closed
//! (ADR-0007) and SQL's floating-aggregate admission regime (ADR-0022)
//! excludes the second-moment family and cannot express a structured
//! per-series result.
//!
//! Two operations are exposed, both mapped from the ES|QL analytic surface:
//!
//! - [`change_point`]: PELT (Pruned Exact Linear Time) segmentation with a
//!   BIC penalty over a Gaussian (mean and variance) cost, classifying each
//!   series into a [`ChangeKind`] and reporting the location and significance
//!   of its most significant change. See the [`change_point`] module.
//! - [`summary`]: exact median, median absolute deviation, percentiles
//!   (Prometheus interpolation), population standard deviation, and
//!   population variance. See the [`summary`] module.
//!
//! NaN handling (ADR-0028 decision 5): NaN values are excluded from both
//! operations and the excluded count is reported per series. NaN is detected
//! by [`f64::is_nan`], never by an equality comparison, so every NaN payload
//! and both signed zeros are handled by IEEE class, not bit accident.
//!
//! The normative specification is docs/analytics.md.

mod change_point;
mod error;
mod summary;

pub use error::AnalyticsError;

/// The classification of a series' most significant change, per ADR-0028
/// decision 3.
///
/// `Stationary` means no significant change was found; `Indeterminable` means
/// the series had fewer than 22 evaluated points, so detection was not
/// attempted (matching Elastic's floor).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    /// A short excursion far above the surrounding baseline that returns.
    Spike,
    /// A short excursion far below the surrounding baseline that returns.
    Dip,
    /// A persistent shift in level (mean) between two segments.
    StepChange,
    /// A change in slope (trend) between two segments.
    TrendChange,
    /// A change in dispersion (variance) with the level held roughly constant.
    DistributionChange,
    /// No significant change was detected.
    Stationary,
    /// Too few evaluated points to attempt detection.
    Indeterminable,
}

/// Parameters for [`change_point`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangePointParams {
    /// Opt in to deterministic fixed-stride bucket-average downsampling when
    /// the series exceeds the 2000-point cap (ADR-0028 decision 4). When
    /// false, an over-cap series is an [`AnalyticsError::SeriesTooLong`]
    /// error; approximation is opt-in and visible.
    pub downsample: bool,
}

/// The result of [`change_point`] for one series.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangePointResult {
    /// The classification of the most significant change.
    pub kind: ChangeKind,
    /// The timestamp (nanoseconds) of the most significant change, or `None`
    /// when the kind is `Stationary` or `Indeterminable`.
    pub ts_ns: Option<i64>,
    /// The significance of the reported change (a cost reduction in nats);
    /// `0.0` when no change was reported.
    pub score: f64,
    /// Whether the series was downsampled before detection.
    pub downsampled: bool,
    /// The number of non-NaN points fed into detection, before any
    /// downsampling.
    pub original_points: usize,
    /// The number of NaN points excluded from detection.
    pub nan_excluded: usize,
}

/// Parameters for [`summary`].
#[derive(Debug, Clone, PartialEq)]
pub struct SummaryParams {
    /// The quantiles to report, each in `[0, 1]`. A value outside that range
    /// (including NaN) is an [`AnalyticsError::InvalidPercentile`] error.
    pub percentiles: Vec<f64>,
}

/// The result of [`summary`] for one series.
#[derive(Debug, Clone, PartialEq)]
pub struct SummaryResult {
    /// The exact median (average of the two central order statistics for an
    /// even count).
    pub median: f64,
    /// The median absolute deviation: the median of `|x - median|`.
    pub mad: f64,
    /// Each requested quantile paired with its interpolated value, in request
    /// order.
    pub percentiles: Vec<(f64, f64)>,
    /// The population standard deviation (square root of `variance`).
    pub stddev: f64,
    /// The population variance (sum of squared deviations divided by count).
    pub variance: f64,
    /// The number of NaN points excluded from the summary.
    pub nan_excluded: usize,
}

/// Detect the most significant change point in one series (ADR-0028
/// decisions 3, 4, 5).
///
/// The series is a slice of `(timestamp_ns, value)` pairs in evaluation-grid
/// order. NaN values are excluded and counted. A series with more than 2000
/// non-NaN points is an error unless [`ChangePointParams::downsample`] is set,
/// in which case it is reduced to at most 2000 points by deterministic
/// fixed-stride bucket averaging. Fewer than 22 evaluated points yields
/// [`ChangeKind::Indeterminable`].
///
/// # Errors
///
/// - [`AnalyticsError::EmptySeries`] if `series` is empty.
/// - [`AnalyticsError::SeriesTooLong`] if the non-NaN count exceeds 2000 and
///   `params.downsample` is false.
pub fn change_point(
    series: &[(i64, f64)],
    params: &ChangePointParams,
) -> Result<ChangePointResult, AnalyticsError> {
    change_point::change_point(series, params)
}

/// Compute robust summary statistics for one series (ADR-0028 decision 3).
///
/// NaN values are excluded and counted. Selection statistics (median, MAD,
/// percentiles) sort by [`f64::total_cmp`]; moment statistics (variance,
/// stddev) use a sequential two-pass fold over the evaluation-grid order.
///
/// # Errors
///
/// - [`AnalyticsError::EmptySeries`] if `series` is empty.
/// - [`AnalyticsError::InvalidPercentile`] if any requested percentile is
///   outside `[0, 1]` (NaN included).
pub fn summary(
    series: &[(i64, f64)],
    params: &SummaryParams,
) -> Result<SummaryResult, AnalyticsError> {
    summary::summary(series, params)
}
