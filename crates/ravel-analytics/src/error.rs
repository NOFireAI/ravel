//! The typed error surface for the analytics stage (ADR-0028 decision 6:
//! out-of-contract input produces typed errors, never panics or silent wrong
//! data).

use std::fmt;

/// An error returned by [`crate::change_point`] or [`crate::summary`].
///
/// `PartialEq` is derived for test convenience; note that
/// `InvalidPercentile { got: NaN }` never equals itself, since NaN is not
/// equal to NaN. Match the variant rather than comparing for equality when
/// the offending value may be NaN.
#[derive(Debug, Clone, PartialEq)]
pub enum AnalyticsError {
    /// A `change_point` series exceeded the 2000-point cap and the caller did
    /// not opt in to downsampling (ADR-0028 decision 4).
    SeriesTooLong {
        /// The point cap (2000).
        max: usize,
        /// The number of non-NaN points the caller supplied.
        got: usize,
    },
    /// The input series was empty.
    EmptySeries,
    /// A requested percentile was outside the closed interval `[0, 1]`
    /// (NaN included).
    InvalidPercentile {
        /// The offending value.
        got: f64,
    },
}

impl fmt::Display for AnalyticsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AnalyticsError::SeriesTooLong { max, got } => write!(
                f,
                "change_point series has {got} points, over the {max}-point cap; \
                 set downsample to reduce it"
            ),
            AnalyticsError::EmptySeries => write!(f, "series is empty"),
            AnalyticsError::InvalidPercentile { got } => {
                write!(f, "percentile {got} is outside [0, 1]")
            }
        }
    }
}

impl std::error::Error for AnalyticsError {}
