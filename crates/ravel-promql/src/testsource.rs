//! In-memory [`SeriesSource`] built from literal series definitions.
//!
//! `pub` so it can be reused as a test fixture from other crates' dev
//! dependencies (e.g. `ravel-query`) without each crate reinventing a fake
//! store just to exercise the evaluator.

use ravel_types::{Label, LabelSet, Sample, TimeRange, TypeError};

use crate::histogram::FloatHistogram;
use crate::matchers;
use crate::source::{
    HistogramSample, HistogramSeriesData, LabelMatcher, SeriesData, SeriesSource, SourceError,
};

/// Building a [`TestSource`] from malformed literal data.
#[derive(Debug, thiserror::Error)]
pub enum TestSourceError {
    #[error("invalid labels: {0}")]
    Labels(#[source] TypeError),
}

/// An in-memory series source built from literal label/sample data,
/// suitable for unit and integration tests. Not a stand-in for a real
/// storage backend's pruning/paging behavior, only for exercising the
/// evaluator against known inputs.
#[derive(Debug, Clone, Default)]
pub struct TestSource {
    series: Vec<SeriesData>,
    histogram_series: Vec<HistogramSeriesData>,
}

impl TestSource {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one series. `labels` are name/value pairs (may include
    /// `__name__`); `samples` are `(ts_ns, value)` pairs and may be listed
    /// in any order, they are sorted by timestamp before being stored
    /// (stably, so equal-timestamp samples keep their relative order: this
    /// is how tests express "last sample at this timestamp wins").
    pub fn with_series(
        mut self,
        labels: &[(&str, &str)],
        samples: &[(i64, f64)],
    ) -> Result<Self, TestSourceError> {
        let label_set = LabelSet::new(
            labels
                .iter()
                .map(|(name, value)| Label {
                    name: (*name).to_string(),
                    value: (*value).to_string(),
                })
                .collect(),
        )
        .map_err(TestSourceError::Labels)?;

        let mut samples: Vec<Sample> = samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect();
        samples.sort_by_key(|s| s.ts_ns);

        self.series.push(SeriesData {
            labels: label_set,
            samples,
        });
        Ok(self)
    }

    /// Add one native-histogram series. `samples` are `(ts_ns, value)`
    /// pairs, sorted by timestamp before storage (stably), the histogram
    /// counterpart of [`TestSource::with_series`].
    pub fn with_histogram_series(
        mut self,
        labels: &[(&str, &str)],
        samples: &[(i64, FloatHistogram)],
    ) -> Result<Self, TestSourceError> {
        let label_set = LabelSet::new(
            labels
                .iter()
                .map(|(name, value)| Label {
                    name: (*name).to_string(),
                    value: (*value).to_string(),
                })
                .collect(),
        )
        .map_err(TestSourceError::Labels)?;

        let mut samples: Vec<HistogramSample> = samples
            .iter()
            .map(|(ts_ns, value)| HistogramSample {
                ts_ns: *ts_ns,
                value: value.clone(),
            })
            .collect();
        samples.sort_by_key(|s| s.ts_ns);

        self.histogram_series.push(HistogramSeriesData {
            labels: label_set,
            samples,
        });
        Ok(self)
    }
}

impl SeriesSource for TestSource {
    fn query(
        &self,
        matchers: &[LabelMatcher],
        window: TimeRange,
    ) -> Result<Vec<SeriesData>, SourceError> {
        let out = self
            .series
            .iter()
            .filter(|s| matchers::matches_series(matchers, &s.labels))
            .map(|s| {
                let samples = s
                    .samples
                    .iter()
                    .filter(|sample| {
                        sample.ts_ns >= window.start_ns && sample.ts_ns <= window.end_ns
                    })
                    .copied()
                    .collect();
                SeriesData {
                    labels: s.labels.clone(),
                    samples,
                }
            })
            .collect();
        Ok(out)
    }

    fn query_histograms(
        &self,
        matchers: &[LabelMatcher],
        window: TimeRange,
    ) -> Result<Vec<HistogramSeriesData>, SourceError> {
        let out = self
            .histogram_series
            .iter()
            .filter(|s| matchers::matches_series(matchers, &s.labels))
            .map(|s| {
                let samples = s
                    .samples
                    .iter()
                    .filter(|sample| {
                        sample.ts_ns >= window.start_ns && sample.ts_ns <= window.end_ns
                    })
                    .cloned()
                    .collect();
                HistogramSeriesData {
                    labels: s.labels.clone(),
                    samples,
                }
            })
            .collect();
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_types::TimeRange;

    #[test]
    fn sorts_samples_by_timestamp_regardless_of_input_order() {
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(30, 3.0), (10, 1.0), (20, 2.0)])
            .expect("valid series");
        let result = source
            .query(
                &[],
                TimeRange {
                    start_ns: 0,
                    end_ns: 100,
                },
            )
            .expect("query succeeds");
        let ts: Vec<i64> = result[0].samples.iter().map(|s| s.ts_ns).collect();
        assert_eq!(ts, vec![10, 20, 30]);
    }

    #[test]
    fn rejects_duplicate_label_names() {
        let err = TestSource::new().with_series(&[("job", "a"), ("job", "b")], &[]);
        assert!(matches!(err, Err(TestSourceError::Labels(_))));
    }

    #[test]
    fn window_filters_samples_inclusive_both_ends() {
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(0, 1.0), (50, 2.0), (100, 3.0)])
            .expect("valid series");
        let result = source
            .query(
                &[],
                TimeRange {
                    start_ns: 0,
                    end_ns: 50,
                },
            )
            .expect("query succeeds");
        let values: Vec<f64> = result[0].samples.iter().map(|s| s.value).collect();
        assert_eq!(values, vec![1.0, 2.0]);
    }
}
