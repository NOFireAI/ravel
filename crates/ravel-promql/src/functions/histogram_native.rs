//! Native-histogram scalar-extraction functions (plan section 8/P11):
//! `histogram_count`, `histogram_sum`, `histogram_avg`. Each maps an instant
//! vector of native-histogram elements to floats, one output per histogram
//! element, dropping `__name__` (every function result drops the metric name).
//!
//! Mirroring Prometheus (`promql/functions.go` `funcHistogramCount` etc.):
//! these operate on native-histogram samples only. A float element in the
//! input is dropped (Prometheus emits an info annotation and skips it; this
//! crate has no annotation channel yet, see the P11 report and #178). So a
//! `histogram_count(some_float_series)` yields an empty vector here, matching
//! Prometheus' value behavior (empty result) if not its annotation.

use crate::eval::{InstantSample, InstantVector, drop_metric_name};

use super::{FunctionDef, FunctionKind};

pub(crate) const FUNCTIONS: &[FunctionDef] = &[
    FunctionDef {
        name: "histogram_count",
        kind: FunctionKind::VectorMap(histogram_count),
    },
    FunctionDef {
        name: "histogram_sum",
        kind: FunctionKind::VectorMap(histogram_sum),
    },
    FunctionDef {
        name: "histogram_avg",
        kind: FunctionKind::VectorMap(histogram_avg),
    },
];

/// Apply `extract` to every native-histogram element, producing a float
/// element with `__name__` dropped; float elements are skipped.
fn map_histograms(
    v: InstantVector,
    extract: impl Fn(&crate::histogram::FloatHistogram) -> f64,
) -> InstantVector {
    v.into_iter()
        .filter_map(|s| {
            s.histogram.as_ref().map(|h| {
                InstantSample::scalar(
                    drop_metric_name(s.labels.clone()),
                    s.ts_ns,
                    s.ts_ns,
                    extract(h),
                )
            })
        })
        .collect()
}

fn histogram_count(v: InstantVector) -> InstantVector {
    map_histograms(v, |h| h.observation_count())
}

fn histogram_sum(v: InstantVector) -> InstantVector {
    map_histograms(v, |h| h.observation_sum())
}

/// `histogram_avg` is `sum / count` (Prometheus' `funcHistogramAvg`).
fn histogram_avg(v: InstantVector) -> InstantVector {
    map_histograms(v, |h| h.observation_sum() / h.observation_count())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::eval::Evaluator;
    use crate::histogram::{FloatHistogram, ResetHint, Span};
    use crate::testsource::TestSource;

    fn hist(count: f64, sum: f64) -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: ResetHint::Unknown,
            scale: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            negative_spans: Vec::new(),
            positive_buckets: vec![count],
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        }
    }

    fn eval_one(query: &str, src: &TestSource) -> Vec<(Vec<(String, String)>, f64)> {
        Evaluator::new()
            .instant(src, query, 0)
            .expect("evaluates")
            .into_iter()
            .map(|s| {
                (
                    s.labels
                        .iter()
                        .map(|l| (l.name.clone(), l.value.clone()))
                        .collect(),
                    s.value,
                )
            })
            .collect()
    }

    fn source() -> TestSource {
        TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("job", "a")], &[(0, hist(6.0, 42.0))])
            .expect("valid histogram series")
    }

    #[test]
    fn histogram_count_reads_count_and_drops_name() {
        let got = eval_one("histogram_count(h)", &source());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1, 6.0);
        assert!(got[0].0.iter().all(|(n, _)| n != "__name__"));
        assert!(got[0].0.iter().any(|(n, v)| n == "job" && v == "a"));
    }

    #[test]
    fn histogram_sum_reads_sum() {
        assert_eq!(eval_one("histogram_sum(h)", &source())[0].1, 42.0);
    }

    #[test]
    fn histogram_avg_is_sum_over_count() {
        assert_eq!(eval_one("histogram_avg(h)", &source())[0].1, 7.0);
    }

    #[test]
    fn histogram_count_of_a_float_series_is_empty() {
        let src = TestSource::new()
            .with_series(&[("__name__", "f")], &[(0, 1.0)])
            .expect("valid series");
        assert!(eval_one("histogram_count(f)", &src).is_empty());
    }
}
