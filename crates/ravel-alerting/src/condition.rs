//! Condition evaluation over already-evaluated query results.
//!
//! This crate never runs PromQL or SQL. The caller (the evaluator loop)
//! executes the query against a real `QueryEngine`/`SqlExecutor`, summarizes
//! the result into a [`QueryResultSummary`], and hands it here. That keeps the
//! firing decision pure and fully unit-testable with no query engine present.

use ravel_types::LabelSet;

use crate::error::AlertError;
use crate::rule::{RuleCondition, ThresholdOp};

/// A caller-supplied summary of an already-evaluated query result, in the
/// minimal shape each condition needs.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryResultSummary {
    /// PromQL-shaped: every series in the result vector with its labels and
    /// instant value. A scalar result is one series with an empty label set.
    /// The threshold condition tests each value; an empty vector means no
    /// series matched and the condition cannot be met.
    Numeric(Vec<(LabelSet, f64)>),
    /// SQL-shaped: the number of rows the query returned. The nonempty-result
    /// condition tests this.
    RowCount(u64),
}

impl QueryResultSummary {
    /// A short static name for error messages.
    fn kind(&self) -> &'static str {
        match self {
            QueryResultSummary::Numeric(_) => "numeric",
            QueryResultSummary::RowCount(_) => "row-count",
        }
    }
}

/// Applies `op` between a series value and the threshold. `Eq`/`Ne` use exact
/// IEEE-754 comparison. A `NaN` series value satisfies none of the five
/// ordering-shaped comparators (`>`, `>=`, `<`, `<=`, `==`), but DOES satisfy
/// `Ne`: IEEE-754 defines `NaN != x` as true for every `x`, including `NaN`
/// itself, and this repo's own PromQL evaluator already relies on exactly
/// that (`crates/ravel-promql/src/binop.rs`'s `!=` keeps a NaN sample rather
/// than dropping it). Matching that existing behavior here, not inventing a
/// different NaN rule for alerting, is the deliberate choice.
fn compare(op: ThresholdOp, value: f64, threshold: f64) -> bool {
    match op {
        ThresholdOp::Gt => value > threshold,
        ThresholdOp::Ge => value >= threshold,
        ThresholdOp::Lt => value < threshold,
        ThresholdOp::Le => value <= threshold,
        ThresholdOp::Eq => value == threshold,
        ThresholdOp::Ne => value != threshold,
    }
}

/// Returns the series of `result` that satisfy `condition`, each with its
/// labels, in result order (ADR-0117 decision 1).
///
/// A threshold keeps every series whose value satisfies the comparator. A SQL
/// result has no series identity (ADR-0117 decision 5), so a nonempty-result
/// condition that holds matches as one series with an empty label set and the
/// row count as its value.
///
/// Returns [`AlertError::ResultShapeMismatch`] when the condition and the
/// result shape do not belong together (a threshold against a row count, or a
/// nonempty-result against a numeric vector): that pairing is a caller bug, not
/// a "not firing" answer, so it is surfaced rather than silently treated as
/// no match.
pub fn matching_series(
    condition: &RuleCondition,
    result: &QueryResultSummary,
) -> Result<Vec<(LabelSet, f64)>, AlertError> {
    match (condition, result) {
        (RuleCondition::Threshold { op, threshold }, QueryResultSummary::Numeric(series)) => {
            Ok(series
                .iter()
                .filter(|(_, v)| compare(*op, *v, *threshold))
                .cloned()
                .collect())
        }
        (RuleCondition::NonEmptyResult, QueryResultSummary::RowCount(n)) => Ok(if *n > 0 {
            vec![(LabelSet::default(), *n as f64)]
        } else {
            Vec::new()
        }),
        (condition, result) => Err(AlertError::ResultShapeMismatch {
            condition: condition.kind(),
            result: result.kind(),
        }),
    }
}

/// Decides whether `condition` is met by `result`: whether
/// [`matching_series`] matches at least one series. Same errors.
pub fn condition_met(
    condition: &RuleCondition,
    result: &QueryResultSummary,
) -> Result<bool, AlertError> {
    Ok(!matching_series(condition, result)?.is_empty())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_types::Label;

    fn numeric(vs: &[f64]) -> QueryResultSummary {
        QueryResultSummary::Numeric(vs.iter().map(|v| (LabelSet::default(), *v)).collect())
    }

    fn series(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(name, value)| Label {
                    name: (*name).to_string(),
                    value: (*value).to_string(),
                })
                .collect(),
        )
        .expect("distinct label names")
    }

    #[test]
    fn ten_series_vector_yields_ten_matches_each_with_its_own_instance_label() {
        // Twenty `up` series: the ten even-numbered hosts are down (0.0) and
        // the ten odd-numbered ones are up (1.0), interleaved so the matcher
        // has to filter rather than take a prefix.
        let result = QueryResultSummary::Numeric(
            (0..20)
                .map(|i| {
                    let instance = format!("host-{i:02}");
                    let value = if i % 2 == 0 { 0.0 } else { 1.0 };
                    (
                        series(&[("__name__", "up"), ("instance", instance.as_str())]),
                        value,
                    )
                })
                .collect(),
        );
        let cond = RuleCondition::Threshold {
            op: ThresholdOp::Eq,
            threshold: 0.0,
        };

        let matched = matching_series(&cond, &result).expect("threshold over numeric");

        let instances: Vec<&str> = matched
            .iter()
            .map(|(labels, _)| labels.get("instance").expect("instance label kept"))
            .collect();
        assert_eq!(
            instances,
            vec![
                "host-00", "host-02", "host-04", "host-06", "host-08", "host-10", "host-12",
                "host-14", "host-16", "host-18",
            ]
        );
        for (labels, value) in &matched {
            assert_eq!(value.to_bits(), 0.0f64.to_bits());
            assert_eq!(labels.get("__name__"), Some("up"));
            assert_eq!(labels.len(), 2);
        }
    }

    #[test]
    fn threshold_fires_when_any_series_exceeds() {
        let cond = RuleCondition::Threshold {
            op: ThresholdOp::Gt,
            threshold: 10.0,
        };
        assert!(condition_met(&cond, &numeric(&[1.0, 5.0, 11.0])).expect("met"));
        assert!(!condition_met(&cond, &numeric(&[1.0, 5.0, 9.0])).expect("met"));
        // Boundary: strictly greater does not fire at exactly the threshold.
        assert!(!condition_met(&cond, &numeric(&[10.0])).expect("met"));
    }

    #[test]
    fn threshold_covers_every_comparator() {
        let met = |op, vs: &[f64]| {
            condition_met(
                &RuleCondition::Threshold { op, threshold: 5.0 },
                &numeric(vs),
            )
            .expect("met")
        };
        assert!(met(ThresholdOp::Ge, &[5.0]));
        assert!(met(ThresholdOp::Lt, &[4.9]));
        assert!(met(ThresholdOp::Le, &[5.0]));
        assert!(met(ThresholdOp::Eq, &[5.0]));
        assert!(!met(ThresholdOp::Eq, &[5.1]));
        assert!(met(ThresholdOp::Ne, &[5.1]));
    }

    #[test]
    fn empty_numeric_result_never_fires() {
        let cond = RuleCondition::Threshold {
            op: ThresholdOp::Gt,
            threshold: 0.0,
        };
        assert!(!condition_met(&cond, &numeric(&[])).expect("met"));
    }

    #[test]
    fn nan_series_value_never_fires_ordering_comparators() {
        for op in [
            ThresholdOp::Gt,
            ThresholdOp::Ge,
            ThresholdOp::Lt,
            ThresholdOp::Le,
            ThresholdOp::Eq,
        ] {
            let cond = RuleCondition::Threshold { op, threshold: 0.0 };
            assert!(
                !condition_met(&cond, &numeric(&[f64::NAN])).expect("met"),
                "NaN must not fire {op:?}"
            );
        }
    }

    #[test]
    fn nan_series_value_does_fire_ne() {
        // IEEE-754: NaN != x is true for every x, including NaN itself. This
        // repo's own PromQL evaluator already relies on the same rule
        // (binop.rs's `!=` keeps a NaN sample); alerting matches it rather
        // than inventing a different NaN rule for Ne alone.
        let cond = RuleCondition::Threshold {
            op: ThresholdOp::Ne,
            threshold: 0.0,
        };
        assert!(
            condition_met(&cond, &numeric(&[f64::NAN])).expect("met"),
            "NaN must fire Ne, matching IEEE-754 and this repo's own PromQL != semantics"
        );
    }

    #[test]
    fn nonempty_result_fires_on_any_row() {
        assert!(
            condition_met(
                &RuleCondition::NonEmptyResult,
                &QueryResultSummary::RowCount(1)
            )
            .expect("met")
        );
        assert!(
            !condition_met(
                &RuleCondition::NonEmptyResult,
                &QueryResultSummary::RowCount(0)
            )
            .expect("met")
        );
    }

    #[test]
    fn mismatched_condition_and_result_shape_is_a_typed_error() {
        // Threshold against a row count.
        let err = condition_met(
            &RuleCondition::Threshold {
                op: ThresholdOp::Gt,
                threshold: 1.0,
            },
            &QueryResultSummary::RowCount(3),
        );
        assert!(matches!(err, Err(AlertError::ResultShapeMismatch { .. })));

        // Nonempty-result against a numeric vector.
        let err = condition_met(&RuleCondition::NonEmptyResult, &numeric(&[1.0]));
        assert!(matches!(err, Err(AlertError::ResultShapeMismatch { .. })));
    }
}
