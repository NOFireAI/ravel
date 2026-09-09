//! Condition evaluation over already-evaluated query results.
//!
//! This crate never runs PromQL or SQL. The caller (the evaluator loop)
//! executes the query against a real `QueryEngine`/`SqlExecutor`, summarizes
//! the result into a [`QueryResultSummary`], and hands it here. That keeps the
//! firing decision pure and fully unit-testable with no query engine present.

use crate::error::AlertError;
use crate::rule::{RuleCondition, ThresholdOp};

/// A caller-supplied summary of an already-evaluated query result, in the
/// minimal shape each condition needs.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryResultSummary {
    /// PromQL-shaped: the instant value of every series in the result vector.
    /// The threshold condition tests these; an empty vector means no series
    /// matched and the condition cannot be met.
    Numeric(Vec<f64>),
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

/// Decides whether `condition` is met by `result`.
///
/// Returns [`AlertError::ResultShapeMismatch`] when the condition and the
/// result shape do not belong together (a threshold against a row count, or a
/// nonempty-result against a numeric vector): that pairing is a caller bug, not
/// a "not firing" answer, so it is surfaced rather than silently treated as
/// false.
pub fn condition_met(
    condition: &RuleCondition,
    result: &QueryResultSummary,
) -> Result<bool, AlertError> {
    match (condition, result) {
        (RuleCondition::Threshold { op, threshold }, QueryResultSummary::Numeric(values)) => {
            Ok(values.iter().any(|v| compare(*op, *v, *threshold)))
        }
        (RuleCondition::NonEmptyResult, QueryResultSummary::RowCount(n)) => Ok(*n > 0),
        (condition, result) => Err(AlertError::ResultShapeMismatch {
            condition: condition.kind(),
            result: result.kind(),
        }),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn numeric(vs: &[f64]) -> QueryResultSummary {
        QueryResultSummary::Numeric(vs.to_vec())
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
