//! Filter pushdown for the `alerts` table, under the same pruning-soundness
//! invariant the `logs` and metrics pushdowns obey (crate::logs_pushdown,
//! crate::pushdown): pruning may only ever *widen* the read set relative to the
//! query's true need, never narrow it. `AlertsTableProvider::supports_filters_pushdown`
//! returns `Inexact` for every filter, so DataFusion always re-applies the
//! originals above the scan.
//!
//! Recognized shapes (everything else contributes nothing and widens):
//!
//! - **ts range**: top-level AND conjuncts of the bare `ts_ns` column compared
//!   to a literal timestamp (`>=`, `>`, `<`, `<=`, `=`), and `BETWEEN`. Feeds
//!   segment-level pruning and the fetch's [`LogQuery`] ts bounds.
//! - **`alert_id = '<hex>'` / `rule_id = '<name>'` equality fast path**: an
//!   equality of the promoted `alert_id` or `rule_id` column against a UTF-8
//!   literal becomes a [`Predicate::Equals`] on the corresponding record
//!   attribute, handed straight to `RlogReader::scan`. Unlike the `logs` table's
//!   stream-attribute equalities (which are unsound to push as a stream-level
//!   prune against the merged `attrs` column), `alert_id`/`rule_id` are genuine
//!   per-record attributes of an alert record (ADR-0040 decision 2), so
//!   [`Predicate::Equals`] on a `FieldSel::Attr` is evaluated *exactly* per row
//!   by the reader (crate `ravel-logseg`, `reader.rs`) -- it prunes blocks by
//!   the attribute bloom and re-checks each surviving row. Pushing it therefore
//!   narrows correctly, and the `Inexact` residual re-applies it regardless.
//!
//! [`LogQuery`]: ravel_query::LogQuery

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use ravel_logseg::{AttrValue, FieldSel, Predicate};

use crate::alerts_schema::{ALERT_ID_KEY, RULE_ID_KEY};

/// Everything the extractor pulled out of an `alerts` filter set, all widen-only.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct AlertsPushdown {
    /// Inclusive lower bound on `ts_ns` in nanoseconds, if provably required.
    pub ts_lo: Option<i64>,
    /// Inclusive upper bound on `ts_ns` in nanoseconds, if provably required.
    pub ts_hi: Option<i64>,
    /// Exact per-record attribute equalities (`alert_id`/`rule_id`) handed to
    /// `RlogReader::scan`, applied exactly there.
    pub content: Vec<Predicate>,
}

impl AlertsPushdown {
    /// The inclusive lower ts bound for the fetch's [`LogQuery`]: the extracted
    /// bound, or `i64::MIN` when none was proven (scan everything).
    ///
    /// [`LogQuery`]: ravel_query::LogQuery
    pub fn ts_min(&self) -> i64 {
        self.ts_lo.unwrap_or(i64::MIN)
    }

    /// The inclusive upper ts bound for the fetch's [`LogQuery`]: the extracted
    /// bound, or `i64::MAX` when none was proven.
    ///
    /// [`LogQuery`]: ravel_query::LogQuery
    pub fn ts_max(&self) -> i64 {
        self.ts_hi.unwrap_or(i64::MAX)
    }
}

/// Extract all sound pushdown from an `alerts` filter set. Each element of
/// `filters` is an implicit top-level AND conjunct; nested `AND`s are split too.
pub fn extract_alerts(filters: &[Expr]) -> AlertsPushdown {
    let mut out = AlertsPushdown::default();
    for f in filters {
        walk_conjunct(f, &mut out);
    }
    out
}

fn walk_conjunct(expr: &Expr, out: &mut AlertsPushdown) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr
        && *op == Operator::And
    {
        walk_conjunct(left, out);
        walk_conjunct(right, out);
        return;
    }
    handle_leaf(expr, out);
}

fn handle_leaf(expr: &Expr, out: &mut AlertsPushdown) {
    match expr {
        Expr::BinaryExpr(be) => handle_binary(be, out),
        // BETWEEN low AND high desugars to `ts_ns >= low AND ts_ns <= high`. A
        // negated BETWEEN is an OR of two ranges: not sound to narrow.
        Expr::Between(bt) if !bt.negated && is_ts_col(&bt.expr) => {
            if let Some(lo) = lit_ts_ns(&bt.low) {
                tighten_lo(out, lo);
            }
            if let Some(hi) = lit_ts_ns(&bt.high) {
                tighten_hi(out, hi);
            }
        }
        _ => {}
    }
}

fn handle_binary(be: &BinaryExpr, out: &mut AlertsPushdown) {
    // ts_ns vs literal timestamp comparison (either operand order).
    if let Some((op, ts_ns)) = ts_comparison(be) {
        apply_ts_bound(out, op, ts_ns);
        return;
    }
    // `alert_id = '<hex>'` / `rule_id = '<name>'` equality fast path.
    if let Some(pred) = attr_equality(be) {
        out.content.push(pred);
    }
}

/// An equality of a promoted string column (`alert_id`/`rule_id`) against a
/// UTF-8 literal, lowered to a [`Predicate::Equals`] on the record attribute the
/// column promotes. Either operand order is accepted. Any other column, a
/// non-`Eq` operator, or a non-string literal contributes nothing.
fn attr_equality(be: &BinaryExpr) -> Option<Predicate> {
    if be.op != Operator::Eq {
        return None;
    }
    let (key, lit) = match (attr_key(&be.left), attr_key(&be.right)) {
        (Some(k), None) => (k, &be.right),
        (None, Some(k)) => (k, &be.left),
        _ => return None,
    };
    let value = lit_utf8(lit)?;
    Some(Predicate::Equals {
        field: FieldSel::Attr(key.to_string()),
        value: AttrValue::Str(value),
    })
}

/// The record attribute key a column name promotes, for the equality fast path.
fn attr_key(e: &Expr) -> Option<&'static str> {
    match e {
        Expr::Column(c) if c.name == "alert_id" => Some(ALERT_ID_KEY),
        Expr::Column(c) if c.name == "rule_id" => Some(RULE_ID_KEY),
        _ => None,
    }
}

// --- ts bound extraction (mirrors crate::logs_pushdown) ---

fn ts_comparison(be: &BinaryExpr) -> Option<(Operator, i64)> {
    let (op, ns) = if is_ts_col(&be.left) {
        (be.op, lit_ts_ns(&be.right)?)
    } else if is_ts_col(&be.right) {
        (flip_op(be.op)?, lit_ts_ns(&be.left)?)
    } else {
        return None;
    };
    match op {
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq => {
            Some((op, ns))
        }
        _ => None,
    }
}

fn apply_ts_bound(out: &mut AlertsPushdown, op: Operator, ns: i64) {
    match op {
        Operator::GtEq => tighten_lo(out, ns),
        // `ts_ns > L` is `ts_ns >= L+1` in integer ns; on overflow drop the bound.
        Operator::Gt => {
            if let Some(lo) = ns.checked_add(1) {
                tighten_lo(out, lo);
            }
        }
        Operator::LtEq => tighten_hi(out, ns),
        // `ts_ns < U` is `ts_ns <= U-1`; on underflow drop the bound.
        Operator::Lt => {
            if let Some(hi) = ns.checked_sub(1) {
                tighten_hi(out, hi);
            }
        }
        Operator::Eq => {
            tighten_lo(out, ns);
            tighten_hi(out, ns);
        }
        _ => {}
    }
}

fn tighten_lo(out: &mut AlertsPushdown, candidate: i64) {
    out.ts_lo = Some(match out.ts_lo {
        Some(cur) => cur.max(candidate),
        None => candidate,
    });
}

fn tighten_hi(out: &mut AlertsPushdown, candidate: i64) {
    out.ts_hi = Some(match out.ts_hi {
        Some(cur) => cur.min(candidate),
        None => candidate,
    });
}

fn is_ts_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "ts_ns")
}

/// A literal timestamp in nanoseconds, or `None` for any non-timestamp or
/// non-literal expression. Integer literals are rejected (ambiguous scale). On
/// overflow the literal is rejected (widen). Identical to the `logs` extractor.
fn lit_ts_ns(e: &Expr) -> Option<i64> {
    let sv = match e {
        Expr::Literal(sv, _) => sv,
        _ => return None,
    };
    match sv {
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => v.checked_mul(1_000),
        ScalarValue::TimestampMillisecond(Some(v), _) => v.checked_mul(1_000_000),
        ScalarValue::TimestampSecond(Some(v), _) => v.checked_mul(1_000_000_000),
        _ => None,
    }
}

fn flip_op(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Eq => Operator::Eq,
        _ => return None,
    })
}

fn lit_utf8(e: &Expr) -> Option<String> {
    match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::logical_expr::{and, or};
    use datafusion::prelude::{col, lit};

    use super::*;

    fn ts_lit(v: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(v), None))
    }

    #[test]
    fn ts_bounds_and_between() {
        let p = extract_alerts(&[
            col("ts_ns").gt_eq(ts_lit(100)),
            col("ts_ns").lt(ts_lit(200)),
        ]);
        assert_eq!((p.ts_lo, p.ts_hi), (Some(100), Some(199)));
        assert_eq!((p.ts_min(), p.ts_max()), (100, 199));

        let e = Expr::Between(datafusion::logical_expr::Between {
            expr: Box::new(col("ts_ns")),
            negated: false,
            low: Box::new(ts_lit(10)),
            high: Box::new(ts_lit(20)),
        });
        let p = extract_alerts(&[e]);
        assert_eq!((p.ts_lo, p.ts_hi), (Some(10), Some(20)));
    }

    #[test]
    fn alert_id_and_rule_id_equality_become_content_predicates() {
        // Column-on-left and literal-on-right.
        let p = extract_alerts(&[col("alert_id").eq(lit("deadbeef"))]);
        assert_eq!(
            p.content,
            vec![Predicate::Equals {
                field: FieldSel::Attr("alert_id".into()),
                value: AttrValue::Str("deadbeef".into())
            }]
        );

        // Literal-on-left and column-on-right (flipped operand order).
        let p = extract_alerts(&[lit("high-latency").eq(col("rule_id"))]);
        assert_eq!(
            p.content,
            vec![Predicate::Equals {
                field: FieldSel::Attr("rule_id".into()),
                value: AttrValue::Str("high-latency".into())
            }]
        );
    }

    #[test]
    fn non_string_or_unrecognized_equality_contributes_nothing() {
        // A non-string literal is not pushed.
        let p = extract_alerts(&[col("alert_id").eq(lit(5i64))]);
        assert!(p.content.is_empty());
        // A column that is not a promoted fast-path column contributes nothing.
        let p = extract_alerts(&[col("state").eq(lit("firing"))]);
        assert!(p.content.is_empty());
    }

    #[test]
    fn or_of_bounds_or_equalities_contributes_nothing() {
        // An OR anywhere at the top level contributes no bound and no predicate.
        let range = and(
            col("ts_ns").gt_eq(ts_lit(100)),
            col("ts_ns").lt(ts_lit(200)),
        );
        let mixed = or(range, col("alert_id").eq(lit("x")));
        let p = extract_alerts(&[mixed]);
        assert_eq!(p, AlertsPushdown::default());
    }
}
