//! Filter pushdown for the `audit` table, under the same pruning-soundness
//! invariant every other table obeys (crate::logs_pushdown, crate::pushdown):
//! pruning may only ever *widen* the read set relative to the query's true need,
//! never narrow it. `AuditTableProvider::supports_filters_pushdown` returns
//! `Inexact` for every filter, so DataFusion always re-applies the originals
//! above the scan.
//!
//! Only the `ts_ns` range is recognized: top-level AND conjuncts of the bare
//! `ts_ns` column compared to a literal timestamp (`>=`, `>`, `<`, `<=`, `=`),
//! and `BETWEEN`. This feeds segment-level pruning and the fetch's [`LogQuery`]
//! ts bounds. The `audit` table promotes no record-specific field into a typed
//! column, so it has no equality fast path (unlike `alerts`): every attribute
//! predicate (`attrs['kind'] = 'legal_hold'`, ...) is evaluated by DataFusion's
//! residual over the merged `attrs` column.
//!
//! [`LogQuery`]: ravel_query::LogQuery

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

/// Everything the extractor pulled out of an `audit` filter set, all widen-only.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct AuditPushdown {
    /// Inclusive lower bound on `ts_ns` in nanoseconds, if provably required.
    pub ts_lo: Option<i64>,
    /// Inclusive upper bound on `ts_ns` in nanoseconds, if provably required.
    pub ts_hi: Option<i64>,
}

impl AuditPushdown {
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

/// Extract all sound pushdown from an `audit` filter set. Each element of
/// `filters` is an implicit top-level AND conjunct; nested `AND`s are split too.
pub fn extract_audit(filters: &[Expr]) -> AuditPushdown {
    let mut out = AuditPushdown::default();
    for f in filters {
        walk_conjunct(f, &mut out);
    }
    out
}

fn walk_conjunct(expr: &Expr, out: &mut AuditPushdown) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr
        && *op == Operator::And
    {
        walk_conjunct(left, out);
        walk_conjunct(right, out);
        return;
    }
    handle_leaf(expr, out);
}

fn handle_leaf(expr: &Expr, out: &mut AuditPushdown) {
    match expr {
        Expr::BinaryExpr(be) => {
            if let Some((op, ts_ns)) = ts_comparison(be) {
                apply_ts_bound(out, op, ts_ns);
            }
        }
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

fn apply_ts_bound(out: &mut AuditPushdown, op: Operator, ns: i64) {
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

fn tighten_lo(out: &mut AuditPushdown, candidate: i64) {
    out.ts_lo = Some(match out.ts_lo {
        Some(cur) => cur.max(candidate),
        None => candidate,
    });
}

fn tighten_hi(out: &mut AuditPushdown, candidate: i64) {
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
        let p = extract_audit(&[
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
        let p = extract_audit(&[e]);
        assert_eq!((p.ts_lo, p.ts_hi), (Some(10), Some(20)));
    }

    #[test]
    fn attribute_equalities_are_not_pushed() {
        // The audit table has no equality fast path: an attribute predicate is
        // the residual's job, and contributes no ts bound here.
        let p = extract_audit(&[col("attrs").eq(lit("x")), col("body").eq(lit("y"))]);
        assert_eq!(p, AuditPushdown::default());
    }

    #[test]
    fn or_of_bounds_contributes_nothing() {
        let range = and(
            col("ts_ns").gt_eq(ts_lit(100)),
            col("ts_ns").lt(ts_lit(200)),
        );
        let mixed = or(range, col("severity_text").eq(lit("ERROR")));
        let p = extract_audit(&[mixed]);
        assert_eq!((p.ts_lo, p.ts_hi), (None, None));
    }
}
