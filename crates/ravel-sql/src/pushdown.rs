//! Filter pushdown under the pruning soundness invariant
//! (docs/arrow-datafusion-plan.md section 2 "Filter pushdown and the pruning
//! soundness invariant", review F8).
//!
//! Source pruning (choosing what to fetch) is fundamentally different from
//! residual filtering. A residual filter that misfires only wastes work:
//! DataFusion re-applies every filter above the scan, so rows are never lost.
//! A prune that drops a segment or a series drops rows DataFusion never sees,
//! and no re-applied filter can recover them. So every prune here must select
//! a *superset* of the rows the query needs: pruning may only ever widen the
//! read set relative to the true need, never narrow it.
//!
//! The extractor therefore recognizes only shapes it can prove sound and
//! contributes nothing (leaving the full read set) for everything else:
//!
//! - Time bounds fire only on top-level AND conjuncts of the bare `ts` column
//!   compared to a literal timestamp (`>=`, `>`, `<`, `<=`, `=`), with
//!   `BETWEEN` handled as its desugared `>=` / `<=` pair. An OR anywhere at the
//!   top level, `ts` inside a function or cast, a non-literal or non-timestamp
//!   bound, or a mixed `(ts ... ) OR value ...` predicate all contribute no
//!   bound, so the full tenant/query time range is scanned. The failure mode is
//!   always "widen to everything", never "guess a narrower range".
//! - Label equality/negation and the `label_match` regex UDF map to the same
//!   `LabelMatcher` shape ravel-query's PromQL selectors use, which the fetcher
//!   applies against SERIES_TABLE. Each maps to a matcher that selects a
//!   superset of the SQL predicate's true row set (see the per-op notes below),
//!   so the residual re-application above the scan removes the surplus exactly.
//! - `series_id = X'..'` collects an allow-set applied as a post-fetch row
//!   filter in the scan (the fetcher's matcher API is label-only, so this
//!   cannot prune the GET itself without a ravel-query change out of scope).
//!
//! Conjuncts are combined the way AND combines them: the tightest lower and
//! upper ts bound win (max of lowers, min of uppers), matchers accumulate, and
//! series-id equalities intersect. Dropping any single conjunct only widens the
//! result, so ignoring an unrecognized shape is always sound.

use std::collections::HashSet;

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use ravel_promql::LabelMatcher;

/// The name of the `label(labels, 'k')` accessor UDF.
pub const LABEL_UDF: &str = "label";
/// The name of the `label_match(labels, 'k', 'pattern')` regex UDF.
pub const LABEL_MATCH_UDF: &str = "label_match";

/// Everything the extractor pulled out of a filter set, all of it widen-only.
#[derive(Debug, Default, Clone)]
pub struct Pushdown {
    /// Inclusive lower bound on `ts` in nanoseconds, if provably required.
    pub ts_lo: Option<i64>,
    /// Inclusive upper bound on `ts` in nanoseconds, if provably required.
    pub ts_hi: Option<i64>,
    /// Label matchers selecting a superset of the needed series.
    pub matchers: Vec<LabelMatcher>,
    /// Allowed series ids; `None` means unconstrained. A `Some` empty set means
    /// contradictory equalities (`series_id = A AND series_id = B`), i.e. no
    /// rows, which is still sound (the residual would also yield nothing).
    pub series_ids: Option<HashSet<[u8; 16]>>,
}

impl Pushdown {
    /// Whether a segment with event-time span `[min_ts, max_ts]` can hold any
    /// row inside the extracted ts bounds. `true` when no bound was extracted
    /// (widen to everything). Never narrows: a segment is dropped only when its
    /// entire span lies outside a proven-required bound.
    pub fn segment_in_range(&self, min_ts: i64, max_ts: i64) -> bool {
        if let Some(lo) = self.ts_lo
            && max_ts < lo
        {
            return false;
        }
        if let Some(hi) = self.ts_hi
            && min_ts > hi
        {
            return false;
        }
        true
    }
}

/// Extract all sound pushdown from a set of filter expressions. Each element of
/// `filters` is an implicit top-level AND conjunct (DataFusion splits `AND` for
/// us, but nested `AND`s are split here too).
pub fn extract(filters: &[Expr]) -> Pushdown {
    let mut out = Pushdown::default();
    for f in filters {
        walk_conjunct(f, &mut out);
    }
    out
}

/// Recurse through top-level `AND` nodes, handling each leaf conjunct. An `OR`
/// (or any other non-`AND` shape) is handled as a single opaque conjunct: it
/// contributes only what `handle_leaf` can prove, which for an `OR` is nothing.
fn walk_conjunct(expr: &Expr, out: &mut Pushdown) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr
        && *op == Operator::And
    {
        walk_conjunct(left, out);
        walk_conjunct(right, out);
        return;
    }
    handle_leaf(expr, out);
}

fn handle_leaf(expr: &Expr, out: &mut Pushdown) {
    match expr {
        Expr::BinaryExpr(be) => handle_binary(be, out),
        Expr::Between(bt) if !bt.negated => {
            // BETWEEN low AND high desugars to `expr >= low AND expr <= high`.
            // A negated BETWEEN is an OR of two ranges: not sound to narrow.
            if is_ts_col(&bt.expr) {
                if let Some(lo) = lit_ts_ns(&bt.low) {
                    tighten_lo(out, lo);
                }
                if let Some(hi) = lit_ts_ns(&bt.high) {
                    tighten_hi(out, hi);
                }
            }
        }
        Expr::ScalarFunction(sf) if sf.func.name() == LABEL_MATCH_UDF => {
            // A bare `label_match(labels, 'k', 'pattern')` boolean predicate.
            // The UDF implements Prometheus-anchored regex over the label,
            // treating an absent label as "", so mapping it to a `regex`
            // matcher is exact, not a narrowing approximation.
            if let Some(m) = label_match_matcher(&sf.args) {
                out.matchers.push(m);
            }
        }
        _ => {}
    }
}

fn handle_binary(be: &BinaryExpr, out: &mut Pushdown) {
    // ts vs literal timestamp comparison (either operand order).
    if let Some((op, ts_ns)) = ts_comparison(be) {
        apply_ts_bound(out, op, ts_ns);
        return;
    }
    // label(labels, 'k') = / != 'v'.
    if let Some(m) = label_eq_matcher(be) {
        out.matchers.push(m);
        return;
    }
    // series_id = X'..16 bytes..'.
    if be.op == Operator::Eq
        && let Some(id) = series_id_equality(be)
    {
        match &mut out.series_ids {
            Some(set) => set.retain(|existing| *existing == id),
            None => {
                let mut set = HashSet::new();
                set.insert(id);
                out.series_ids = Some(set);
            }
        }
    }
}

/// A comparison of the bare `ts` column against a literal timestamp, normalized
/// so `op` always reads `ts <op> literal`. Returns `None` for any other shape
/// (function-wrapped ts, non-literal bound, non-timestamp literal).
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

fn apply_ts_bound(out: &mut Pushdown, op: Operator, ns: i64) {
    match op {
        Operator::GtEq => tighten_lo(out, ns),
        // `ts > L` is `ts >= L+1` in integer ns; on overflow drop the bound
        // (widen), never fabricate one.
        Operator::Gt => {
            if let Some(lo) = ns.checked_add(1) {
                tighten_lo(out, lo);
            }
        }
        Operator::LtEq => tighten_hi(out, ns),
        // `ts < U` is `ts <= U-1`; on underflow drop the bound.
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

fn tighten_lo(out: &mut Pushdown, candidate: i64) {
    out.ts_lo = Some(match out.ts_lo {
        Some(cur) => cur.max(candidate),
        None => candidate,
    });
}

fn tighten_hi(out: &mut Pushdown, candidate: i64) {
    out.ts_hi = Some(match out.ts_hi {
        Some(cur) => cur.min(candidate),
        None => candidate,
    });
}

fn is_ts_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "ts")
}

/// A literal timestamp in nanoseconds, or `None` for any non-timestamp or
/// non-literal expression. Only unambiguous timestamp scalars are accepted;
/// integer literals are rejected because their scale is ambiguous and a
/// mis-scaled bound would narrow (lose rows). Scaling is exact integer
/// multiplication; on overflow the literal is rejected (widen).
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

/// `label(labels, 'k') = 'v'` -> equality matcher; `!=` -> negation matcher.
///
/// Both are sound supersets of the SQL predicate. The SQL `label(...)` UDF
/// returns NULL for an absent label, so `= 'v'` drops absent-label rows and
/// `!= 'v'` also drops them (NULL comparisons are not true); the matcher treats
/// an absent label as "", so the equality matcher matches exactly the same set
/// and the negation matcher keeps the absent-label rows too, i.e. a superset.
/// The surplus is removed by the residual filter DataFusion applies above the
/// scan.
fn label_eq_matcher(be: &BinaryExpr) -> Option<LabelMatcher> {
    let (func_side, lit_side) = if is_label_udf(&be.left) {
        (&be.left, &be.right)
    } else if is_label_udf(&be.right) {
        (&be.right, &be.left)
    } else {
        return None;
    };
    let key = label_udf_key(func_side)?;
    let value = lit_utf8(lit_side)?;
    match be.op {
        Operator::Eq => Some(LabelMatcher::equal(key, value)),
        Operator::NotEq => Some(LabelMatcher::not_equal(key, value)),
        _ => None,
    }
}

/// `label_match(labels, 'k', 'pattern')` -> anchored regex matcher, matching
/// the UDF's own Prometheus-anchored semantics exactly.
fn label_match_matcher(args: &[Expr]) -> Option<LabelMatcher> {
    if args.len() != 3 {
        return None;
    }
    if !is_labels_col(&args[0]) {
        return None;
    }
    let key = lit_utf8(&args[1])?;
    let pattern = lit_utf8(&args[2])?;
    // A pattern that fails to compile contributes no prune (widen); the UDF
    // itself will surface the error when the residual runs.
    LabelMatcher::regex(key, pattern).ok()
}

fn is_label_udf(e: &Expr) -> bool {
    matches!(e, Expr::ScalarFunction(sf) if sf.func.name() == LABEL_UDF)
}

/// The `'k'` argument of a `label(labels, 'k')` call, requiring the first
/// argument to be the bare `labels` column so a wrapped/aliased first arg does
/// not accidentally match.
fn label_udf_key(e: &Expr) -> Option<String> {
    let Expr::ScalarFunction(sf) = e else {
        return None;
    };
    if sf.func.name() != LABEL_UDF || sf.args.len() != 2 || !is_labels_col(&sf.args[0]) {
        return None;
    }
    lit_utf8(&sf.args[1])
}

fn is_labels_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "labels")
}

fn lit_utf8(e: &Expr) -> Option<String> {
    match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        _ => None,
    }
}

/// The 16-byte series id on the literal side of `series_id = X'..'`.
fn series_id_equality(be: &BinaryExpr) -> Option<[u8; 16]> {
    let lit = if is_series_id_col(&be.left) {
        &be.right
    } else if is_series_id_col(&be.right) {
        &be.left
    } else {
        return None;
    };
    let bytes = match lit.as_ref() {
        Expr::Literal(ScalarValue::FixedSizeBinary(16, Some(b)), _) => b,
        Expr::Literal(ScalarValue::Binary(Some(b)), _)
        | Expr::Literal(ScalarValue::LargeBinary(Some(b)), _) => b,
        _ => return None,
    };
    <[u8; 16]>::try_from(bytes.as_slice()).ok()
}

fn is_series_id_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "series_id")
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::logical_expr::{Between, and, or};
    use datafusion::prelude::{col, lit};
    use ravel_promql::MatchOp;

    use super::*;
    use crate::udf::{label_match_udf, label_udf};

    fn ts_lit(v: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(v), None))
    }

    #[test]
    fn top_level_and_conjuncts_narrow_ts() {
        // ts >= 100 AND ts < 200  ->  [100, 199]
        let p = extract(&[col("ts").gt_eq(ts_lit(100)), col("ts").lt(ts_lit(200))]);
        assert_eq!(p.ts_lo, Some(100));
        assert_eq!(p.ts_hi, Some(199));
        // Nested AND in a single expr splits identically.
        let p2 = extract(&[and(col("ts").gt_eq(ts_lit(100)), col("ts").lt(ts_lit(200)))]);
        assert_eq!((p2.ts_lo, p2.ts_hi), (Some(100), Some(199)));
    }

    #[test]
    fn all_comparison_ops_and_operand_order() {
        assert_eq!(extract(&[col("ts").gt(ts_lit(100))]).ts_lo, Some(101));
        assert_eq!(extract(&[col("ts").lt_eq(ts_lit(200))]).ts_hi, Some(200));
        let eq = extract(&[col("ts").eq(ts_lit(150))]);
        assert_eq!((eq.ts_lo, eq.ts_hi), (Some(150), Some(150)));
        // Literal on the left: 100 <= ts  ->  lo = 100.
        assert_eq!(ts_lit(100).lt_eq(col("ts")).pipe_extract().ts_lo, Some(100));
    }

    #[test]
    fn between_desugars_to_inclusive_pair() {
        let e = Expr::Between(Between {
            expr: Box::new(col("ts")),
            negated: false,
            low: Box::new(ts_lit(100)),
            high: Box::new(ts_lit(200)),
        });
        let p = extract(&[e]);
        assert_eq!((p.ts_lo, p.ts_hi), (Some(100), Some(200)));
    }

    #[test]
    fn negated_between_contributes_nothing() {
        let e = Expr::Between(Between {
            expr: Box::new(col("ts")),
            negated: true,
            low: Box::new(ts_lit(100)),
            high: Box::new(ts_lit(200)),
        });
        let p = extract(&[e]);
        assert_eq!((p.ts_lo, p.ts_hi), (None, None));
    }

    #[test]
    fn or_at_top_level_never_narrows() {
        // (ts >= 100 AND ts < 200) OR value > 5  ->  no ts bound at all.
        let range = and(col("ts").gt_eq(ts_lit(100)), col("ts").lt(ts_lit(200)));
        let mixed = or(range, col("value").gt(lit(5.0)));
        let p = extract(&[mixed]);
        assert_eq!(
            (p.ts_lo, p.ts_hi),
            (None, None),
            "a mixed OR predicate must widen to everything (review F8)"
        );
    }

    #[test]
    fn function_wrapped_ts_never_narrows() {
        // -ts >= 100 : ts is inside a function, so no bound.
        let e = Expr::Negative(Box::new(col("ts"))).gt_eq(ts_lit(100));
        let p = extract(&[e]);
        assert_eq!((p.ts_lo, p.ts_hi), (None, None));
    }

    #[test]
    fn non_literal_and_non_timestamp_bounds_ignored() {
        // ts >= value : right side is a column, not a literal.
        assert_eq!(extract(&[col("ts").gt_eq(col("value"))]).ts_lo, None);
        // ts >= 100 (integer literal): ambiguous scale, deliberately ignored.
        assert_eq!(extract(&[col("ts").gt_eq(lit(100i64))]).ts_lo, None);
    }

    #[test]
    fn label_equality_and_negation_map_to_matchers() {
        let eq = label_udf()
            .call(vec![col("labels"), lit("host")])
            .eq(lit("a"));
        let ne = label_udf()
            .call(vec![col("labels"), lit("env")])
            .not_eq(lit("prod"));
        let p = extract(&[eq, ne]);
        assert_eq!(p.matchers.len(), 2);
        assert_eq!(p.matchers[0], LabelMatcher::equal("host", "a"));
        assert_eq!(p.matchers[1], LabelMatcher::not_equal("env", "prod"));
    }

    #[test]
    fn label_match_maps_to_anchored_regex_matcher() {
        let e = label_match_udf().call(vec![col("labels"), lit("host"), lit("web.*")]);
        let p = extract(&[e]);
        assert_eq!(p.matchers.len(), 1);
        // The matcher is an anchored regex, matching the UDF's own semantics.
        assert!(matches!(p.matchers[0].op, MatchOp::Re(_)));
        assert_eq!(p.matchers[0].name, "host");
    }

    #[test]
    fn series_id_equality_collects_allow_set() {
        let id = [3u8; 16];
        let e = col("series_id").eq(lit(ScalarValue::FixedSizeBinary(16, Some(id.to_vec()))));
        let p = extract(&[e]);
        let set = p.series_ids.expect("series id allow-set");
        assert_eq!(set.len(), 1);
        assert!(set.contains(&id));
    }

    #[test]
    fn segment_in_range_is_widen_only() {
        let p = extract(&[col("ts").gt_eq(ts_lit(100)), col("ts").lt(ts_lit(200))]);
        // [100, 199]: a segment entirely below or above is dropped, one that
        // overlaps (even partially) is kept.
        assert!(!p.segment_in_range(0, 99));
        assert!(!p.segment_in_range(200, 300));
        assert!(p.segment_in_range(150, 150));
        assert!(p.segment_in_range(0, 100));
        assert!(p.segment_in_range(199, 5000));
        // No bound -> every segment kept.
        let none = Pushdown::default();
        assert!(none.segment_in_range(i64::MIN, i64::MAX));
    }

    /// Test-only helper so the literal-on-left case reads left-to-right.
    trait PipeExtract {
        fn pipe_extract(self) -> Pushdown;
    }
    impl PipeExtract for Expr {
        fn pipe_extract(self) -> Pushdown {
            extract(&[self])
        }
    }
}
