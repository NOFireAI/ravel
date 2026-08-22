//! Selector planning (ADR-0021 §1): a pass over the whole PromQL AST that
//! reports every selector's resolved matchers, fetch window, and offset/`@`
//! timestamp, independent of whether the evaluator itself currently
//! implements the surrounding construct. A future phase's query engine uses
//! this to prefetch every window a query needs in one pass instead of
//! resolving one selector's window at a time (the current prefetch
//! contract).
//!
//! This module deliberately walks constructs `eval::Evaluator` still
//! rejects (`Aggregate`, `Binary`, `Call`, `Subquery`, `Extension`): a plan
//! is a static property of the query text, not of what this evaluator phase
//! can execute, so it must not regress when the executable grammar grows.

use promql_parser::parser::{Expr, VectorSelector};

use crate::eval::{self, Error};
use crate::source::LabelMatcher;

/// One selector found anywhere in a query, with its fetch window and
/// lookup-time anchoring fully resolved against the enclosing query's
/// parameters and any ancestor subquery/`@`/offset.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectorPlan {
    /// The selector's resolved matchers, including the implicit
    /// `__name__` matcher for a bare metric name.
    pub matchers: Vec<LabelMatcher>,
    /// Total window this selector's own lookback/range needs, in
    /// nanoseconds: the default instant lookback, the matrix selector's
    /// own `range`, plus any ancestor subquery ranges accumulated
    /// additively on the way up (ADR-0021 §1: "report every selector's
    /// range").
    pub range_ns: i64,
    /// This selector's own `offset` (positive: look backward), already
    /// combined with any ancestor subquery offset (offsets accumulate
    /// additively through nesting, per [`PlanAnchor::Window`]).
    pub offset_ns: i64,
    /// The resolved anchor for this selector's lookup instant.
    pub anchor: PlanAnchor,
    /// ADR-0103 aggregation-pushdown tag: `true` only for the single
    /// `SelectorPlan` produced by a `count_over_time` call whose matrix
    /// argument, after unwrapping parentheses, is a literal matrix selector
    /// (never a subquery). This is a purely structural, AST-shape-only marker
    /// set by [`plan_walk`]'s `Expr::Call` arm; it records nothing about the
    /// concrete reduction window (a `PlanAnchor::Window` selector has no
    /// concrete timestamp until a query's evaluation window is known at fetch
    /// time). Every other selector -- a different range function, a subquery
    /// argument, or a bare selector with no wrapping call -- leaves it `false`.
    pub count_over_time_pushdown_candidate: bool,
}

/// How a selector's lookup instant is anchored, after folding every `@`
/// modifier and offset found from the selector up through its ancestors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanAnchor {
    /// No `@` anywhere in the selector's ancestry: the lookup instant is
    /// the ambient per-step evaluation time, shifted by `offset_ns`
    /// (applied by the caller once it knows the step).
    Window,
    /// An `@` (the selector's own, or the innermost ancestor's if the
    /// selector itself has none) pins the lookup instant to this absolute
    /// nanosecond timestamp, already offset-adjusted. Constant across
    /// every step of a range query.
    Pinned(i64),
}

/// Walk `query`'s whole AST and report every selector found, resolving each
/// one's window/offset/@ against `eval_start_ms`/`eval_end_ms` (the query's
/// own parameters; for an instant query these are equal).
pub fn plan_selectors(
    query: &str,
    eval_start_ms: i64,
    eval_end_ms: i64,
) -> Result<Vec<SelectorPlan>, Error> {
    let expr = promql_parser::parser::parse(query).map_err(Error::Parse)?;
    let start_ns = eval::ms_to_ns(eval_start_ms)?;
    let end_ns = eval::ms_to_ns(eval_end_ms)?;

    let mut out = Vec::new();
    plan_walk(&expr, 0, 0, None, start_ns, end_ns, &mut out)?;
    Ok(out)
}

/// Recursive walk. `ancestor_range_ns`/`ancestor_offset_ns` accumulate
/// additively through subqueries; `ancestor_anchor` carries the innermost
/// ancestor `@` seen so far (a selector's own `@`, if present, always wins
/// over it).
fn plan_walk(
    expr: &Expr,
    ancestor_range_ns: i64,
    ancestor_offset_ns: i64,
    ancestor_anchor: Option<i64>,
    query_start_ns: i64,
    query_end_ns: i64,
    out: &mut Vec<SelectorPlan>,
) -> Result<(), Error> {
    match expr {
        Expr::VectorSelector(vs) => {
            let total_range_ns = ancestor_range_ns
                .checked_add(eval::DEFAULT_LOOKBACK_NS)
                .ok_or(Error::TimeOverflow)?;
            let plan = resolve_selector(
                vs,
                total_range_ns,
                ancestor_offset_ns,
                ancestor_anchor,
                query_start_ns,
                query_end_ns,
            )?;
            out.push(plan);
            Ok(())
        }
        Expr::MatrixSelector(ms) => {
            let own_range_ns = eval::duration_to_ns(ms.range)?;
            let total_range_ns = ancestor_range_ns
                .checked_add(own_range_ns)
                .ok_or(Error::TimeOverflow)?;
            let plan = resolve_selector(
                &ms.vs,
                total_range_ns,
                ancestor_offset_ns,
                ancestor_anchor,
                query_start_ns,
                query_end_ns,
            )?;
            out.push(plan);
            Ok(())
        }
        Expr::Paren(p) => plan_walk(
            &p.expr,
            ancestor_range_ns,
            ancestor_offset_ns,
            ancestor_anchor,
            query_start_ns,
            query_end_ns,
            out,
        ),
        Expr::Unary(u) => plan_walk(
            &u.expr,
            ancestor_range_ns,
            ancestor_offset_ns,
            ancestor_anchor,
            query_start_ns,
            query_end_ns,
            out,
        ),
        Expr::Subquery(sq) => {
            let own_range_ns = eval::duration_to_ns(sq.range)?;
            let total_range_ns = ancestor_range_ns
                .checked_add(own_range_ns)
                .ok_or(Error::TimeOverflow)?;
            let own_offset_ns = eval::signed_offset_ns(sq.offset.as_ref())?;
            let total_offset_ns = ancestor_offset_ns
                .checked_add(own_offset_ns)
                .ok_or(Error::TimeOverflow)?;
            let own_at_ns = eval::resolve_at(sq.at.as_ref(), query_start_ns, query_end_ns)?;
            // The subquery's own @ (if any) always wins over any ancestor
            // anchor for everything nested inside it, matching the same
            // innermost-wins rule a selector applies to its own @ vs. its
            // ancestors'.
            let anchor_for_children = own_at_ns.or(ancestor_anchor);
            plan_walk(
                &sq.expr,
                total_range_ns,
                total_offset_ns,
                anchor_for_children,
                query_start_ns,
                query_end_ns,
                out,
            )
        }
        Expr::Aggregate(a) => {
            plan_walk(
                &a.expr,
                ancestor_range_ns,
                ancestor_offset_ns,
                ancestor_anchor,
                query_start_ns,
                query_end_ns,
                out,
            )?;
            if let Some(param) = &a.param {
                plan_walk(
                    param,
                    ancestor_range_ns,
                    ancestor_offset_ns,
                    ancestor_anchor,
                    query_start_ns,
                    query_end_ns,
                    out,
                )?;
            }
            Ok(())
        }
        Expr::Binary(b) => {
            plan_walk(
                &b.lhs,
                ancestor_range_ns,
                ancestor_offset_ns,
                ancestor_anchor,
                query_start_ns,
                query_end_ns,
                out,
            )?;
            plan_walk(
                &b.rhs,
                ancestor_range_ns,
                ancestor_offset_ns,
                ancestor_anchor,
                query_start_ns,
                query_end_ns,
                out,
            )
        }
        Expr::Call(c) => {
            // ADR-0103 pushdown candidate detection, computed BEFORE recursing
            // so it reflects this call's own shape, not a nested one. A
            // `count_over_time` over a literal matrix selector (never a
            // subquery) is the only shape a coordinator may later push a
            // precomputed count down for. `matrix_arg` is reused verbatim from
            // `crate::functions` so the Paren-unwrap and the selector-vs-
            // subquery distinction stay identical to the evaluator's own. A
            // zero-arg call (`time()`, `pi()`) never reaches the index because
            // the name check short-circuits first, and `count_over_time` always
            // parses with exactly one matrix argument.
            let is_candidate = c.func.name == "count_over_time"
                && matches!(
                    crate::functions::matrix_arg(&c.args.args[0]),
                    Ok(crate::functions::MatrixArg::Selector(_))
                );
            let before = out.len();
            for arg in &c.args.args {
                plan_walk(
                    arg,
                    ancestor_range_ns,
                    ancestor_offset_ns,
                    ancestor_anchor,
                    query_start_ns,
                    query_end_ns,
                    out,
                )?;
            }
            // The literal-selector case pushes exactly one `SelectorPlan`; tag
            // it. Any other count (a subquery argument recurses through
            // `Expr::Subquery`'s own arm and never enters this branch's
            // `is_candidate`, and a candidate whose recursion pushed a
            // different number of plans is not the simple literal shape) leaves
            // every plan `false`.
            if is_candidate && out.len() == before + 1 {
                out[before].count_over_time_pushdown_candidate = true;
            }
            Ok(())
        }
        Expr::Extension(ext) => {
            for child in ext.expr.children() {
                plan_walk(
                    child,
                    ancestor_range_ns,
                    ancestor_offset_ns,
                    ancestor_anchor,
                    query_start_ns,
                    query_end_ns,
                    out,
                )?;
            }
            Ok(())
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_selector(
    vs: &VectorSelector,
    range_ns: i64,
    ancestor_offset_ns: i64,
    ancestor_anchor: Option<i64>,
    query_start_ns: i64,
    query_end_ns: i64,
) -> Result<SelectorPlan, Error> {
    let matchers = eval::build_matchers(vs)?;
    let own_offset_ns = eval::signed_offset_ns(vs.offset.as_ref())?;
    let offset_ns = ancestor_offset_ns
        .checked_add(own_offset_ns)
        .ok_or(Error::TimeOverflow)?;

    let own_at_ns = eval::resolve_at(vs.at.as_ref(), query_start_ns, query_end_ns)?;
    let anchor = match own_at_ns.or(ancestor_anchor) {
        Some(pinned_ns) => {
            let resolved = pinned_ns
                .checked_sub(offset_ns)
                .ok_or(Error::TimeOverflow)?;
            PlanAnchor::Pinned(resolved)
        }
        None => PlanAnchor::Window,
    };

    Ok(SelectorPlan {
        matchers,
        range_ns,
        offset_ns,
        anchor,
        // Structural pushdown tag, set by `plan_walk`'s `Expr::Call` arm once
        // it knows this plan was pushed by a `count_over_time` over a literal
        // selector; every selector is built here first with it `false`.
        count_over_time_pushdown_candidate: false,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn bare_selector_has_default_lookback_range_and_no_offset() {
        let plans = plan_selectors("up", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].range_ns, eval::DEFAULT_LOOKBACK_NS);
        assert_eq!(plans[0].offset_ns, 0);
        assert_eq!(plans[0].anchor, PlanAnchor::Window);
    }

    #[test]
    fn matrix_selector_range_is_its_own_window() {
        let plans = plan_selectors("up[5m]", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].range_ns,
            eval::duration_to_ns(std::time::Duration::from_secs(300)).expect("ok")
        );
    }

    #[test]
    fn subquery_range_accumulates_additively_with_the_inner_selector() {
        // rate(up[1m])[10m:30s]: outer subquery range 10m, inner matrix
        // selector range 1m; the plan's range for the leaf selector must be
        // their sum (10m + 1m), not either alone. plan_walk must reach
        // through the Call node (rate) unaffected by whether the evaluator
        // itself supports it yet.
        let plans = plan_selectors("rate(up[1m])[10m:30s]", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        let expected = eval::duration_to_ns(std::time::Duration::from_secs(600)).expect("ok")
            + eval::duration_to_ns(std::time::Duration::from_secs(60)).expect("ok");
        assert_eq!(plans[0].range_ns, expected);
    }

    #[test]
    fn bare_selector_nested_in_a_subquery_adds_its_own_default_lookback() {
        // `up[10m:1m]`: the subquery's own range (10m) plus the inner bare
        // selector's default 5m lookback (the selector is evaluated as an
        // instant vector at each subquery step), not the subquery range
        // alone.
        let plans = plan_selectors("up[10m:1m]", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        let expected = eval::duration_to_ns(std::time::Duration::from_secs(600)).expect("ok")
            + eval::DEFAULT_LOOKBACK_NS;
        assert_eq!(plans[0].range_ns, expected);
    }

    #[test]
    fn offsets_accumulate_additively_through_subquery_nesting() {
        let plans = plan_selectors("(up offset 1m)[10m:1m] offset 2m", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        let expected = eval::signed_offset_ns(Some(&promql_parser::parser::Offset::Pos(
            std::time::Duration::from_secs(60),
        )))
        .expect("ok")
            + eval::signed_offset_ns(Some(&promql_parser::parser::Offset::Pos(
                std::time::Duration::from_secs(120),
            )))
            .expect("ok");
        assert_eq!(plans[0].offset_ns, expected);
    }

    #[test]
    fn selectors_own_at_modifier_wins_over_ancestor_subquery_at() {
        let plans = plan_selectors("(up @ 100)[10m:1m] @ 500", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        let expected_ns = eval::ms_to_ns(100_000).expect("ok");
        assert_eq!(plans[0].anchor, PlanAnchor::Pinned(expected_ns));
    }

    #[test]
    fn ancestor_subquery_at_pins_a_selector_with_no_at_of_its_own() {
        let plans = plan_selectors("up[10m:1m] @ 500", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        let expected_ns = eval::ms_to_ns(500_000).expect("ok");
        assert_eq!(plans[0].anchor, PlanAnchor::Pinned(expected_ns));
    }

    #[test]
    fn at_start_and_end_resolve_against_the_outer_query_parameters() {
        let plans = plan_selectors("up @ start()", 1_000, 9_000).expect("parses");
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].anchor,
            PlanAnchor::Pinned(eval::ms_to_ns(1_000).expect("ok"))
        );

        let plans = plan_selectors("up @ end()", 1_000, 9_000).expect("parses");
        assert_eq!(
            plans[0].anchor,
            PlanAnchor::Pinned(eval::ms_to_ns(9_000).expect("ok"))
        );
    }

    #[test]
    fn binary_expression_reports_both_operands_selectors() {
        let plans = plan_selectors("up + down", 0, 0).expect("parses");
        assert_eq!(plans.len(), 2);
    }

    #[test]
    fn aggregate_reports_inner_selector_and_parameter_selector() {
        // topk's param must itself be scalar-typed, so a selector can only
        // appear in it via a vector-to-scalar function like `scalar()`;
        // `scalar(down)` still contains a selector (`down`) that must be
        // reported alongside `up`, the aggregate's main expr.
        let plans = plan_selectors("topk(scalar(down), up)", 0, 0).expect("parses");
        assert_eq!(plans.len(), 2);
    }

    #[test]
    fn function_call_reports_every_argument_selector() {
        // clamp(vector, min scalar, max scalar): `scalar(down)` as the min
        // bound still carries a selector, so both `up` and `down` must be
        // reported.
        let plans = plan_selectors("clamp(up, scalar(down), 0)", 0, 0).expect("parses");
        assert_eq!(plans.len(), 2);
    }

    #[test]
    fn no_selector_query_reports_nothing() {
        let plans = plan_selectors("42", 0, 0).expect("parses");
        assert!(plans.is_empty());
    }

    #[test]
    fn count_over_time_over_literal_selector_is_a_pushdown_candidate() {
        // The one shape ADR-0103's count pushdown targets: a `count_over_time`
        // over a literal matrix selector. Exactly one plan, tagged.
        let plans = plan_selectors("count_over_time(m[5m])", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        assert!(
            plans[0].count_over_time_pushdown_candidate,
            "count_over_time over a literal selector is a candidate"
        );
    }

    #[test]
    fn other_range_functions_and_bare_selectors_are_not_candidates() {
        // A different `_over_time` function is not the count shape.
        let plans = plan_selectors("sum_over_time(m[5m])", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        assert!(
            !plans[0].count_over_time_pushdown_candidate,
            "sum_over_time is not a count_over_time candidate"
        );

        // A bare matrix selector with no wrapping call never enters the Call
        // arm, so it is never tagged.
        let plans = plan_selectors("m[5m]", 0, 0).expect("parses");
        assert_eq!(plans.len(), 1);
        assert!(
            !plans[0].count_over_time_pushdown_candidate,
            "a bare selector is not a candidate"
        );
    }

    #[test]
    fn count_over_time_over_subquery_is_not_a_candidate() {
        // `count_over_time(m[1h:5m])`: the argument is a subquery, whose inner
        // selector is reached via `Expr::Subquery`'s own recursion, never
        // through the `Call` arm's `is_candidate` branch. No plan is tagged.
        let plans = plan_selectors("count_over_time(m[1h:5m])", 0, 0).expect("parses");
        assert!(!plans.is_empty());
        assert!(
            plans.iter().all(|p| !p.count_over_time_pushdown_candidate),
            "a subquery argument is structurally excluded"
        );
    }
}
