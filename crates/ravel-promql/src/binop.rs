//! Binary operator evaluation (docs/promql-evaluator-plan.md P7): scalar and
//! vector arithmetic, filter- and bool-mode comparisons, one-to-one and
//! many-to-one/one-to-many vector matching (`on`/`ignoring`,
//! `group_left`/`group_right`), and the `and`/`or`/`unless` set operators.
//!
//! [`promql_parser`]'s `check_ast_for_binary_expr` (run at parse time)
//! already guarantees: `bool` only follows a comparison operator; a
//! scalar/scalar comparison always carries `bool`; `on`/`ignoring` labels
//! never overlap `group_left`/`group_right` labels; set operators never
//! have a scalar operand and never carry an explicit grouping modifier
//! (their cardinality is forced to `ManyToMany`, even absent any modifier
//! at all); every operand is Scalar or Vector; vector-matching modifiers
//! only appear when both operands are Vector. None of that is re-checked
//! here.
//!
//! Prometheus' `VectorMatchFillValues` (`<expr> + on() lhs=1,rhs=2 <expr>`)
//! is a promql-parser grammar extension with no real Prometheus equivalent;
//! any non-default value is rejected as [`Error::Unsupported`].

use std::collections::{HashMap, HashSet};

use promql_parser::label::Labels;
use promql_parser::parser::token::{
    T_ADD, T_ATAN2, T_DIV, T_EQLC, T_GTE, T_GTR, T_LAND, T_LOR, T_LSS, T_LTE, T_LUNLESS, T_MOD,
    T_MUL, T_NEQ, T_POW, T_SUB, TokenId,
};
use promql_parser::parser::{BinModifier, BinaryExpr, LabelModifier, VectorMatchCardinality};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL};

use crate::eval::{Error, Evaluator, InstantSample, InstantVector, QueryWindow, Value};
use crate::source::SeriesSource;

/// Evaluate a `BinaryExpr` at one instant: evaluate both operands, then
/// dispatch on their runtime types (promql-parser only ever produces
/// Scalar or Vector operands for a binary expression).
pub(crate) fn eval_binary(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    b: &BinaryExpr,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let lhs = evaluator.eval_expr(source, &b.lhs, eval_ts_ns, ctx)?;
    let rhs = evaluator.eval_expr(source, &b.rhs, eval_ts_ns, ctx)?;
    let modifier = b.modifier.clone().unwrap_or_default();
    if modifier.fill_values.lhs.is_some() || modifier.fill_values.rhs.is_some() {
        return Err(Error::Unsupported {
            construct: "vector matching fill-in values".to_string(),
        });
    }
    let op = b.op.id();

    match (lhs, rhs) {
        (Value::Scalar(l), Value::Scalar(r)) => Ok(Value::Scalar(eval_scalar_scalar(op, l, r))),
        (Value::Scalar(l), Value::Vector(r)) => {
            eval_scalar_vector(op, l, r, &modifier, true).map(Value::Vector)
        }
        (Value::Vector(l), Value::Scalar(r)) => {
            eval_scalar_vector(op, r, l, &modifier, false).map(Value::Vector)
        }
        (Value::Vector(l), Value::Vector(r)) => {
            eval_vector_vector(op, l, r, &modifier).map(Value::Vector)
        }
        (l, r) => unreachable!(
            "promql-parser only allows scalar/vector binary operands, got {} and {}",
            l.type_name(),
            r.type_name()
        ),
    }
}

fn is_comparison(op: TokenId) -> bool {
    matches!(op, T_EQLC | T_NEQ | T_GTR | T_LSS | T_GTE | T_LTE)
}

fn is_set_operator(op: TokenId) -> bool {
    matches!(op, T_LAND | T_LOR | T_LUNLESS)
}

/// Go's `math.Mod`/C `fmod` semantics: Rust's float `%` already implements
/// truncated-division remainder (sign follows the dividend), matching
/// Prometheus' `%` bit for bit in the general case.
fn apply_arith(op: TokenId, l: f64, r: f64) -> f64 {
    match op {
        T_ADD => l + r,
        T_SUB => l - r,
        T_MUL => l * r,
        T_DIV => l / r,
        T_MOD => l % r,
        T_POW => l.powf(r),
        // Prometheus computes `math.Atan2(lhs, rhs)`; Rust's `l.atan2(r)` is
        // `atan2(self, other)` with the same (y, x) argument order.
        T_ATAN2 => l.atan2(r),
        _ => unreachable!("apply_arith called with non-arithmetic operator {op}"),
    }
}

/// Rust's `f64` comparison operators already follow IEEE 754 (false for any
/// NaN operand except `!=`, which is true), matching Go's PromQL semantics
/// exactly with no special-casing needed.
fn apply_cmp(op: TokenId, l: f64, r: f64) -> bool {
    match op {
        T_EQLC => l == r,
        T_NEQ => l != r,
        T_GTR => l > r,
        T_LSS => l < r,
        T_GTE => l >= r,
        T_LTE => l <= r,
        _ => unreachable!("apply_cmp called with non-comparison operator {op}"),
    }
}

/// Whether `op`'s result drops `__name__`: every arithmetic operator does,
/// and so does a bool-mode comparison (the output is a synthesized 0/1, not
/// a passthrough of either operand). A filter-mode comparison keeps it: the
/// surviving operand's original identity is preserved.
fn should_drop_metric_name(op: TokenId, return_bool: bool) -> bool {
    !is_comparison(op) || return_bool
}

/// Scalar/scalar: parse-time validation guarantees `op` is arithmetic, or a
/// comparison with `bool` set (a bare scalar/scalar comparison is a parse
/// error), so a comparison here always yields 0.0/1.0.
fn eval_scalar_scalar(op: TokenId, l: f64, r: f64) -> f64 {
    if is_comparison(op) {
        if apply_cmp(op, l, r) { 1.0 } else { 0.0 }
    } else {
        apply_arith(op, l, r)
    }
}

/// The combined value for one matched (or scalar-paired) sample: `l`/`r`
/// are the operator's literal left/right values. `filter_value` is what a
/// filter-mode (non-`bool`) comparison reports when it holds — the
/// surviving operand's own original value, not necessarily `l` (e.g. `5 <
/// vector` keeps the vector's value, even though it is `r`). Returns `None`
/// when a filter-mode comparison does not hold, meaning the pair is
/// dropped.
fn combine_value(op: TokenId, l: f64, r: f64, return_bool: bool, filter_value: f64) -> Option<f64> {
    if is_comparison(op) {
        let holds = apply_cmp(op, l, r);
        if return_bool {
            Some(if holds { 1.0 } else { 0.0 })
        } else if holds {
            Some(filter_value)
        } else {
            None
        }
    } else {
        Some(apply_arith(op, l, r))
    }
}

/// Scalar/vector or vector/scalar. `scalar_is_lhs` fixes operator order
/// (`5 - v` vs `v - 5`) and which operand's value a filter-mode comparison
/// reports (always the vector's own, since the output is a vector).
fn eval_scalar_vector(
    op: TokenId,
    scalar: f64,
    vector: InstantVector,
    modifier: &BinModifier,
    scalar_is_lhs: bool,
) -> Result<InstantVector, Error> {
    let drop_name = should_drop_metric_name(op, modifier.return_bool);
    let mut out = Vec::with_capacity(vector.len());
    for s in vector {
        let (l, r) = if scalar_is_lhs {
            (scalar, s.value)
        } else {
            (s.value, scalar)
        };
        if let Some(value) = combine_value(op, l, r, modifier.return_bool, s.value) {
            let labels = if drop_name {
                crate::eval::drop_metric_name(s.labels)
            } else {
                s.labels
            };
            out.push(InstantSample {
                labels,
                ts_ns: s.ts_ns,
                orig_sample_ts_ns: s.ts_ns,
                value,
                histogram: None,
            });
        }
    }
    Ok(out)
}

/// Vector/vector: set operators first (their cardinality is always
/// `ManyToMany`, forced by the parser), then one-to-one or grouped
/// (`group_left`/`group_right`) matching for everything else.
fn eval_vector_vector(
    op: TokenId,
    lhs: InstantVector,
    rhs: InstantVector,
    modifier: &BinModifier,
) -> Result<InstantVector, Error> {
    if is_set_operator(op) {
        let matching = modifier.matching.as_ref();
        return Ok(match op {
            T_LAND => set_and(lhs, rhs, matching),
            T_LOR => set_or(lhs, rhs, matching),
            T_LUNLESS => set_unless(lhs, rhs, matching),
            _ => unreachable!("is_set_operator matched an unhandled token"),
        });
    }
    match &modifier.card {
        VectorMatchCardinality::OneToOne => one_to_one(op, lhs, rhs, modifier),
        VectorMatchCardinality::ManyToOne(extra) => {
            group_match(op, lhs, rhs, modifier, extra, true)
        }
        VectorMatchCardinality::OneToMany(extra) => {
            group_match(op, lhs, rhs, modifier, extra, false)
        }
        VectorMatchCardinality::ManyToMany => {
            unreachable!("promql-parser only produces ManyToMany for set operators")
        }
    }
}

/// The grouping/matching key: `labels` minus `__name__`, restricted to
/// `on(...)` names or with `ignoring(...)` names removed. An absent
/// modifier behaves like `ignoring()` with an empty list, i.e. the full
/// label set, which is Prometheus' default (equal label sets required).
fn matching_signature(labels: &LabelSet, matching: Option<&LabelModifier>) -> LabelSet {
    let filtered: Vec<Label> = labels
        .iter()
        .filter(|l| l.name != METRIC_NAME_LABEL)
        .filter(|l| label_in_signature(matching, &l.name))
        .cloned()
        .collect();
    LabelSet::new(filtered).unwrap_or_default()
}

fn label_in_signature(matching: Option<&LabelModifier>, name: &str) -> bool {
    match matching {
        None => true,
        Some(LabelModifier::Include(on)) => on.labels.iter().any(|n| n == name),
        Some(LabelModifier::Exclude(ignoring)) => !ignoring.labels.iter().any(|n| n == name),
    }
}

/// Output label set for a one-to-one (ungrouped) match: `base` (the query's
/// literal left operand) restricted exactly like the matching signature.
/// Prometheus' `on(...)` keeps only the named labels (so an explicit `on`
/// always drops `__name__` too, matching signature exactly);
/// `ignoring(...)`/no modifier keeps everything except the ignored names,
/// with `__name__` additionally dropped only when `drop_name` (arithmetic
/// or bool-mode comparison; a filter-mode comparison keeps it).
fn one_to_one_output_labels(
    base: &LabelSet,
    matching: Option<&LabelModifier>,
    drop_name: bool,
) -> LabelSet {
    let is_on = matches!(matching, Some(LabelModifier::Include(_)));
    let filtered: Vec<Label> = base
        .iter()
        .filter(|l| {
            if is_on {
                label_in_signature(matching, &l.name)
            } else {
                (!drop_name || l.name != METRIC_NAME_LABEL) && label_in_signature(matching, &l.name)
            }
        })
        .cloned()
        .collect();
    LabelSet::new(filtered).unwrap_or_default()
}

/// Output label set for a grouped (`group_left`/`group_right`) match: every
/// label (except `__name__`, when `drop_name`) from `many` (the higher-
/// cardinality side's own sample, unrestricted by `on`/`ignoring`, which
/// only narrows the matching key), overlaid with `extra`'s named labels
/// copied from `one` — Prometheus sets each to the "one" side's value if
/// present there, and otherwise removes it even if `many` already carried
/// its own value under that name.
fn grouped_output_labels(
    many: &LabelSet,
    one: &LabelSet,
    extra: &Labels,
    drop_name: bool,
) -> LabelSet {
    let mut out: Vec<Label> = many
        .iter()
        .filter(|l| !drop_name || l.name != METRIC_NAME_LABEL)
        .cloned()
        .collect();
    for name in &extra.labels {
        out.retain(|l| &l.name != name);
        if let Some(value) = one.get(name) {
            out.push(Label {
                name: name.clone(),
                value: value.to_string(),
            });
        }
    }
    LabelSet::new(out).unwrap_or_default()
}

/// One-to-one vector matching: exactly one candidate per signature on
/// either side. Two distinct failure shapes, both
/// [`Error::AmbiguousMatch`]: two `rhs` entries share a signature
/// (detected unconditionally while indexing `rhs`), or two `lhs` entries
/// share a signature that also matches something on `rhs` (an `lhs`
/// signature with no `rhs` match is never flagged, matching Prometheus:
/// unmatched duplicates are simply dropped, not an error).
fn one_to_one(
    op: TokenId,
    lhs: InstantVector,
    rhs: InstantVector,
    modifier: &BinModifier,
) -> Result<InstantVector, Error> {
    let matching = modifier.matching.as_ref();
    let mut rhs_map: HashMap<LabelSet, &InstantSample> = HashMap::new();
    for s in &rhs {
        let key = matching_signature(&s.labels, matching);
        if rhs_map.insert(key.clone(), s).is_some() {
            return Err(ambiguous_match_error(&key));
        }
    }

    let drop_name = should_drop_metric_name(op, modifier.return_bool);
    let mut matched_sigs: HashSet<LabelSet> = HashSet::new();
    let mut out = Vec::new();
    for l in &lhs {
        let key = matching_signature(&l.labels, matching);
        let Some(r) = rhs_map.get(&key) else {
            continue;
        };
        if !matched_sigs.insert(key.clone()) {
            return Err(ambiguous_match_error(&key));
        }
        if let Some(value) = combine_value(op, l.value, r.value, modifier.return_bool, l.value) {
            out.push(InstantSample {
                labels: one_to_one_output_labels(&l.labels, matching, drop_name),
                ts_ns: l.ts_ns,
                orig_sample_ts_ns: l.ts_ns,
                value,
                histogram: None,
            });
        }
    }
    check_unique_output_labels(out)
}

/// `group_left`/`group_right` matching. `lhs_is_many` selects which operand
/// is the many side (`group_left`: `lhs`; `group_right`: `rhs`); the "one"
/// side must have a unique signature unconditionally (unlike the many
/// side, where repeats are the entire point of grouping). `l_val`/`r_val`
/// preserve the query's literal left/right order for the operator and for
/// a filter-mode comparison's surviving value, regardless of which side is
/// "many".
fn group_match(
    op: TokenId,
    lhs: InstantVector,
    rhs: InstantVector,
    modifier: &BinModifier,
    extra: &Labels,
    lhs_is_many: bool,
) -> Result<InstantVector, Error> {
    let matching = modifier.matching.as_ref();
    let (many, one): (&InstantVector, &InstantVector) = if lhs_is_many {
        (&lhs, &rhs)
    } else {
        (&rhs, &lhs)
    };

    let mut one_map: HashMap<LabelSet, &InstantSample> = HashMap::new();
    for s in one {
        let key = matching_signature(&s.labels, matching);
        if one_map.insert(key.clone(), s).is_some() {
            return Err(ambiguous_match_error(&key));
        }
    }

    let drop_name = should_drop_metric_name(op, modifier.return_bool);
    let mut out = Vec::new();
    for m in many {
        let key = matching_signature(&m.labels, matching);
        let Some(o) = one_map.get(&key) else {
            continue;
        };
        let (l_val, r_val) = if lhs_is_many {
            (m.value, o.value)
        } else {
            (o.value, m.value)
        };
        if let Some(value) = combine_value(op, l_val, r_val, modifier.return_bool, l_val) {
            out.push(InstantSample {
                labels: grouped_output_labels(&m.labels, &o.labels, extra, drop_name),
                ts_ns: m.ts_ns,
                orig_sample_ts_ns: m.ts_ns,
                value,
                histogram: None,
            });
        }
    }
    check_unique_output_labels(out)
}

/// General "a query's result vector may not contain two series with the
/// same label set" invariant. Reachable in practice mainly through
/// `group_left`/`group_right`'s label copying, e.g. when the copied label
/// collapses two otherwise-distinct many-side series onto the same output
/// identity.
fn check_unique_output_labels(out: InstantVector) -> Result<InstantVector, Error> {
    let mut seen: HashSet<&LabelSet> = HashSet::new();
    for s in &out {
        if !seen.insert(&s.labels) {
            return Err(Error::AmbiguousMatch {
                detail: format!(
                    "multiple output series would carry the identical label set {:?}",
                    s.labels
                ),
            });
        }
    }
    Ok(out)
}

fn ambiguous_match_error(signature: &LabelSet) -> Error {
    Error::AmbiguousMatch {
        detail: format!(
            "many-to-many matching not allowed: matching labels must be unique on one side \
             (duplicate signature {signature:?})"
        ),
    }
}

fn signature_set(v: &InstantVector, matching: Option<&LabelModifier>) -> HashSet<LabelSet> {
    v.iter()
        .map(|s| matching_signature(&s.labels, matching))
        .collect()
}

/// `vector1 and vector2`: `vector1`'s own elements (value and labels
/// untouched, `__name__` included) that have a matching signature
/// somewhere in `vector2`. Duplicate signatures within either side are not
/// an error for set operators (the parser's `ManyToMany` cardinality is
/// exactly this: only "does a match exist", never a 1:1 pairing).
fn set_and(
    lhs: InstantVector,
    rhs: InstantVector,
    matching: Option<&LabelModifier>,
) -> InstantVector {
    let rhs_sigs = signature_set(&rhs, matching);
    lhs.into_iter()
        .filter(|s| rhs_sigs.contains(&matching_signature(&s.labels, matching)))
        .collect()
}

/// `vector1 unless vector2`: `vector1`'s own elements with no matching
/// signature in `vector2`.
fn set_unless(
    lhs: InstantVector,
    rhs: InstantVector,
    matching: Option<&LabelModifier>,
) -> InstantVector {
    let rhs_sigs = signature_set(&rhs, matching);
    lhs.into_iter()
        .filter(|s| !rhs_sigs.contains(&matching_signature(&s.labels, matching)))
        .collect()
}

/// `vector1 or vector2`: every element of `vector1`, plus `vector2`'s
/// elements whose signature does not already appear in `vector1`. A
/// `vector2` element that shares a full label set (not just a signature)
/// with a `vector1` element necessarily also shares its signature, so it is
/// already excluded; the union cannot itself introduce a duplicate output
/// label set.
fn set_or(
    lhs: InstantVector,
    rhs: InstantVector,
    matching: Option<&LabelModifier>,
) -> InstantVector {
    let lhs_sigs = signature_set(&lhs, matching);
    let mut out = lhs;
    for s in rhs {
        if !lhs_sigs.contains(&matching_signature(&s.labels, matching)) {
            out.push(s);
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::eval::{Error, Evaluator, Value};
    use crate::testsource::TestSource;

    fn source() -> TestSource {
        TestSource::new()
            .with_series(
                &[("__name__", "a"), ("job", "x"), ("region", "us")],
                &[(0, 2.0)],
            )
            .expect("valid series")
            .with_series(
                &[("__name__", "a"), ("job", "y"), ("region", "eu")],
                &[(0, 5.0)],
            )
            .expect("valid series")
            .with_series(&[("__name__", "b"), ("job", "x")], &[(0, 10.0)])
            .expect("valid series")
            .with_series(&[("__name__", "b"), ("job", "y")], &[(0, 20.0)])
            .expect("valid series")
    }

    fn eval_vector(query: &str) -> Vec<(Vec<(String, String)>, f64)> {
        eval_vector_over(query, &source())
    }

    fn eval_vector_over(query: &str, src: &TestSource) -> Vec<(Vec<(String, String)>, f64)> {
        let v = Evaluator::new()
            .instant(src, query, 0)
            .expect("must evaluate");
        let mut out: Vec<(Vec<(String, String)>, f64)> = v
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
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = pairs
            .iter()
            .map(|(n, val)| (n.to_string(), val.to_string()))
            .collect();
        v.sort();
        v
    }

    fn eval_scalar(query: &str) -> f64 {
        match Evaluator::new()
            .eval_instant(&source(), query, 0)
            .expect("must evaluate")
        {
            Value::Scalar(x) => x,
            other => panic!("expected a scalar result for {query:?}, got {other:?}"),
        }
    }

    #[test]
    fn scalar_scalar_arithmetic_and_bool_comparison() {
        assert_eq!(eval_scalar("2 + 3"), 5.0);
        assert_eq!(eval_scalar("2 - 3"), -1.0);
        assert_eq!(eval_scalar("2 * 3"), 6.0);
        assert_eq!(eval_scalar("7 / 2"), 3.5);
        assert_eq!(eval_scalar("7 % 3"), 1.0);
        assert_eq!(eval_scalar("-7 % 3"), -1.0);
        assert_eq!(eval_scalar("2 ^ 10"), 1024.0);
        assert_eq!(eval_scalar("1 atan2 1"), std::f64::consts::FRAC_PI_4);
        assert_eq!(eval_scalar("2 == bool 2"), 1.0);
        assert_eq!(eval_scalar("2 == bool 3"), 0.0);
        assert_eq!(eval_scalar("2 != bool 3"), 1.0);
    }

    #[test]
    fn scalar_vector_filter_and_bool_both_directions() {
        // arithmetic: metric name dropped, other labels kept.
        assert_eq!(
            eval_vector("a{job=\"x\"} + 1"),
            vec![(labels(&[("job", "x"), ("region", "us")]), 3.0)]
        );
        assert_eq!(
            eval_vector("1 + a{job=\"x\"}"),
            vec![(labels(&[("job", "x"), ("region", "us")]), 3.0)]
        );
        // filter-mode comparison on scalar/vector: surviving value/labels
        // are always the vector's own, regardless of lhs/rhs position.
        assert_eq!(
            eval_vector("a{job=\"x\"} > 1"),
            vec![(
                labels(&[("__name__", "a"), ("job", "x"), ("region", "us")]),
                2.0
            )]
        );
        assert_eq!(
            eval_vector("1 < a{job=\"x\"}"),
            vec![(
                labels(&[("__name__", "a"), ("job", "x"), ("region", "us")]),
                2.0
            )]
        );
        assert_eq!(eval_vector("a{job=\"x\"} > 10"), vec![]);
        // bool-mode comparison: metric name dropped, synthesized 0/1.
        assert_eq!(
            eval_vector("a{job=\"x\"} > bool 10"),
            vec![(labels(&[("job", "x"), ("region", "us")]), 0.0)]
        );
    }

    #[test]
    fn one_to_one_default_matching_drops_name_on_arithmetic() {
        // both sides share job labels 1:1 (a{job=x,region=us}+b{job=x},
        // a{job=y,region=eu}+b{job=y}); default matching requires the full
        // label set to agree, but a has an extra `region` label b lacks, so
        // nothing matches under the implicit "equal label sets" rule.
        assert_eq!(eval_vector("a + b"), vec![]);
    }

    #[test]
    fn one_to_one_on_restricts_matching_and_output_labels() {
        // on(job) restricts both matching AND the output label set: only
        // `job` survives, __name__ is always dropped under an explicit on().
        assert_eq!(
            eval_vector("a + on(job) b"),
            vec![
                (labels(&[("job", "x")]), 12.0),
                (labels(&[("job", "y")]), 25.0),
            ]
        );
    }

    #[test]
    fn one_to_one_ignoring_keeps_lhs_labels_minus_ignored() {
        // ignoring(region) both widens matching (region need not agree) and
        // deletes `region` from the output, on top of arithmetic's own
        // __name__ drop: only `job` survives.
        assert_eq!(
            eval_vector("a + ignoring(region) b"),
            vec![
                (labels(&[("job", "x")]), 12.0),
                (labels(&[("job", "y")]), 25.0),
            ]
        );
    }

    #[test]
    fn one_to_one_filter_comparison_keeps_literal_lhs_value_and_name() {
        // filter-mode comparison keeps the literal lhs value, but on(job)'s
        // Keep-list restriction still drops everything but `job`, including
        // __name__ (which is never in an explicit on() list).
        assert_eq!(
            eval_vector("a < on(job) b"),
            vec![
                (labels(&[("job", "x")]), 2.0),
                (labels(&[("job", "y")]), 5.0),
            ]
        );
    }

    #[test]
    fn group_left_copies_one_side_label_overwriting_many_side() {
        // `pod` distinguishes the two many-side series so the output label
        // sets stay distinct even though `instance` is overwritten to the
        // same copied value on both (a genuine collision there would
        // correctly be an ambiguous-match error, tested separately).
        let src = TestSource::new()
            .with_series(
                &[
                    ("__name__", "many"),
                    ("job", "x"),
                    ("pod", "p1"),
                    ("instance", "i1"),
                ],
                &[(0, 1.0)],
            )
            .expect("valid series")
            .with_series(
                &[
                    ("__name__", "many"),
                    ("job", "x"),
                    ("pod", "p2"),
                    ("instance", "i2"),
                ],
                &[(0, 2.0)],
            )
            .expect("valid series")
            .with_series(
                &[
                    ("__name__", "one"),
                    ("job", "x"),
                    ("instance", "overwritten"),
                ],
                &[(0, 100.0)],
            )
            .expect("valid series");

        // group_left(instance) copies the "one" side's `instance` onto every
        // many-side output, overwriting the many side's own `instance`.
        assert_eq!(
            eval_vector_over("many + on(job) group_left(instance) one", &src),
            vec![
                (
                    labels(&[("instance", "overwritten"), ("job", "x"), ("pod", "p1")]),
                    101.0
                ),
                (
                    labels(&[("instance", "overwritten"), ("job", "x"), ("pod", "p2")]),
                    102.0
                ),
            ]
        );
    }

    #[test]
    fn group_left_deletes_label_absent_on_one_side() {
        let src = TestSource::new()
            .with_series(
                &[("__name__", "many"), ("job", "x"), ("extra", "keep-me-out")],
                &[(0, 1.0)],
            )
            .expect("valid series")
            .with_series(&[("__name__", "one"), ("job", "x")], &[(0, 100.0)])
            .expect("valid series");

        assert_eq!(
            eval_vector_over("many + on(job) group_left(extra) one", &src),
            vec![(labels(&[("job", "x")]), 101.0)]
        );
    }

    #[test]
    fn set_operators_and_or_unless() {
        // `and`: a's elements whose signature (job) also appears in b.
        assert_eq!(
            eval_vector("a and on(job) b"),
            vec![
                (
                    labels(&[("__name__", "a"), ("job", "x"), ("region", "us")]),
                    2.0
                ),
                (
                    labels(&[("__name__", "a"), ("job", "y"), ("region", "eu")]),
                    5.0
                ),
            ]
        );
        // `unless`: a's elements with no matching signature in a metric
        // that shares no job values (none here, so nothing is excluded);
        // use a query with a genuinely disjoint job value.
        assert_eq!(
            eval_vector("a unless on(job) (b unless on(job) b)"),
            eval_vector("a")
        );
        // `or`: every lhs element, plus rhs elements whose signature isn't
        // already present on lhs.
        assert_eq!(eval_vector("a or on(job) b").len(), 2);
    }

    #[test]
    fn range_query_re_evaluates_the_binary_expression_at_every_step() {
        let matrix = Evaluator::new()
            .range(&source(), "a{job=\"x\"} + 1", 0, 2000, 1000)
            .expect("must evaluate");
        assert_eq!(matrix.len(), 1);
        let (out_labels, samples) = &matrix[0];
        assert_eq!(
            out_labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect::<Vec<_>>(),
            labels(&[("job", "x"), ("region", "us")])
        );
        assert_eq!(
            samples.iter().map(|s| s.value).collect::<Vec<_>>(),
            vec![3.0, 3.0, 3.0]
        );
    }

    #[test]
    fn nan_comparisons_filter_and_bool() {
        let src = TestSource::new()
            .with_series(&[("__name__", "n")], &[(0, f64::NAN)])
            .expect("valid series");
        assert_eq!(eval_vector_over("n > bool 0", &src), vec![(vec![], 0.0)]);
        assert_eq!(eval_vector_over("n < bool 0", &src), vec![(vec![], 0.0)]);
        assert_eq!(eval_vector_over("n == bool 0", &src), vec![(vec![], 0.0)]);
        assert_eq!(eval_vector_over("n != bool 0", &src), vec![(vec![], 1.0)]);
        assert_eq!(eval_vector_over("n > 0", &src), vec![]);
        let filtered = eval_vector_over("n != 0", &src);
        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].1.is_nan());
    }

    #[test]
    fn duplicate_signature_rhs_is_always_an_ambiguous_match_error() {
        let src = TestSource::new()
            .with_series(&[("__name__", "l"), ("job", "x")], &[(0, 1.0)])
            .expect("valid series")
            .with_series(&[("__name__", "r"), ("job", "x"), ("k", "1")], &[(0, 2.0)])
            .expect("valid series")
            .with_series(&[("__name__", "r"), ("job", "x"), ("k", "2")], &[(0, 3.0)])
            .expect("valid series");
        let err = Evaluator::new()
            .instant(&src, "l + on(job) r", 0)
            .expect_err("must be rejected as ambiguous");
        assert!(matches!(err, Error::AmbiguousMatch { .. }));
    }

    #[test]
    fn duplicate_signature_lhs_is_only_an_error_when_matched() {
        let src = TestSource::new()
            .with_series(&[("__name__", "l"), ("job", "x"), ("k", "1")], &[(0, 1.0)])
            .expect("valid series")
            .with_series(&[("__name__", "l"), ("job", "x"), ("k", "2")], &[(0, 2.0)])
            .expect("valid series");
        // no rhs series at all: both lhs duplicates go unmatched, which is
        // not an error (Prometheus silently drops unmatched entries).
        assert_eq!(eval_vector_over("l + on(job) missing", &src), vec![]);
    }

    #[test]
    fn group_match_one_side_duplicate_is_unconditionally_an_error() {
        let src = TestSource::new()
            .with_series(&[("__name__", "many"), ("job", "x")], &[(0, 1.0)])
            .expect("valid series")
            .with_series(
                &[("__name__", "one"), ("job", "x"), ("k", "1")],
                &[(0, 2.0)],
            )
            .expect("valid series")
            .with_series(
                &[("__name__", "one"), ("job", "x"), ("k", "2")],
                &[(0, 3.0)],
            )
            .expect("valid series");
        let err = Evaluator::new()
            .instant(&src, "many + on(job) group_left one", 0)
            .expect_err("one-side duplicate must be rejected even though many-side matches");
        assert!(matches!(err, Error::AmbiguousMatch { .. }));
    }
}
