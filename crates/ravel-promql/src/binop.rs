//! Binary operator evaluation: scalar and
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
//!
//! # Native histograms
//!
//! Seven pairings compute a result: `h + h` and `h - h` (a histogram),
//! `h * f`, `f * h` and `h / f` (a scaled histogram), and `h == h` and
//! `h != h` (filter mode passes the surviving histogram through, `bool` mode
//! answers 1/0). Every other pairing that carries a histogram operand drops
//! the sample and raises one info annotation: any arithmetic between a
//! histogram and a float other than `*` and `h / f`, `h * h`, `h / h`, every
//! `%`, `^` and `atan2` pairing, any ordering comparison (`<`, `>`, `<=`,
//! `>=`) involving a histogram, and `==`/`!=` between a histogram and a
//! float. Set operators (`and`, `or`, `unless`) pass matched samples through
//! without combining values, so they carry histograms unchanged and are not
//! part of this split.
//!
//! `h + h` and `h - h` align their operands first, as every other combining
//! caller in [`crate::histogram`] does: an exponential/custom-buckets mix, or
//! two custom-buckets histograms with different bounds, cannot be combined at
//! all and drops with a warning annotation (the incompatible-bucket-layout
//! warning, one text for both shapes); two exponential histograms at
//! different scales are both down-converted to the coarser scale before the
//! buckets are merged. Stored data really does mix scales within one series
//! (RSEG down-converts a flush whose bucket count exceeds its limit), so this
//! is a normal input shape, not a corner case.

use std::collections::{HashMap, HashSet};

use promql_parser::label::Labels;
use promql_parser::parser::token::{
    T_ADD, T_ATAN2, T_DIV, T_EQLC, T_GTE, T_GTR, T_LAND, T_LOR, T_LSS, T_LTE, T_LUNLESS, T_MOD,
    T_MUL, T_NEQ, T_POW, T_SUB, TokenId,
};
use promql_parser::parser::{BinModifier, BinaryExpr, LabelModifier, VectorMatchCardinality};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL};

use crate::eval::{Error, Evaluator, InstantSample, InstantVector, QueryWindow, Value};
use crate::histogram::FloatHistogram;
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
    // Operator class first, before any dispatch on operand types. Everything
    // downstream splits the operator space two ways (`is_comparison` picks
    // `apply_cmp`, everything else falls to `apply_arith`; `is_set_operator`
    // picks the set path only on a Vector/Vector pair), so a token in neither
    // the arithmetic nor the comparison set would arrive at `apply_arith`'s
    // fallback. Ravel narrows it here rather than resting on promql-parser's
    // own `check_ast`, which sits on a caret version range.
    if !is_set_operator(op) && !is_arithmetic(op) && !is_comparison(op) {
        return Err(Error::Unsupported {
            construct: format!("binary operator {}", b.op),
        });
    }
    // A set operator is only defined over two instant vectors. The scalar
    // shapes below would otherwise route through `eval_scalar_scalar` or
    // `eval_scalar_vector`, both of which treat a non-comparison operator as
    // arithmetic and reach `apply_arith`'s fallback.
    if is_set_operator(op)
        && matches!(
            (&lhs, &rhs),
            (Value::Scalar(_), Value::Scalar(_))
                | (Value::Scalar(_), Value::Vector(_))
                | (Value::Vector(_), Value::Scalar(_))
        )
    {
        return Err(Error::Unsupported {
            construct: format!(
                "set operator {} over {} and {} operands",
                b.op,
                lhs.type_name(),
                rhs.type_name()
            ),
        });
    }

    match (lhs, rhs) {
        (Value::Scalar(l), Value::Scalar(r)) => Ok(Value::Scalar(eval_scalar_scalar(op, l, r))),
        (Value::Scalar(l), Value::Vector(r)) => {
            eval_scalar_vector(op, l, r, &modifier, true, ctx).map(Value::Vector)
        }
        (Value::Vector(l), Value::Scalar(r)) => {
            eval_scalar_vector(op, r, l, &modifier, false, ctx).map(Value::Vector)
        }
        (Value::Vector(l), Value::Vector(r)) => {
            eval_vector_vector(op, l, r, &modifier, ctx).map(Value::Vector)
        }
        (l, r) => Err(Error::Unsupported {
            construct: format!(
                "binary operator over {} and {} operands",
                l.type_name(),
                r.type_name()
            ),
        }),
    }
}

fn is_comparison(op: TokenId) -> bool {
    matches!(op, T_EQLC | T_NEQ | T_GTR | T_LSS | T_GTE | T_LTE)
}

fn is_set_operator(op: TokenId) -> bool {
    matches!(op, T_LAND | T_LOR | T_LUNLESS)
}

/// The seven tokens [`apply_arith`] implements. `eval_binary` uses this to
/// refuse anything outside the three operator classes up front, so
/// `apply_arith`'s fallback arm has no caller that can reach it.
fn is_arithmetic(op: TokenId) -> bool {
    matches!(op, T_ADD | T_SUB | T_MUL | T_DIV | T_MOD | T_POW | T_ATAN2)
}

/// A binary operator's source symbol, for the annotations raised when a
/// histogram-carrying pair falls outside the supported set or cannot be
/// aligned. Only the
/// arithmetic and comparison tokens reach [`combine_value`]; the catch-all
/// keeps this total without an `unreachable!`.
fn op_symbol(op: TokenId) -> &'static str {
    match op {
        T_ADD => "+",
        T_SUB => "-",
        T_MUL => "*",
        T_DIV => "/",
        T_MOD => "%",
        T_POW => "^",
        T_ATAN2 => "atan2",
        T_EQLC => "==",
        T_NEQ => "!=",
        T_GTR => ">",
        T_LSS => "<",
        T_GTE => ">=",
        T_LTE => "<=",
        _ => "?",
    }
}

/// Prometheus' `IncompatibleTypesInBinOpInfo` message: the operator is not
/// defined for this histogram/float operand pairing, so the sample is dropped
/// and this info is raised. Matches Prometheus' wording so the differential
/// harness (which compares the `infos` channel by presence) and a reader see
/// the same text.
fn incompatible_types_info(op: TokenId, lhs_is_histogram: bool, rhs_is_histogram: bool) -> String {
    let ty = |is_hist: bool| if is_hist { "histogram" } else { "float" };
    format!(
        "incompatible sample types encountered for binary operator \"{sym}\": {lhs} {sym} {rhs}",
        sym = op_symbol(op),
        lhs = ty(lhs_is_histogram),
        rhs = ty(rhs_is_histogram),
    )
}

/// Prometheus' `IncompatibleBucketLayoutInBinOpWarning` message: `+`/`-`
/// between two histograms with no common bucket layout has no defined result,
/// so the sample drops and this is raised.
///
/// Prometheus raises one annotation for both unalignable shapes. Its
/// `VectorBinop` catches `ErrHistogramsIncompatibleSchema` and
/// `ErrHistogramsIncompatibleBounds` from `FloatHistogram.Add`/`Sub` and calls
/// `NewIncompatibleBucketLayoutInBinOpWarning(op)` for either, on the warnings
/// channel rather than the infos one. Rendered in the same convention as
/// [`incompatible_types_info`]: Prometheus' channel prefix and position suffix
/// are dropped, the operator symbol is kept.
fn incompatible_bucket_layout_warning(op: TokenId) -> String {
    format!(
        "incompatible bucket layout encountered for binary operator {}",
        op_symbol(op)
    )
}

/// Establish [`FloatHistogram::add_assign`]/[`FloatHistogram::sub_assign`]'s
/// precondition for a `h + h` / `h - h` pair, returning the two operands to
/// combine or `None` for a pair that cannot be combined at all.
///
/// `combine` merges by absolute bucket index and keeps the receiver's scale,
/// so two operands at different scales would otherwise add bucket `i` to
/// bucket `i` across two different value ranges and label the result with the
/// receiver's scale. Both sides are down-converted to the coarser scale first,
/// exactly as [`crate::histogram::sum_histograms`] and
/// [`crate::histogram::histogram_rate`] do. Custom-bucket histograms cannot be
/// rescaled, so the two unalignable shapes Prometheus rejects outright (a mix
/// of the two schema families, and differing custom bounds) drop instead. The
/// caller raises one [`incompatible_bucket_layout_warning`] for either, which
/// is the single annotation Prometheus raises for both.
///
/// A differing `zero_threshold` is NOT reconciled here: no caller in this
/// crate does. Prometheus reconciles one inside `FloatHistogram.Add`/`Sub`
/// themselves (`reconcileZeroBuckets`, called before the buckets are merged),
/// not as a caller-side alignment step, so two operands with different zero
/// thresholds combine their zero counts here as if the thresholds matched.
fn align_histogram_operands(
    lhs: &FloatHistogram,
    rhs: &FloatHistogram,
) -> Option<(FloatHistogram, FloatHistogram)> {
    if lhs.uses_custom_buckets() != rhs.uses_custom_buckets() {
        return None;
    }
    if lhs.uses_custom_buckets() {
        if !lhs.custom_bounds_match(rhs) {
            return None;
        }
        return Some((lhs.clone(), rhs.clone()));
    }
    let scale = lhs.scale.min(rhs.scale);
    Some((lhs.copy_to_scale(scale), rhs.copy_to_scale(scale)))
}

/// The outcome of combining one matched (or scalar-paired) sample: a plain
/// float, a native histogram, or a drop. A drop is either a filter-mode
/// comparison that did not hold (no annotation) or a histogram/float pairing
/// the operator does not define (annotated through [`QueryWindow::info`] by
/// [`combine_value`] before returning).
enum Combined {
    Value(f64),
    Histogram(FloatHistogram),
    Drop,
}

/// Build the output sample for a combined pair, or `None` when it was dropped.
fn output_sample(combined: Combined, labels: LabelSet, ts_ns: i64) -> Option<InstantSample> {
    match combined {
        Combined::Drop => None,
        Combined::Value(value) => Some(InstantSample::scalar(labels, ts_ns, ts_ns, value)),
        Combined::Histogram(h) => Some(InstantSample::histogram(labels, ts_ns, ts_ns, h)),
    }
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
        // unreachable-allow: eval_binary's operator-class check -- it rejects
        // any token that is neither arithmetic nor a comparison, and rejects a
        // set operator on the Scalar/Scalar and Scalar/Vector shapes, before
        // eval_scalar_scalar or eval_scalar_vector runs; is_comparison then
        // routes every remaining comparison token to apply_cmp, leaving only
        // the seven arithmetic tokens for this match.
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
        // unreachable-allow: is_comparison(op) -- apply_cmp has exactly two
        // callers, eval_scalar_scalar and combine_value, and each calls it
        // only from the branch where that check already returned true.
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

/// Combine one matched (or scalar-paired) pair. `lhs`/`rhs` carry the
/// operator's literal left/right operands as `(value, histogram)`; the
/// histogram is `Some` for a native-histogram element and `None` for a plain
/// float. `filter` is the operand a filter-mode (non-`bool`) comparison
/// reports when it holds — the surviving operand's own value/histogram, not
/// necessarily `lhs` (e.g. `5 < vector` keeps the vector's value, even though
/// it is the right operand).
///
/// The supported histogram set, pinned empirically against the Prometheus
/// v3.13.1 binary (issue #1700): `histogram + histogram`,
/// `histogram - histogram`, `histogram * float`, `float * histogram`,
/// `histogram / float`, and `==`/`!=` between two histograms. Every other
/// pairing that carries a histogram drops the sample and raises an info
/// annotation. A filter-mode comparison that does not hold drops without an
/// annotation, exactly as for floats. The two combining arms (`+`, `-`) run
/// [`align_histogram_operands`] first; a pair that shares no bucket layout
/// drops there with its own warning annotation instead.
fn combine_value(
    op: TokenId,
    lhs: (f64, Option<&FloatHistogram>),
    rhs: (f64, Option<&FloatHistogram>),
    return_bool: bool,
    filter: (f64, Option<&FloatHistogram>),
    ctx: &QueryWindow,
) -> Combined {
    let (lv, lh) = lhs;
    let (rv, rh) = rhs;
    let unsupported = |ctx: &QueryWindow| {
        ctx.info(incompatible_types_info(op, lh.is_some(), rh.is_some()));
        Combined::Drop
    };

    if is_comparison(op) {
        match (lh, rh) {
            (None, None) => {
                let holds = apply_cmp(op, lv, rv);
                if return_bool {
                    Combined::Value(if holds { 1.0 } else { 0.0 })
                } else if holds {
                    Combined::Value(filter.0)
                } else {
                    Combined::Drop
                }
            }
            // Only equality is defined between two histograms; ordering is not.
            (Some(a), Some(b)) if matches!(op, T_EQLC | T_NEQ) => {
                let holds = if op == T_EQLC {
                    a.equals(b)
                } else {
                    !a.equals(b)
                };
                if return_bool {
                    Combined::Value(if holds { 1.0 } else { 0.0 })
                } else if holds {
                    match filter.1 {
                        Some(h) => Combined::Histogram(h.clone()),
                        None => Combined::Value(filter.0),
                    }
                } else {
                    Combined::Drop
                }
            }
            _ => unsupported(ctx),
        }
    } else {
        match (op, lh, rh) {
            (_, None, None) => Combined::Value(apply_arith(op, lv, rv)),
            (T_ADD | T_SUB, Some(a), Some(b)) => match align_histogram_operands(a, b) {
                Some((mut out, other)) => {
                    if op == T_ADD {
                        out.add_assign(&other);
                    } else {
                        out.sub_assign(&other);
                    }
                    Combined::Histogram(out)
                }
                None => {
                    ctx.warn(incompatible_bucket_layout_warning(op));
                    Combined::Drop
                }
            },
            (T_MUL, Some(a), None) => {
                let mut out = a.clone();
                out.mul(rv);
                Combined::Histogram(out)
            }
            (T_MUL, None, Some(b)) => {
                let mut out = b.clone();
                out.mul(lv);
                Combined::Histogram(out)
            }
            (T_DIV, Some(a), None) => {
                let mut out = a.clone();
                out.div(rv);
                Combined::Histogram(out)
            }
            _ => unsupported(ctx),
        }
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
    ctx: &QueryWindow,
) -> Result<InstantVector, Error> {
    let drop_name = should_drop_metric_name(op, modifier.return_bool);
    let mut out = Vec::with_capacity(vector.len());
    for s in vector {
        let (lhs, rhs) = if scalar_is_lhs {
            ((scalar, None), (s.value, s.histogram.as_ref()))
        } else {
            ((s.value, s.histogram.as_ref()), (scalar, None))
        };
        // A filter-mode comparison always reports the vector operand's own
        // value/histogram, regardless of the scalar's position.
        let filter = (s.value, s.histogram.as_ref());
        let combined = combine_value(op, lhs, rhs, modifier.return_bool, filter, ctx);
        if matches!(combined, Combined::Drop) {
            continue;
        }
        let labels = if drop_name {
            crate::eval::drop_metric_name(s.labels)
        } else {
            s.labels
        };
        if let Some(sample) = output_sample(combined, labels, s.ts_ns) {
            out.push(sample);
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
    ctx: &QueryWindow,
) -> Result<InstantVector, Error> {
    if is_set_operator(op) {
        let matching = modifier.matching.as_ref();
        return Ok(match op {
            T_LAND => set_and(lhs, rhs, matching),
            T_LOR => set_or(lhs, rhs, matching),
            T_LUNLESS => set_unless(lhs, rhs, matching),
            // unreachable-allow: is_set_operator(op) -- the guard on the
            // enclosing `if` already narrowed op to exactly these three
            // tokens before this match runs.
            _ => unreachable!("is_set_operator matched an unhandled token"),
        });
    }
    match &modifier.card {
        VectorMatchCardinality::OneToOne => one_to_one(op, lhs, rhs, modifier, ctx),
        VectorMatchCardinality::ManyToOne(extra) => {
            group_match(op, lhs, rhs, modifier, extra, true, ctx)
        }
        VectorMatchCardinality::OneToMany(extra) => {
            group_match(op, lhs, rhs, modifier, extra, false, ctx)
        }
        VectorMatchCardinality::ManyToMany => Err(Error::Unsupported {
            construct: "many-to-many vector matching for a non-set binary operator".to_string(),
        }),
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
    ctx: &QueryWindow,
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
        // The surviving value/histogram of a filter-mode comparison is the
        // literal left operand's own.
        let combined = combine_value(
            op,
            (l.value, l.histogram.as_ref()),
            (r.value, r.histogram.as_ref()),
            modifier.return_bool,
            (l.value, l.histogram.as_ref()),
            ctx,
        );
        let labels = one_to_one_output_labels(&l.labels, matching, drop_name);
        if let Some(sample) = output_sample(combined, labels, l.ts_ns) {
            out.push(sample);
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
    ctx: &QueryWindow,
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
        // Preserve the query's literal left/right order for the operator and
        // for a filter-mode comparison's surviving value, regardless of which
        // side is "many".
        let (lhs_operand, rhs_operand) = if lhs_is_many {
            (
                (m.value, m.histogram.as_ref()),
                (o.value, o.histogram.as_ref()),
            )
        } else {
            (
                (o.value, o.histogram.as_ref()),
                (m.value, m.histogram.as_ref()),
            )
        };
        let combined = combine_value(
            op,
            lhs_operand,
            rhs_operand,
            modifier.return_bool,
            lhs_operand,
            ctx,
        );
        let labels = grouped_output_labels(&m.labels, &o.labels, extra, drop_name);
        if let Some(sample) = output_sample(combined, labels, m.ts_ns) {
            out.push(sample);
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

    /// One native histogram, mirroring `eval::tests::nh`'s fixture.
    fn nh(count: f64, sum: f64) -> crate::histogram::FloatHistogram {
        crate::histogram::FloatHistogram {
            counter_reset_hint: crate::histogram::ResetHint::Unknown,
            scale: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![crate::histogram::Span {
                offset: 1,
                length: 1,
            }],
            negative_spans: Vec::new(),
            positive_buckets: vec![count],
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        }
    }

    /// A positive-only histogram at `scale`: `counts.len()` consecutive
    /// buckets from absolute index 1, `sum` chosen as the running total so the
    /// fixture stays readable.
    fn nh_at_scale(scale: i32, counts: &[f64]) -> crate::histogram::FloatHistogram {
        let total: f64 = counts.iter().sum();
        crate::histogram::FloatHistogram {
            counter_reset_hint: crate::histogram::ResetHint::Unknown,
            scale,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: total,
            sum: total * 2.0,
            positive_spans: vec![crate::histogram::Span {
                offset: 1,
                length: counts.len() as u32,
            }],
            negative_spans: Vec::new(),
            positive_buckets: counts.to_vec(),
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        }
    }

    /// A custom-buckets (NHCB) histogram: scale `-53` with explicit ascending
    /// boundaries, the schema family that cannot be rescaled.
    fn nh_custom(bounds: &[f64], counts: &[f64]) -> crate::histogram::FloatHistogram {
        let mut h = nh_at_scale(crate::histogram::CUSTOM_BUCKETS_SCALE, counts);
        h.custom_values = bounds.to_vec();
        h
    }

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

    /// Issue #1701: `eval_binary`'s final match arm used to `unreachable!` on
    /// an operand pair that is neither Scalar nor Vector, trusting
    /// promql-parser's own type check. A synthetic `BinaryExpr` over two
    /// String-typed operands (no real query text produces this: a bare
    /// string literal is not a valid binary operand) exercises the
    /// defensive fallback directly.
    #[test]
    fn binary_over_non_scalar_non_vector_operands_rejects_without_panicking() {
        use promql_parser::parser::token::{T_ADD, TokenType};
        use promql_parser::parser::{BinaryExpr, parse};

        let b = BinaryExpr {
            op: TokenType::new(T_ADD),
            lhs: Box::new(parse(r#""a""#).expect("parses")),
            rhs: Box::new(parse(r#""b""#).expect("parses")),
            modifier: None,
        };
        let ctx = crate::eval::QueryWindow::for_test();
        let err = super::eval_binary(&Evaluator::new(), &source(), &b, 0, &ctx)
            .expect_err("must reject, not panic");
        let Error::Unsupported { construct } = err else {
            panic!("expected Error::Unsupported, got {err:?}");
        };
        assert!(
            construct.contains("string"),
            "rejection should name the operand types, got {construct:?}"
        );
    }

    /// Issue #1701 fix round: a set operator on two scalars reaches
    /// `eval_scalar_scalar`, which treats every non-comparison token as
    /// arithmetic and hands it to `apply_arith`'s `unreachable!` fallback.
    /// promql-parser's `check_ast` is what keeps `1 and 2` from parsing, and
    /// it sits on a caret version range, so `eval_binary` now refuses the
    /// shape itself. Built from a synthetic `BinaryExpr` because the parser
    /// will not produce one today.
    #[test]
    fn set_operator_on_two_scalars_rejects_without_panicking() {
        use promql_parser::parser::token::{T_LAND, TokenType};
        use promql_parser::parser::{BinaryExpr, parse};

        let b = BinaryExpr {
            op: TokenType::new(T_LAND),
            lhs: Box::new(parse("1").expect("parses")),
            rhs: Box::new(parse("2").expect("parses")),
            modifier: None,
        };
        let ctx = crate::eval::QueryWindow::for_test();
        let err = super::eval_binary(&Evaluator::new(), &source(), &b, 0, &ctx)
            .expect_err("must reject, not panic");
        let Error::Unsupported { construct } = err else {
            panic!("expected Error::Unsupported, got {err:?}");
        };
        assert_eq!(
            construct,
            "set operator and over scalar and scalar operands"
        );
    }

    /// Issue #1701 fix round: the Scalar/Vector half of the same defect.
    /// `eval_scalar_vector` routes a non-comparison token to `combine_value`,
    /// which hands it to `apply_arith`'s fallback the same way.
    #[test]
    fn set_operator_on_scalar_and_vector_rejects_without_panicking() {
        use promql_parser::parser::token::{T_LOR, TokenType};
        use promql_parser::parser::{BinaryExpr, parse};

        let b = BinaryExpr {
            op: TokenType::new(T_LOR),
            lhs: Box::new(parse("1").expect("parses")),
            rhs: Box::new(parse("a").expect("parses")),
            modifier: None,
        };
        let ctx = crate::eval::QueryWindow::for_test();
        let err = super::eval_binary(&Evaluator::new(), &source(), &b, 0, &ctx)
            .expect_err("must reject, not panic");
        let Error::Unsupported { construct } = err else {
            panic!("expected Error::Unsupported, got {err:?}");
        };
        assert_eq!(
            construct,
            "set operator or over scalar and instant vector operands"
        );

        // and the other operand order.
        let b = BinaryExpr {
            op: TokenType::new(T_LOR),
            lhs: Box::new(parse("a").expect("parses")),
            rhs: Box::new(parse("1").expect("parses")),
            modifier: None,
        };
        let err = super::eval_binary(&Evaluator::new(), &source(), &b, 0, &ctx)
            .expect_err("must reject, not panic");
        let Error::Unsupported { construct } = err else {
            panic!("expected Error::Unsupported, got {err:?}");
        };
        assert_eq!(
            construct,
            "set operator or over instant vector and scalar operands"
        );
    }

    /// The same three queries as query TEXT, through the public evaluator.
    /// promql-parser 0.10 rejects them at parse time; this pins that an
    /// upgrade which starts accepting them still cannot reintroduce the panic
    /// path, because `eval_binary`'s own check refuses the shape. Either a
    /// parse error or an `Error::Unsupported` is a pass; a panic is not.
    #[test]
    fn set_operator_on_scalars_as_query_text_never_panics() {
        for query in ["1 and 2", "1 or 2", "1 unless 2"] {
            let err = Evaluator::new()
                .eval_instant(&source(), query, 0)
                .expect_err("must reject, not panic");
            assert!(
                matches!(err, Error::Parse(_) | Error::Unsupported { .. }),
                "{query:?} should be a parse error or an Unsupported rejection, got {err:?}"
            );
        }
    }

    /// Issue #1701: `eval_vector_vector`'s `ManyToMany` arm used to
    /// `unreachable!`, trusting promql-parser to only ever produce
    /// `ManyToMany` cardinality for a set operator. Calling it directly with
    /// `ManyToMany` and a non-set operator (no real query text produces
    /// this: the parser only assigns `ManyToMany` to `and`/`or`/`unless`)
    /// exercises the defensive fallback.
    #[test]
    fn many_to_many_cardinality_on_non_set_operator_rejects_without_panicking() {
        use promql_parser::parser::token::T_ADD;
        use promql_parser::parser::{BinModifier, VectorMatchCardinality};

        let modifier = BinModifier {
            card: VectorMatchCardinality::ManyToMany,
            ..BinModifier::default()
        };
        let ctx = crate::eval::QueryWindow::for_test();
        let err = super::eval_vector_vector(T_ADD, Vec::new(), Vec::new(), &modifier, &ctx)
            .expect_err("must reject, not panic");
        let Error::Unsupported { construct } = err else {
            panic!("expected Error::Unsupported, got {err:?}");
        };
        assert!(
            construct.contains("many-to-many"),
            "rejection should name the cardinality, got {construct:?}"
        );
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

    /// Issue #1700: `histogram * scalar` scales every population, matching the
    /// pinned Prometheus binary. Reverting the fix (restoring the histogram
    /// guard in `eval_scalar_vector`) makes `instant(..).expect(..)` panic
    /// here, before any count/sum assertion runs, because the pre-fix code
    /// returned `Error::Unsupported` for this query.
    #[test]
    fn scalar_vector_multiplication_scales_the_histogram() {
        let src = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("job", "x")], &[(0, nh(6.0, 42.0))])
            .expect("valid histogram series");
        let v = Evaluator::new()
            .instant(&src, "h * 2", 0)
            .expect("h * 2 must scale the histogram, not be rejected");
        assert_eq!(v.len(), 1);
        let h = v[0]
            .histogram
            .as_ref()
            .expect("the result element must carry a histogram");
        assert_eq!(h.observation_count(), 12.0, "count scaled by 2");
        assert_eq!(h.observation_sum(), 84.0, "sum scaled by 2");
        assert_eq!(v[0].value, 0.0, "a histogram element's float value is 0.0");
    }

    /// Issue #1700: `histogram + histogram` sums the populations, matching the
    /// pinned Prometheus binary.
    #[test]
    fn vector_vector_addition_over_two_histograms_sums_populations() {
        let src = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("job", "x")], &[(0, nh(6.0, 42.0))])
            .expect("valid histogram series");
        let v = Evaluator::new()
            .instant(&src, "h + h", 0)
            .expect("h + h must sum the histograms, not be rejected");
        assert_eq!(v.len(), 1);
        let h = v[0]
            .histogram
            .as_ref()
            .expect("the result element must carry a histogram");
        assert_eq!(h.observation_count(), 12.0);
        assert_eq!(h.observation_sum(), 84.0);
    }

    /// Issue #1700 fix round: `h + h` over two exponential histograms at
    /// different scales down-converts both to the coarser scale before merging
    /// buckets, so every bucket index means the same value range on both sides.
    /// The finer operand is on the left, which is the direction that exposes
    /// the unaligned merge: `combine` keeps the receiver's scale, so without
    /// alignment the result is labelled scale 1 and carries the coarse
    /// operand's counts at indexes that mean a different range there.
    #[test]
    fn addition_of_two_histograms_at_different_scales_aligns_to_the_coarser_scale() {
        // Scale 1, indexes 1..=4. Down-converting to scale 0 merges index
        // pairs: (1, 2) -> 1 and (3, 4) -> 2, so counts 1+2 = 3 and 3+4 = 7.
        let fine = nh_at_scale(1, &[1.0, 2.0, 3.0, 4.0]);
        // Scale 0, indexes 1..=2, already at the coarser scale.
        let coarse = nh_at_scale(0, &[10.0, 20.0]);
        let src = TestSource::new()
            .with_histogram_series(&[("__name__", "hf"), ("job", "x")], &[(0, fine)])
            .expect("valid histogram series")
            .with_histogram_series(&[("__name__", "hc"), ("job", "x")], &[(0, coarse)])
            .expect("valid histogram series");
        let v = Evaluator::new()
            .instant(&src, "hf + hc", 0)
            .expect("mixed-scale addition must evaluate");
        assert_eq!(v.len(), 1);
        let h = v[0]
            .histogram
            .as_ref()
            .expect("the result element must carry a histogram");
        assert_eq!(h.scale, 0, "the result is labelled with the coarser scale");
        assert_eq!(
            h.positive_spans,
            vec![crate::histogram::Span {
                offset: 1,
                length: 2
            }],
            "two merged buckets from index 1"
        );
        assert_eq!(
            h.positive_buckets,
            vec![13.0, 27.0],
            "index 1 is 1+2+10 and index 2 is 3+4+20 once both sides are at scale 0"
        );
        assert_eq!(h.observation_count(), 40.0, "10 + 30");
        assert_eq!(h.observation_sum(), 80.0, "20 + 60");
    }

    /// Issue #1700 fix round: a custom-buckets histogram and an
    /// exponential-schema one have no common bucket layout, so `h + h` drops
    /// the sample and raises exactly one warning. Prometheus' `VectorBinop`
    /// catches `ErrHistogramsIncompatibleSchema` from `FloatHistogram.Add` and
    /// answers with the incompatible-bucket-layout warning, on the warnings
    /// channel, so nothing lands on the infos channel here.
    #[test]
    fn addition_of_a_custom_buckets_and_exponential_pair_drops_and_annotates() {
        let src = TestSource::new()
            .with_histogram_series(
                &[("__name__", "hn"), ("job", "x")],
                &[(0, nh_custom(&[1.0, 2.0, 4.0], &[1.0, 2.0, 3.0]))],
            )
            .expect("valid histogram series")
            .with_histogram_series(
                &[("__name__", "he"), ("job", "x")],
                &[(0, nh_at_scale(0, &[1.0, 2.0, 3.0]))],
            )
            .expect("valid histogram series");
        let (value, annotations) = Evaluator::new()
            .eval_instant_annotated(&src, "hn + he", 0)
            .expect("an unalignable pair must drop, not error");
        let Value::Vector(v) = value else {
            panic!("expected a vector result");
        };
        assert!(
            v.is_empty(),
            "a custom-buckets and exponential pair cannot be combined"
        );
        assert_eq!(
            annotations.warnings(),
            ["incompatible bucket layout encountered for binary operator +"],
            "exactly one warning, carrying Prometheus' \
             IncompatibleBucketLayoutInBinOpWarning wording"
        );
        assert!(
            annotations.infos().is_empty(),
            "the drop is a warning, not an info"
        );
    }

    /// Issue #1700 fix round: two custom-buckets histograms whose boundaries
    /// differ have no shared layout either, and `-` drops them the same way
    /// `+` does. Prometheus answers `ErrHistogramsIncompatibleBounds` with the
    /// same one warning it uses for the incompatible-schema case, so only the
    /// operator symbol differs from the `+` test above.
    #[test]
    fn subtraction_of_custom_buckets_with_different_bounds_drops_and_annotates() {
        let src = TestSource::new()
            .with_histogram_series(
                &[("__name__", "ha"), ("job", "x")],
                &[(0, nh_custom(&[1.0, 2.0, 4.0], &[1.0, 2.0, 3.0]))],
            )
            .expect("valid histogram series")
            .with_histogram_series(
                &[("__name__", "hb"), ("job", "x")],
                &[(0, nh_custom(&[1.0, 3.0, 5.0], &[1.0, 2.0, 3.0]))],
            )
            .expect("valid histogram series");
        let (value, annotations) = Evaluator::new()
            .eval_instant_annotated(&src, "ha - hb", 0)
            .expect("an unalignable pair must drop, not error");
        let Value::Vector(v) = value else {
            panic!("expected a vector result");
        };
        assert!(v.is_empty(), "different custom bounds cannot be combined");
        assert_eq!(
            annotations.warnings(),
            ["incompatible bucket layout encountered for binary operator -"],
            "exactly one warning, carrying Prometheus' \
             IncompatibleBucketLayoutInBinOpWarning wording"
        );
        assert!(
            annotations.infos().is_empty(),
            "the drop is a warning, not an info"
        );
    }

    /// Issue #1700 second fix round: `h - h` subtracts zero counts like any
    /// other population, so a right operand with the larger zero count leaves
    /// the result's `zero_count` negative. `all_buckets` gates the zero bucket
    /// on a strictly positive count, as Prometheus' `allFloatBucketIterator`
    /// does, so the negative zero bucket is not rendered at all. No corpus cell
    /// can express this: both difftest histogram fixtures carry a constant
    /// `zero_count` of 1, so every corpus `h - h` cancels to exactly zero.
    #[test]
    fn subtraction_leaving_a_negative_zero_count_renders_no_zero_bucket() {
        let mut small = nh_at_scale(0, &[1.0]);
        small.zero_threshold = 0.5;
        small.zero_count = 1.0;
        small.count += 1.0;
        let mut large = nh_at_scale(0, &[1.0]);
        large.zero_threshold = 0.5;
        large.zero_count = 4.0;
        large.count += 4.0;
        let src = TestSource::new()
            .with_histogram_series(&[("__name__", "hs"), ("job", "x")], &[(0, small)])
            .expect("valid histogram series")
            .with_histogram_series(&[("__name__", "hl"), ("job", "x")], &[(0, large)])
            .expect("valid histogram series");
        let (value, annotations) = Evaluator::new()
            .eval_instant_annotated(&src, "hs - hl", 0)
            .expect("two aligned exponential histograms must subtract");
        let Value::Vector(v) = value else {
            panic!("expected a vector result");
        };
        assert_eq!(v.len(), 1);
        assert!(
            annotations.is_empty(),
            "an alignable pair raises no annotation"
        );
        let h = v[0]
            .histogram
            .as_ref()
            .expect("the result element must carry a histogram");
        assert_eq!(h.zero_count, -3.0, "1 - 4, subtracted like any population");
        assert!(
            h.all_buckets()
                .iter()
                .all(|b| !(b.lower == -0.5 && b.upper == 0.5)),
            "a non-positive zero count renders no zero bucket, got {:?}",
            h.all_buckets()
        );
    }

    /// Issue #1700: a comparison between a histogram and a float is not defined
    /// (only `==`/`!=` between two histograms is), so the sample is dropped and
    /// exactly one info annotation is raised, read back through
    /// `eval_instant_annotated`. The difftest comparator matches the `infos`
    /// channel by presence, so this behavior is what pins the corpus cell.
    #[test]
    fn comparison_over_histogram_drops_and_annotates() {
        let src = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("job", "x")], &[(0, nh(6.0, 42.0))])
            .expect("valid histogram series");
        let (value, annotations) = Evaluator::new()
            .eval_instant_annotated(&src, "h > 5", 0)
            .expect("h > 5 must succeed with a dropped sample, not error");
        match value {
            Value::Vector(v) => assert!(v.is_empty(), "the histogram sample must be dropped"),
            other => panic!("expected an empty vector, got {other:?}"),
        }
        assert_eq!(
            annotations.infos(),
            [
                "incompatible sample types encountered for binary operator \">\": \
                 histogram > float"
            ],
            "the one info must carry Prometheus' IncompatibleTypesInBinOpInfo wording"
        );
        assert!(
            annotations.warnings().is_empty(),
            "no warning, only an info"
        );
    }

    /// Issue #1700: `histogram == histogram` in filter mode keeps the surviving
    /// left histogram unchanged when the two are equal, and raises no
    /// annotation (it is a supported operation).
    #[test]
    fn equality_over_two_equal_histograms_keeps_the_left_histogram() {
        let src = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("job", "x")], &[(0, nh(6.0, 42.0))])
            .expect("valid histogram series");
        let (value, annotations) = Evaluator::new()
            .eval_instant_annotated(&src, "h == h", 0)
            .expect("h == h must succeed");
        let Value::Vector(v) = value else {
            panic!("expected a vector result");
        };
        assert_eq!(v.len(), 1);
        let h = v[0].histogram.as_ref().expect("carries the left histogram");
        assert_eq!(h.observation_count(), 6.0);
        assert_eq!(h.observation_sum(), 42.0);
        assert!(
            annotations.is_empty(),
            "a supported comparison raises nothing"
        );
    }

    /// Issue #1700: an unsupported arithmetic pairing (a histogram divided by
    /// a histogram) drops the sample and raises one info annotation, rather
    /// than fabricating a value.
    #[test]
    fn division_of_two_histograms_drops_and_annotates() {
        let src = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("job", "x")], &[(0, nh(6.0, 42.0))])
            .expect("valid histogram series");
        let (value, annotations) = Evaluator::new()
            .eval_instant_annotated(&src, "h / h", 0)
            .expect("h / h must succeed with a dropped sample, not error");
        let Value::Vector(v) = value else {
            panic!("expected a vector result");
        };
        assert!(v.is_empty(), "histogram / histogram is not defined");
        assert_eq!(annotations.infos().len(), 1);
    }

    /// Set operators pass matched samples through unchanged (no value
    /// combination), so they already handle histogram operands correctly
    /// and must NOT be guarded: this pins that `and` keeps working, so a
    /// future change can't silently widen the guard onto set operators.
    #[test]
    fn set_operator_and_over_histograms_is_unaffected() {
        let src = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("job", "x")], &[(0, nh(6.0, 42.0))])
            .expect("valid histogram series")
            .with_series(&[("__name__", "g"), ("job", "x")], &[(0, 1.0)])
            .expect("valid series");
        let v = Evaluator::new()
            .instant(&src, "h and on(job) g", 0)
            .expect("set operators over histograms must keep working");
        assert_eq!(v.len(), 1);
        assert!(
            v[0].histogram.is_some(),
            "the histogram element must survive `and` unchanged"
        );
    }
}
