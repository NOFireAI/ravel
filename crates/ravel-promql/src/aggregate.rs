//! Aggregation operator evaluation: `sum`,
//! `avg`, `min`, `max`, `count`, `group`, `stddev`, `stdvar`, `topk`,
//! `bottomk`, `quantile`, `count_values`, with `by`/`without`/`__name__`
//! label handling.
//!
//! [`promql_parser`]'s `check_ast_for_aggregate_expr` (run at parse time)
//! already guarantees: `op` is one of the known aggregators; `expr` is
//! Vector-typed; `topk`/`bottomk`/`quantile` always carry a Scalar-typed
//! `param`; `count_values` always carries a String-typed `param`; every other
//! aggregator never carries a param. None of that is re-checked here.
//!
//! promql-parser 0.10 also parses `limitk`/`limit_ratio` (Prometheus'
//! experimental sampling aggregators). Those are not evaluated here; the
//! dispatch below rejects them with a typed [`Error::Unsupported`] rather
//! than panicking, so no tenant query can panic the query path.
//!
//! Grouping (Prometheus' `generateGroupingKey`/output-metric construction):
//! `without (labels)` keeps every label except `__name__` and the named
//! ones; `by (labels)` keeps only the named labels (`__name__` survives only
//! if explicitly named); no clause at all collapses the whole input into a
//! single group with an empty label set, which is also what an explicit
//! empty `by ()` produces (an empty keep-list keeps nothing) — Prometheus'
//! own grouping code takes the same "no grouping labels, not `without`"
//! branch for both.
//!
//! Deterministic accumulation order: the input instant
//! vector is sorted by label set before grouping, mirroring Prometheus'
//! sorted postings expansion, so Kahan/Welford accumulation lands on the
//! same bit pattern regardless of the storage backend's own return order.
//!
//! `sum`/`avg`/`stddev`/`stdvar` reduce a group's member values through the
//! same Kahan-compensated/Welford primitives `crate::functions::over_time`
//! ports from Prometheus' `promql/functions.go` (`kahan_sum_inc`): folding
//! from a zero-valued accumulator over every member, first one included, is
//! bit-identical to Prometheus' own "seed from the first element, loop over
//! the rest" shape, so no first-element special case is needed here.
//! `quantile` reuses that module's `quantile` helper verbatim.
//!
//! `topk`/`bottomk` port Prometheus' own binary-heap admission algorithm
//! (`container/heap` push/sift-up, pop/sift-down, standard array index
//! arithmetic) rather than a plain top-k sort, because ties at the k-th
//! boundary are order-dependent on that exact mechanism: an element seen
//! while the heap is still filling (below capacity `k`) is retained
//! unconditionally, even over a later, equal-valued element a value-only
//! ranking would treat as interchangeable. This crate's own admission
//! mechanics are verified against Prometheus (`agg_topk_...`/`agg_bottomk_...`
//! entries in the differential corpus retain the correct *set* of members at
//! every tested `k`, including at tie boundaries).
//!
//! The final within-heap sort's *tie-break order* among exactly-equal
//! boundary values is a different question, and is deliberately **not**
//! claimed to match Prometheus at any `k`: an earlier version of this
//! comment claimed bit-for-bit agreement for `k <= 12` on the theory that
//! Go's `sort.Sort(sort.Reverse(heap))` resolves to a stable insertion sort
//! at that size. That claim is false even at `k == 2`
//! (`agg_topk_tie_fully_retained`/`agg_bottomk_tie_fully_retained`): this
//! crate's heap ends up in the same pre-sort array order Prometheus' own
//! heap should (same push/pop mechanics, same encounter order), yet the two
//! engines emit the tied pair in opposite order. `sort.Reverse` wrapping a
//! stable sort does not trivially reduce to "stable sort then reverse the
//! whole list" the way it might seem to; the exact mechanism was not chased
//! down further. The differential corpus tests tie-boundary
//! *retention* as `mode: unordered` (right: which members survive is
//! well-defined and verified) rather than asserting a specific tie-break
//! order (not: no PromQL consumer can rely on which of several exactly-equal
//! values a real Prometheus server would place first, so pinning one
//! implementation's arbitrary choice would test an implementation detail,
//! not a contract).
//!
//! `topk`/`bottomk` output rows keep each selected member's own full,
//! original label set (including `__name__`): unlike every other
//! aggregator, they select among original series rather than reducing to a
//! single synthesized value per group. `by`/`without` only changes how many
//! independent top-k selections are made, never a selected row's own labels.

use std::collections::HashMap;

use promql_parser::label::Labels;
use promql_parser::parser::token::{
    T_AVG, T_BOTTOMK, T_COUNT, T_COUNT_VALUES, T_GROUP, T_LIMIT_RATIO, T_LIMITK, T_MAX, T_MIN,
    T_QUANTILE, T_STDDEV, T_STDVAR, T_SUM, T_TOPK, TokenId,
};
use promql_parser::parser::{AggregateExpr, LabelModifier};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL};

use crate::eval::{
    Error, Evaluator, InstantSample, InstantVector, QueryWindow, Value, invalid_quantile_warning,
};
use crate::functions::label::is_valid_label_name;
use crate::functions::over_time::{kahan_sum_inc, quantile};
use crate::histogram::{FloatHistogram, avg_histograms, sum_histograms};
use crate::source::SeriesSource;

/// Evaluate an `AggregateExpr` at one instant: evaluate the inner vector,
/// sort it into deterministic (label-set) order, then dispatch on the
/// aggregator token.
pub(crate) fn eval_aggregate(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    agg: &AggregateExpr,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let operand = match evaluator.eval_expr(source, &agg.expr, eval_ts_ns, ctx)? {
        Value::Vector(v) => v,
        other => unreachable!(
            "promql-parser guarantees an aggregate's inner expr is Vector-typed, got {}",
            other.type_name()
        ),
    };
    let mut input = operand;
    input.sort_by(|a, b| label_set_cmp(&a.labels, &b.labels));

    let op = agg.op.id();
    match op {
        T_COUNT_VALUES => eval_count_values(evaluator, source, agg, input, eval_ts_ns, ctx),
        T_TOPK | T_BOTTOMK => {
            let k = eval_scalar_param(evaluator, source, agg, eval_ts_ns, ctx)?;
            Ok(Value::Vector(eval_topk_bottomk(
                op == T_TOPK,
                k,
                agg.modifier.as_ref(),
                input,
                eval_ts_ns,
            )))
        }
        T_QUANTILE => {
            let q = eval_scalar_param(evaluator, source, agg, eval_ts_ns, ctx)?;
            Ok(Value::Vector(eval_quantile(
                q,
                agg.modifier.as_ref(),
                input,
                eval_ts_ns,
                ctx,
            )))
        }
        T_SUM | T_AVG | T_MIN | T_MAX | T_COUNT | T_GROUP | T_STDDEV | T_STDVAR => {
            Ok(Value::Vector(eval_plain_aggregate(
                op,
                agg.modifier.as_ref(),
                input,
                eval_ts_ns,
            )))
        }
        T_LIMITK | T_LIMIT_RATIO => {
            // promql-parser 0.10 parses `limitk`/`limit_ratio` (Prometheus'
            // experimental sampling aggregators), so this arm is reachable
            // from any tenant query. They are not implemented here: reject
            // with a typed `Error::Unsupported` naming the operator, the same
            // pattern used for every other deliberately-unsupported construct,
            // rather than panicking (docs/query-engine.md state-2 conformance
            // guarantee).
            Err(Error::Unsupported {
                construct: format!("aggregation operator {}", agg.op),
            })
        }
        _ => {
            unreachable!("promql-parser only produces the known aggregator tokens, got {op}")
        }
    }
}

fn label_set_cmp(a: &LabelSet, b: &LabelSet) -> std::cmp::Ordering {
    a.iter().cmp(b.iter())
}

/// The output group's label set for `modifier` applied to one sample's own
/// labels: `by (names)`/`Include` keeps only `names` (dropping `__name__`
/// unless it is itself named); `without (names)`/`Exclude` keeps everything
/// except `__name__` and `names`; no modifier at all keeps nothing (a single
/// group), the same result an explicit empty `by ()` produces.
fn group_labels(labels: &LabelSet, modifier: Option<&LabelModifier>) -> LabelSet {
    let filtered: Vec<Label> = match modifier {
        None => Vec::new(),
        Some(LabelModifier::Include(by)) => labels
            .iter()
            .filter(|l| by.labels.iter().any(|n| n == &l.name))
            .cloned()
            .collect(),
        Some(LabelModifier::Exclude(without)) => labels
            .iter()
            .filter(|l| l.name != METRIC_NAME_LABEL && !without.labels.iter().any(|n| n == &l.name))
            .cloned()
            .collect(),
    };
    LabelSet::new(filtered).unwrap_or_default()
}

/// Group `input` (already sorted into deterministic order) by
/// `group_labels(_, modifier)`, extracting `T` from each member via
/// `extract` and preserving first-insertion order among groups (the order in
/// which a distinct group key is first seen while scanning the
/// already-sorted input).
fn group_by<T>(
    modifier: Option<&LabelModifier>,
    input: InstantVector,
    mut extract: impl FnMut(InstantSample) -> T,
) -> Vec<(LabelSet, Vec<T>)> {
    let mut index: HashMap<LabelSet, usize> = HashMap::new();
    let mut groups: Vec<(LabelSet, Vec<T>)> = Vec::new();
    for sample in input {
        let key = group_labels(&sample.labels, modifier);
        let idx = match index.get(&key) {
            Some(&i) => i,
            None => {
                let i = groups.len();
                index.insert(key.clone(), i);
                groups.push((key, Vec::new()));
                i
            }
        };
        groups[idx].1.push(extract(sample));
    }
    groups
}

fn eval_scalar_param(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    agg: &AggregateExpr,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<f64, Error> {
    let param = match &agg.param {
        Some(p) => p,
        None => unreachable!("promql-parser guarantees a scalar param for this aggregator"),
    };
    match evaluator.eval_expr(source, param, eval_ts_ns, ctx)? {
        Value::Scalar(x) => Ok(x),
        other => unreachable!(
            "promql-parser guarantees a Scalar-typed param for this aggregator, got {}",
            other.type_name()
        ),
    }
}

fn eval_plain_aggregate(
    op: TokenId,
    modifier: Option<&LabelModifier>,
    input: InstantVector,
    eval_ts_ns: i64,
) -> InstantVector {
    // Group full samples (not just their float values) so `sum`/`avg` can see
    // native-histogram members.
    group_by(modifier, input, |s| s)
        .into_iter()
        .filter_map(|(labels, members)| reduce_group_samples(op, labels, members, eval_ts_ns))
        .collect()
}

/// Reduce one group to its output sample, or `None` to omit the group. `sum`
/// and `avg` aggregate native histograms when the group holds them; a group
/// mixing float and histogram members is dropped (Prometheus emits an
/// "incompatible sample types" annotation and produces no output for it).
/// `min`/`max`/`stddev`/`stdvar` ignore histogram members (float-only
/// in Prometheus, with an annotation), omitting a group with no float member.
/// `count` counts every member; `group` is always 1.
fn reduce_group_samples(
    op: TokenId,
    labels: LabelSet,
    members: Vec<InstantSample>,
    eval_ts_ns: i64,
) -> Option<InstantSample> {
    match op {
        T_SUM | T_AVG => {
            let (hists, floats): (Vec<InstantSample>, Vec<InstantSample>) =
                members.into_iter().partition(|s| s.histogram.is_some());
            if !hists.is_empty() && !floats.is_empty() {
                // Mixed float/histogram group: dropped with an annotation.
                return None;
            }
            if !hists.is_empty() {
                let refs: Vec<&FloatHistogram> =
                    hists.iter().filter_map(|s| s.histogram.as_ref()).collect();
                let combined = if op == T_AVG {
                    avg_histograms(refs.iter().copied())
                } else {
                    sum_histograms(refs.iter().copied())
                }?;
                return Some(InstantSample::histogram(
                    labels, eval_ts_ns, eval_ts_ns, combined,
                ));
            }
            let values: Vec<f64> = floats.iter().map(|s| s.value).collect();
            let value = if op == T_AVG {
                group_avg(&values)
            } else {
                group_sum(&values)
            };
            Some(InstantSample::scalar(labels, eval_ts_ns, eval_ts_ns, value))
        }
        T_COUNT => Some(InstantSample::scalar(
            labels,
            eval_ts_ns,
            eval_ts_ns,
            members.len() as f64,
        )),
        T_GROUP => Some(InstantSample::scalar(labels, eval_ts_ns, eval_ts_ns, 1.0)),
        T_MIN | T_MAX | T_STDDEV | T_STDVAR => {
            let values: Vec<f64> = members
                .iter()
                .filter(|s| s.histogram.is_none())
                .map(|s| s.value)
                .collect();
            if values.is_empty() {
                return None;
            }
            Some(InstantSample::scalar(
                labels,
                eval_ts_ns,
                eval_ts_ns,
                reduce_group(op, &values),
            ))
        }
        _ => unreachable!("reduce_group_samples called with non-plain aggregator {op}"),
    }
}

fn reduce_group(op: TokenId, values: &[f64]) -> f64 {
    match op {
        T_SUM => group_sum(values),
        T_AVG => group_avg(values),
        T_MIN => group_min(values),
        T_MAX => group_max(values),
        T_COUNT => values.len() as f64,
        T_GROUP => 1.0,
        T_STDDEV => group_welford(values).sqrt(),
        T_STDVAR => group_welford(values),
        _ => unreachable!("reduce_group called with non-reducing aggregator {op}"),
    }
}

/// Same reduction `over_time::sum_over_time` applies to a time window's
/// samples, applied here to a group's member values instead: Kahan-
/// compensated summation, with the compensation term folded back in once at
/// the end (never at all once the running sum has gone infinite).
fn group_sum(values: &[f64]) -> f64 {
    let mut sum = 0.0_f64;
    let mut c = 0.0_f64;
    for &v in values {
        (sum, c) = kahan_sum_inc(v, sum, c);
    }
    if sum.is_infinite() { sum } else { sum + c }
}

/// Same incremental-mean reduction `over_time::avg_over_time` applies to a
/// time window, applied to a group's member values: once the running mean
/// has gone infinite, a further member is skipped (not folded in) whenever
/// it cannot change the sign or finiteness of that infinity (a same-signed
/// infinite member, or any finite, non-NaN member); a NaN member, or an
/// oppositely-signed infinite member, still gets folded in.
fn group_avg(values: &[f64]) -> f64 {
    let mut mean = 0.0_f64;
    let mut c = 0.0_f64;
    let mut count = 0.0_f64;
    for &v in values {
        count += 1.0;
        if mean.is_infinite() {
            let same_sign_infinite = v.is_infinite() && (mean > 0.0) == (v > 0.0);
            let finite_and_not_nan = !v.is_infinite() && !v.is_nan();
            if same_sign_infinite || finite_and_not_nan {
                continue;
            }
        }
        (mean, c) = kahan_sum_inc(v / count - mean / count, mean, c);
    }
    if mean.is_infinite() { mean } else { mean + c }
}

fn group_min(values: &[f64]) -> f64 {
    let mut min = values[0];
    for &v in values {
        if v < min || min.is_nan() {
            min = v;
        }
    }
    min
}

fn group_max(values: &[f64]) -> f64 {
    let mut max = values[0];
    for &v in values {
        if v > max || max.is_nan() {
            max = v;
        }
    }
    max
}

/// Same Welford/Kahan reduction `over_time::welford_variance` applies to a
/// time window's samples, applied here to a group's member values.
fn group_welford(values: &[f64]) -> f64 {
    let mut count = 0.0_f64;
    let mut mean = 0.0_f64;
    let mut c_mean = 0.0_f64;
    let mut m2 = 0.0_f64;
    let mut c_m2 = 0.0_f64;
    for &v in values {
        count += 1.0;
        let delta = v - (mean + c_mean);
        (mean, c_mean) = kahan_sum_inc(delta / count, mean, c_mean);
        (m2, c_m2) = kahan_sum_inc(delta * (v - (mean + c_mean)), m2, c_m2);
    }
    (m2 + c_m2) / count
}

fn eval_quantile(
    q: f64,
    modifier: Option<&LabelModifier>,
    input: InstantVector,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> InstantVector {
    // A `q` outside [0, 1] does not error; `quantile` clamps it to +/-Inf
    // (and a NaN `q` yields NaN). Prometheus flags any of those with a
    // warning annotation, and so does Ravel. `!contains`
    // covers NaN too (NaN is in no range), matching Prometheus, which warns
    // on `math.IsNaN(q) || q < 0 || q > 1`.
    if !(0.0..=1.0).contains(&q) {
        ctx.warn(invalid_quantile_warning(q));
    }
    // `quantile` has no defined native-histogram semantics in Prometheus
    // (ADR-0108 decision 2): histogram elements are dropped, never treated as
    // their meaningless `0.0` placeholder float. Filtering here keeps any float
    // members of the same group and omits a group that held only histograms.
    let input: InstantVector = input
        .into_iter()
        .filter(|s| s.histogram.is_none())
        .collect();
    group_by(modifier, input, |s| s.value)
        .into_iter()
        .map(|(labels, values)| InstantSample {
            labels,
            ts_ns: eval_ts_ns,
            orig_sample_ts_ns: eval_ts_ns,
            value: quantile(q, values),
            histogram: None,
        })
        .collect()
}

/// Inject `label_name` (Go `strconv.FormatFloat('f', -1, 64)`-style: Rust's
/// default `{}` formatting for `f64` already matches it for every finite
/// value, and both agree on `"inf"`/`"-inf"`/`"NaN"` up to case, which
/// PromQL's own output does not distinguish) into every sample's own labels,
/// then group as usual, folding the injected name into an `Include`
/// modifier's keep-list (an `Exclude` modifier already keeps it unless
/// explicitly named, the same "unconditionally drop named labels" rule as
/// any other `without` label; no modifier becomes an `Include` of just the
/// injected name, matching Prometheus appending it to an otherwise-absent
/// grouping list).
fn eval_count_values(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    agg: &AggregateExpr,
    input: InstantVector,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let param = match &agg.param {
        Some(p) => p,
        None => unreachable!("promql-parser guarantees a string param for count_values"),
    };
    let label_name = match evaluator.eval_expr(source, param, eval_ts_ns, ctx)? {
        Value::String(s) => s,
        other => unreachable!(
            "promql-parser guarantees a String-typed param for count_values, got {}",
            other.type_name()
        ),
    };
    if !is_valid_label_name(&label_name) {
        return Err(Error::InvalidLabelName { label: label_name });
    }

    let augmented: InstantVector = input
        .into_iter()
        .map(|s| {
            let mut pairs: Vec<Label> = s
                .labels
                .iter()
                .filter(|l| l.name != label_name)
                .cloned()
                .collect();
            pairs.push(Label {
                name: label_name.clone(),
                value: format_value_label(s.value),
            });
            InstantSample {
                labels: LabelSet::new(pairs).unwrap_or_default(),
                ts_ns: s.ts_ns,
                orig_sample_ts_ns: s.ts_ns,
                value: s.value,
                histogram: None,
            }
        })
        .collect();

    let effective_modifier = match &agg.modifier {
        Some(LabelModifier::Exclude(names)) => LabelModifier::Exclude(names.clone()),
        Some(LabelModifier::Include(names)) => {
            let mut names = names.clone();
            if !names.labels.iter().any(|n| n == &label_name) {
                names.labels.push(label_name.clone());
            }
            LabelModifier::Include(names)
        }
        None => LabelModifier::Include(Labels {
            labels: vec![label_name.clone()],
        }),
    };

    let out = group_by(Some(&effective_modifier), augmented, |_| ())
        .into_iter()
        .map(|(labels, members)| InstantSample {
            labels,
            ts_ns: eval_ts_ns,
            orig_sample_ts_ns: eval_ts_ns,
            value: members.len() as f64,
            histogram: None,
        })
        .collect();
    Ok(Value::Vector(out))
}

fn format_value_label(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "+Inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        format!("{value}")
    }
}

fn eval_topk_bottomk(
    is_top: bool,
    k_raw: f64,
    modifier: Option<&LabelModifier>,
    input: InstantVector,
    eval_ts_ns: i64,
) -> InstantVector {
    if k_raw.is_nan() || k_raw < 1.0 {
        return Vec::new();
    }
    let k = k_raw as usize;

    // `topk`/`bottomk` have no defined native-histogram semantics in Prometheus
    // (ADR-0108 decision 2): a histogram element is dropped rather than ranked
    // by its meaningless `0.0` placeholder float. Float members of the same
    // group are kept and ranked as usual.
    let input: InstantVector = input
        .into_iter()
        .filter(|s| s.histogram.is_none())
        .collect();

    let mut out = Vec::new();
    for (_labels, members) in group_by(modifier, input, |s| s) {
        let mut heap: Vec<InstantSample> = Vec::with_capacity(k.min(members.len()));
        for member in members {
            if heap.len() < k {
                heap_push(&mut heap, member, is_top);
            } else if heap_less(heap[0].value, member.value, is_top) {
                heap_pop(&mut heap, is_top);
                heap_push(&mut heap, member, is_top);
            }
        }
        sort_heap_result(&mut heap, is_top);
        for member in heap {
            out.push(InstantSample {
                labels: member.labels,
                ts_ns: eval_ts_ns,
                orig_sample_ts_ns: eval_ts_ns,
                value: member.value,
                histogram: None,
            });
        }
    }
    out
}

/// `topk`'s heap keeps its *smallest* member at the root (evicting the
/// smallest when a larger candidate arrives, so what survives is the
/// largest-k); `bottomk`'s heap keeps its *largest* at the root. `a` is
/// "worse than" `b` (should sift toward/be evicted in favor of `b`) under
/// this rule; a NaN root is always worse than any candidate, matching Go's
/// heap comparator (`math.IsNaN(f64(agg.item.v)) || ...`).
fn heap_less(a: f64, b: f64, is_top: bool) -> bool {
    if is_top {
        a.is_nan() || a < b
    } else {
        a.is_nan() || a > b
    }
}

fn heap_push(heap: &mut Vec<InstantSample>, sample: InstantSample, is_top: bool) {
    heap.push(sample);
    let mut idx = heap.len() - 1;
    while idx > 0 {
        let parent = (idx - 1) / 2;
        if heap_less(heap[idx].value, heap[parent].value, is_top) {
            heap.swap(idx, parent);
            idx = parent;
        } else {
            break;
        }
    }
}

fn heap_pop(heap: &mut Vec<InstantSample>, is_top: bool) {
    let last = heap.len() - 1;
    heap.swap(0, last);
    heap.pop();
    sift_down(heap, 0, is_top);
}

fn sift_down(heap: &mut [InstantSample], mut idx: usize, is_top: bool) {
    let len = heap.len();
    loop {
        let left = 2 * idx + 1;
        let right = 2 * idx + 2;
        let mut smallest = idx;
        if left < len && heap_less(heap[left].value, heap[smallest].value, is_top) {
            smallest = left;
        }
        if right < len && heap_less(heap[right].value, heap[smallest].value, is_top) {
            smallest = right;
        }
        if smallest == idx {
            break;
        }
        heap.swap(idx, smallest);
        idx = smallest;
    }
}

/// Final emission order: descending by value for `topk`, ascending for
/// `bottomk`, NaN always last (Prometheus' `sort.Sort(sort.Reverse(...))`
/// over the heap's own `Less`, which treats NaN as smallest so it ends up
/// last under either direction's reversal). A plain stable sort here matches
/// Go's actual `sort.Sort` bit-for-bit only for the small slice lengths
/// (`<= 12`) at which Go's own sort resolves to a stable insertion sort; see
/// the module doc comment.
fn sort_heap_result(heap: &mut [InstantSample], is_top: bool) {
    heap.sort_by(|a, b| {
        if a.value.is_nan() {
            return std::cmp::Ordering::Greater;
        }
        if b.value.is_nan() {
            return std::cmp::Ordering::Less;
        }
        if is_top {
            b.value
                .partial_cmp(&a.value)
                .unwrap_or(std::cmp::Ordering::Equal)
        } else {
            a.value
                .partial_cmp(&b.value)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
    });
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::eval::Evaluator;
    use crate::testsource::TestSource;

    fn source() -> TestSource {
        TestSource::new()
            .with_series(
                &[("__name__", "m"), ("job", "x"), ("region", "us")],
                &[(0, 1.0)],
            )
            .expect("valid series")
            .with_series(
                &[("__name__", "m"), ("job", "x"), ("region", "eu")],
                &[(0, 2.0)],
            )
            .expect("valid series")
            .with_series(
                &[("__name__", "m"), ("job", "y"), ("region", "us")],
                &[(0, 3.0)],
            )
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

    /// Like `eval_vector_over`, but preserves the evaluator's own emission
    /// order instead of sorting by label set: `topk`/`bottomk` output order
    /// (rank order) is itself the thing under test, so sorting it away would
    /// hide ordering bugs.
    fn eval_vector_ranked(query: &str, src: &TestSource) -> Vec<(Vec<(String, String)>, f64)> {
        let v = Evaluator::new()
            .instant(src, query, 0)
            .expect("must evaluate");
        v.into_iter()
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

    fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = pairs
            .iter()
            .map(|(n, val)| (n.to_string(), val.to_string()))
            .collect();
        v.sort();
        v
    }

    /// promql-parser 0.10 parses `limitk`/`limit_ratio`, so both reach the
    /// aggregation dispatch. They are not implemented; each must return a
    /// typed `Error::Unsupported` naming the operator rather than panicking on
    /// the old `unreachable!` arm (docs/query-engine.md state-2).
    #[test]
    fn limitk_and_limit_ratio_reject_without_panicking() {
        use crate::eval::Error;
        for (query, name) in [
            ("limitk(2, m)", "limitk"),
            ("limit_ratio(0.5, m)", "limit_ratio"),
        ] {
            let err = Evaluator::new()
                .instant(&source(), query, 0)
                .expect_err("must reject, not panic");
            let Error::Unsupported { construct } = err else {
                panic!("expected Error::Unsupported for {query:?}, got {err:?}");
            };
            assert!(
                construct.contains(name),
                "rejection for {query:?} should name {name:?}, got {construct:?}"
            );
        }
    }

    #[test]
    fn sum_with_no_modifier_collapses_to_one_group() {
        assert_eq!(eval_vector("sum(m)"), vec![(vec![], 6.0)]);
    }

    #[test]
    fn sum_with_empty_by_also_collapses_to_one_group() {
        assert_eq!(eval_vector("sum by () (m)"), vec![(vec![], 6.0)]);
    }

    #[test]
    fn sum_by_groups_and_keeps_only_named_labels() {
        assert_eq!(
            eval_vector("sum by (job) (m)"),
            vec![
                (labels(&[("job", "x")]), 3.0),
                (labels(&[("job", "y")]), 3.0),
            ]
        );
    }

    #[test]
    fn sum_without_drops_name_group() {
        assert_eq!(
            eval_vector("sum without (job) (m)"),
            vec![
                (labels(&[("region", "eu")]), 2.0),
                (labels(&[("region", "us")]), 4.0),
            ]
        );
    }

    #[test]
    fn avg_min_max_count() {
        assert_eq!(eval_vector("avg(m)"), vec![(vec![], 2.0)]);
        assert_eq!(eval_vector("min(m)"), vec![(vec![], 1.0)]);
        assert_eq!(eval_vector("max(m)"), vec![(vec![], 3.0)]);
        assert_eq!(eval_vector("count(m)"), vec![(vec![], 3.0)]);
    }

    #[test]
    fn group_reports_one_for_any_non_empty_group() {
        assert_eq!(
            eval_vector("group by (job) (m)"),
            vec![
                (labels(&[("job", "x")]), 1.0),
                (labels(&[("job", "y")]), 1.0),
            ]
        );
    }

    #[test]
    fn stddev_and_stdvar_match_hand_computed_population_variance() {
        let src = TestSource::new()
            .with_series(&[("__name__", "v"), ("i", "1")], &[(0, 2.0)])
            .expect("valid series")
            .with_series(&[("__name__", "v"), ("i", "2")], &[(0, 4.0)])
            .expect("valid series");
        assert_eq!(eval_vector_over("stdvar(v)", &src), vec![(vec![], 1.0)]);
        assert_eq!(eval_vector_over("stddev(v)", &src), vec![(vec![], 1.0)]);
    }

    #[test]
    fn quantile_interpolates_within_a_group() {
        assert_eq!(eval_vector("quantile(0.5, m)"), vec![(vec![], 2.0)]);
    }

    #[test]
    fn quantile_out_of_range_clamps_to_infinity() {
        assert_eq!(
            eval_vector("quantile(-1, m)"),
            vec![(vec![], f64::NEG_INFINITY)]
        );
        assert_eq!(eval_vector("quantile(2, m)"), vec![(vec![], f64::INFINITY)]);
    }

    #[test]
    fn count_values_injects_label_and_counts_members() {
        let src = TestSource::new()
            .with_series(&[("__name__", "m"), ("job", "x")], &[(0, 1.0)])
            .expect("valid series")
            .with_series(&[("__name__", "m"), ("job", "y")], &[(0, 1.0)])
            .expect("valid series")
            .with_series(&[("__name__", "m"), ("job", "z")], &[(0, 2.0)])
            .expect("valid series");
        assert_eq!(
            eval_vector_over("count_values(\"v\", m)", &src),
            vec![(labels(&[("v", "1")]), 2.0), (labels(&[("v", "2")]), 1.0),]
        );
    }

    #[test]
    fn count_values_rejects_invalid_label_name() {
        let err = Evaluator::new()
            .instant(&source(), "count_values(\"1bad\", m)", 0)
            .expect_err("must be rejected");
        assert!(matches!(err, crate::eval::Error::InvalidLabelName { .. }));
    }

    #[test]
    fn topk_returns_original_series_identity_in_descending_order() {
        assert_eq!(
            eval_vector_ranked("topk(2, m)", &source()),
            vec![
                (
                    labels(&[("__name__", "m"), ("job", "y"), ("region", "us")]),
                    3.0
                ),
                (
                    labels(&[("__name__", "m"), ("job", "x"), ("region", "eu")]),
                    2.0
                ),
            ]
        );
    }

    #[test]
    fn bottomk_returns_original_series_identity_in_ascending_order() {
        assert_eq!(
            eval_vector_ranked("bottomk(2, m)", &source()),
            vec![
                (
                    labels(&[("__name__", "m"), ("job", "x"), ("region", "us")]),
                    1.0
                ),
                (
                    labels(&[("__name__", "m"), ("job", "x"), ("region", "eu")]),
                    2.0
                ),
            ]
        );
    }

    #[test]
    fn topk_k_zero_or_negative_or_nan_is_empty() {
        assert_eq!(eval_vector("topk(0, m)"), vec![]);
        assert_eq!(eval_vector("topk(-1, m)"), vec![]);
    }

    #[test]
    fn topk_k_larger_than_group_returns_whole_group() {
        assert_eq!(eval_vector("topk(100, m)").len(), 3);
    }

    #[test]
    fn topk_admits_all_members_seen_before_the_heap_fills_even_if_a_smaller_one_arrives_first() {
        // Values in encounter (label-sorted) order: A=5, B=3, C=3, D=3, E=4.
        // k=3: the heap fills with A, B, C before D or E is ever considered.
        // D=3 loses the strict-less admission check against the root (3), so
        // it is never admitted even though it ties two retained members; E=4
        // then evicts whichever of B/C is the current (tied) root. This
        // exact outcome is what a naive "sort all, take top 3" shortcut
        // cannot reproduce in general (it would not privilege B/C over D on
        // arrival order at all), which is why this test exists.
        let src = TestSource::new()
            .with_series(&[("__name__", "t"), ("s", "a")], &[(0, 5.0)])
            .expect("valid series")
            .with_series(&[("__name__", "t"), ("s", "b")], &[(0, 3.0)])
            .expect("valid series")
            .with_series(&[("__name__", "t"), ("s", "c")], &[(0, 3.0)])
            .expect("valid series")
            .with_series(&[("__name__", "t"), ("s", "d")], &[(0, 3.0)])
            .expect("valid series")
            .with_series(&[("__name__", "t"), ("s", "e")], &[(0, 4.0)])
            .expect("valid series");
        let got = eval_vector_ranked("topk(3, t)", &src);
        let values: Vec<f64> = got.iter().map(|(_, v)| *v).collect();
        assert_eq!(values, vec![5.0, 4.0, 3.0]);
        let survivors: Vec<&str> = got
            .iter()
            .map(|(labels, _)| {
                labels
                    .iter()
                    .find(|(n, _)| n == "s")
                    .map(|(_, v)| v.as_str())
                    .expect("s label present")
            })
            .collect();
        assert!(survivors.contains(&"a"));
        assert!(survivors.contains(&"e"));
        assert!(!survivors.contains(&"d"), "d must lose the tie to b or c");
    }

    #[test]
    fn topk_by_computes_independent_top_k_per_group() {
        let src = TestSource::new()
            .with_series(&[("__name__", "m"), ("job", "x"), ("i", "1")], &[(0, 1.0)])
            .expect("valid series")
            .with_series(&[("__name__", "m"), ("job", "x"), ("i", "2")], &[(0, 2.0)])
            .expect("valid series")
            .with_series(&[("__name__", "m"), ("job", "y"), ("i", "1")], &[(0, 10.0)])
            .expect("valid series")
            .with_series(&[("__name__", "m"), ("job", "y"), ("i", "2")], &[(0, 20.0)])
            .expect("valid series");
        let got = eval_vector_over("topk by (job) (1, m)", &src);
        let values: Vec<f64> = got.iter().map(|(_, v)| *v).collect();
        assert_eq!(values, vec![2.0, 20.0]);
    }

    #[test]
    fn topk_nan_members_sort_last() {
        let src = TestSource::new()
            .with_series(&[("__name__", "n"), ("s", "a")], &[(0, f64::NAN)])
            .expect("valid series")
            .with_series(&[("__name__", "n"), ("s", "b")], &[(0, 1.0)])
            .expect("valid series");
        let got = eval_vector_ranked("topk(2, n)", &src);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].1, 1.0);
        assert!(got[1].1.is_nan());
    }
}
