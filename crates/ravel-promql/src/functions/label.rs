//! Label-manipulation function family (plan section 10/P6): `label_replace`,
//! `label_join`, `absent`. Ports of Prometheus' `promql/functions.go`
//! `funcLabelReplace`, `funcLabelJoin`, `funcAbsent` /
//! `createLabelsForAbsentFunction`.
//!
//! None of these three drop `__name__`: `label_replace`/`label_join` only
//! ever touch the destination label they are told to (which callers must
//! not name `__name__` in practice, though nothing here forbids it), and
//! `absent`'s output has no metric name to begin with (it starts from an
//! empty label set).

use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL};

use crate::eval::{Error, Evaluator, InstantSample, QueryWindow, Value};
use crate::source::SeriesSource;

use super::{FunctionDef, FunctionKind, string_arg, vector_arg};

pub(crate) const FUNCTIONS: &[FunctionDef] = &[
    FunctionDef {
        name: "label_replace",
        kind: FunctionKind::Instant(label_replace),
    },
    FunctionDef {
        name: "label_join",
        kind: FunctionKind::Instant(label_join),
    },
    FunctionDef {
        name: "absent",
        kind: FunctionKind::Instant(absent),
    },
];

/// `^[a-zA-Z_][a-zA-Z0-9_]*$`, Prometheus' `model.LabelNameRE`.
pub(crate) fn is_valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Rebuild `labels` with `name` set to `value`, or removed entirely if
/// `value` is empty: Prometheus' `labels.Builder.Set` treats setting a
/// label to `""` as deleting it, and both `label_replace` and `label_join`
/// rely on that to drop a destination label the computed value emptied
/// out.
fn set_or_delete(labels: &LabelSet, name: &str, value: &str) -> LabelSet {
    let mut out: Vec<Label> = labels.iter().filter(|l| l.name != name).cloned().collect();
    if !value.is_empty() {
        out.push(Label {
            name: name.to_string(),
            value: value.to_string(),
        });
    }
    LabelSet::new(out).unwrap_or_default()
}

fn label_replace(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let v = vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
    let dst = string_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
    let replacement = string_arg(evaluator, source, &call.args.args[2], eval_ts_ns, ctx)?;
    let src = string_arg(evaluator, source, &call.args.args[3], eval_ts_ns, ctx)?;
    let pattern = string_arg(evaluator, source, &call.args.args[4], eval_ts_ns, ctx)?;

    if !is_valid_label_name(&dst) {
        return Err(Error::InvalidLabelName { label: dst });
    }
    let re = regex::Regex::new(&format!("^(?:{pattern})$")).map_err(|e| Error::InvalidRegex {
        pattern: pattern.clone(),
        reason: e.to_string(),
    })?;

    let out = v
        .into_iter()
        .map(|s| {
            let src_val = s.labels.get(&src).unwrap_or("");
            let labels = match re.captures(src_val) {
                Some(caps) => {
                    let mut expanded = String::new();
                    caps.expand(&replacement, &mut expanded);
                    set_or_delete(&s.labels, &dst, &expanded)
                }
                None => s.labels,
            };
            InstantSample {
                labels,
                ts_ns: s.ts_ns,
                orig_sample_ts_ns: s.ts_ns,
                value: s.value,
                histogram: None,
            }
        })
        .collect();
    Ok(Value::Vector(out))
}

fn label_join(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let v = vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
    let dst = string_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
    let separator = string_arg(evaluator, source, &call.args.args[2], eval_ts_ns, ctx)?;
    if !is_valid_label_name(&dst) {
        return Err(Error::InvalidLabelName { label: dst });
    }
    let mut src_labels = Vec::with_capacity(call.args.args.len().saturating_sub(3));
    for expr in &call.args.args[3..] {
        src_labels.push(string_arg(evaluator, source, expr, eval_ts_ns, ctx)?);
    }

    let out = v
        .into_iter()
        .map(|s| {
            let joined = src_labels
                .iter()
                .map(|name| s.labels.get(name).unwrap_or(""))
                .collect::<Vec<_>>()
                .join(&separator);
            InstantSample {
                labels: set_or_delete(&s.labels, &dst, &joined),
                ts_ns: s.ts_ns,
                orig_sample_ts_ns: s.ts_ns,
                value: s.value,
                histogram: None,
            }
        })
        .collect();
    Ok(Value::Vector(out))
}

fn absent(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let v = vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
    if !v.is_empty() {
        return Ok(Value::Vector(Vec::new()));
    }
    let labels = labels_for_absent(&call.args.args[0]);
    Ok(Value::Vector(vec![InstantSample {
        labels,
        ts_ns: eval_ts_ns,
        orig_sample_ts_ns: eval_ts_ns,
        value: 1.0,
        histogram: None,
    }]))
}

/// Mirrors Prometheus' `createLabelsForAbsentFunction`: if `expr` (after
/// unwrapping `Paren`) is literally a bare vector selector, derive labels
/// from its matchers as described in [`vector_selector_labels`]. Any other
/// argument shape (a bare selector combined with `or`, or any composite
/// expression) yields an empty label set, matching Prometheus' behavior for
/// every case its own switch does not special-case.
fn labels_for_absent(expr: &promql_parser::parser::Expr) -> LabelSet {
    use promql_parser::parser::Expr;

    let mut cur = expr;
    loop {
        match cur {
            Expr::Paren(p) => cur = &p.expr,
            Expr::VectorSelector(vs) => return vector_selector_labels(vs),
            _ => return LabelSet::default(),
        }
    }
}

/// Ports Prometheus' `createLabelsForAbsentFunction` matcher walk exactly:
/// `__name__` is always skipped; an equality matcher with an empty value
/// removes that name and blocks it from being (re-)set by a later matcher in
/// the same call; the *first* equality matcher for a given name (with a
/// non-empty value) sets it; every matcher after that for the same name -
/// equality or not, same value or different - removes it again rather than
/// overwriting it (this is Prometheus' own documented quirk: `absent(x{job=
/// "a",job="b",foo="bar"})` drops `job` entirely, not just on a value
/// conflict).
fn vector_selector_labels(vs: &promql_parser::parser::VectorSelector) -> LabelSet {
    use promql_parser::label::MatchOp;
    use std::collections::HashMap;

    if !vs.matchers.or_matchers.is_empty() {
        return LabelSet::default();
    }
    let mut values: HashMap<&str, String> = HashMap::new();
    let mut has: HashMap<&str, bool> = HashMap::new();
    for m in &vs.matchers.matchers {
        if m.name == METRIC_NAME_LABEL {
            continue;
        }
        if matches!(m.op, MatchOp::Equal) && m.value.is_empty() {
            has.insert(m.name.as_str(), false);
            values.remove(m.name.as_str());
            continue;
        }
        let already_set = *has.get(m.name.as_str()).unwrap_or(&false);
        if matches!(m.op, MatchOp::Equal) && !already_set {
            values.insert(m.name.as_str(), m.value.clone());
            has.insert(m.name.as_str(), true);
        } else {
            values.remove(m.name.as_str());
            has.insert(m.name.as_str(), false);
        }
    }
    let labels: Vec<Label> = values
        .into_iter()
        .map(|(name, value)| Label {
            name: name.to_string(),
            value,
        })
        .collect();
    LabelSet::new(labels).unwrap_or_default()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_types::{Label, LabelSet};

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: n.to_string(),
                    value: v.to_string(),
                })
                .collect(),
        )
        .expect("distinct label names")
    }

    #[test]
    fn valid_label_names() {
        assert!(is_valid_label_name("foo"));
        assert!(is_valid_label_name("_foo123"));
        assert!(!is_valid_label_name("1foo"));
        assert!(!is_valid_label_name("foo-bar"));
        assert!(!is_valid_label_name(""));
    }

    #[test]
    fn set_or_delete_removes_on_empty_value() {
        let base = labels(&[("a", "1")]);
        let set = set_or_delete(&base, "b", "2");
        assert_eq!(set.get("b"), Some("2"));
        let deleted = set_or_delete(&set, "b", "");
        assert_eq!(deleted.get("b"), None);
        assert_eq!(deleted.get("a"), Some("1"));
    }

    #[test]
    fn label_replace_expands_captures() {
        let re = regex::Regex::new("^(?:(.*)-(.*))$").expect("valid pattern");
        let caps = re.captures("east-1").expect("pattern matches");
        let mut expanded = String::new();
        caps.expand("$1/$2", &mut expanded);
        assert_eq!(expanded, "east/1");
    }

    fn selector_labels(query: &str) -> LabelSet {
        let expr = promql_parser::parser::parse(query).expect("parses");
        labels_for_absent(&expr)
    }

    #[test]
    fn absent_labels_come_from_equality_matchers_excluding_name() {
        let set = selector_labels(r#"nonexistent{job="api",instance="1"}"#);
        assert_eq!(set.get("job"), Some("api"));
        assert_eq!(set.get("instance"), Some("1"));
        assert_eq!(set.get("__name__"), None);
    }

    #[test]
    fn absent_labels_drop_a_name_matched_twice_even_with_the_same_value() {
        // Prometheus' own documented quirk: a second matcher on a name
        // already set removes it, regardless of whether the value agrees.
        let dropped_same = selector_labels(r#"nonexistent{job="api",job="api"}"#);
        assert_eq!(dropped_same.get("job"), None);

        let dropped_conflict = selector_labels(r#"nonexistent{job="api",job="other"}"#);
        assert_eq!(dropped_conflict.get("job"), None);
    }

    #[test]
    fn absent_labels_skip_empty_value_and_non_equality_matchers() {
        let set = selector_labels(r#"nonexistent{job="",instance=~"1|2",region="east"}"#);
        assert_eq!(set.get("job"), None);
        assert_eq!(set.get("instance"), None);
        assert_eq!(set.get("region"), Some("east"));
    }

    #[test]
    fn absent_labels_are_empty_for_a_composite_expression() {
        let set = selector_labels(r#"nonexistent{job="api"} or nonexistent{job="other"}"#);
        assert!(set.is_empty());
    }
}
