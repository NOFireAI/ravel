//! Matcher evaluation and AST conversion (ADR-0007).
//!
//! Prometheus rule, applied uniformly to all four operators: a label that is
//! absent from a series behaves exactly as if it were present with the
//! empty string as its value. That single rule is what produces every one
//! of the documented edge cases:
//!
//! - `{foo=""}` matches series without `foo` (absent reads as `""`, `"" ==
//!   ""`).
//! - `{foo=~".*"}` matches everything, including series without `foo`
//!   (`.*` matches `""` too).
//! - `{foo!=""}` matches only series where `foo` is present and non-empty
//!   (absent reads as `""`, and `"" != ""` is false).
//! - `{foo=~""}` matches only series without `foo` (anchored `^(?:)$` only
//!   matches the empty string).
//!
//! No special-casing is needed anywhere below; treating absence as `""` and
//! applying the operator normally is sufficient.

use ravel_types::LabelSet;

use crate::source::{LabelMatcher, MatchOp};

impl LabelMatcher {
    /// Whether `labels` satisfies this matcher, per the module-level rule.
    pub fn is_match(&self, labels: &LabelSet) -> bool {
        let value = labels.get(&self.name).unwrap_or("");
        match &self.op {
            MatchOp::Eq => self.value == value,
            MatchOp::Ne => self.value != value,
            MatchOp::Re(re) => re.is_match(value),
            MatchOp::Nre(re) => !re.is_match(value),
        }
    }
}

/// Whether `labels` satisfies every matcher in `matchers` (an empty slice
/// matches everything).
pub fn matches_series(matchers: &[LabelMatcher], labels: &LabelSet) -> bool {
    matchers.iter().all(|m| m.is_match(labels))
}

/// Convert one promql-parser AST matcher into our [`LabelMatcher`], reusing
/// its already-anchored compiled `Regex` for `=~`/`!~` rather than
/// recompiling: promql-parser anchors as `^(?:value)$` (with a fallback
/// escape for Go-style `{n}` repeat patterns) at parse time, so cloning its
/// `Regex` guarantees identical anchoring to the parsed query.
pub fn from_ast_matcher(m: &promql_parser::label::Matcher) -> LabelMatcher {
    let (op, value) = match &m.op {
        promql_parser::label::MatchOp::Equal => (MatchOp::Eq, m.value.clone()),
        promql_parser::label::MatchOp::NotEqual => (MatchOp::Ne, m.value.clone()),
        promql_parser::label::MatchOp::Re(re) => (MatchOp::Re(re.clone()), m.value.clone()),
        promql_parser::label::MatchOp::NotRe(re) => (MatchOp::Nre(re.clone()), m.value.clone()),
    };
    LabelMatcher {
        name: m.name.clone(),
        op,
        value,
    }
}

/// Convert every plain (non-`or`-grouped) matcher on a parsed `Matchers`
/// into our `LabelMatcher` list. Use [`has_or_group`] first: this evaluator
/// does not support the `or`-grouped alternative-matcher extension.
pub fn from_ast_matchers(m: &promql_parser::label::Matchers) -> Vec<LabelMatcher> {
    m.matchers.iter().map(from_ast_matcher).collect()
}

/// Whether `m` uses the `or`-grouped alternative-matcher extension, which
/// this evaluator rejects as unsupported.
pub fn has_or_group(m: &promql_parser::label::Matchers) -> bool {
    !m.or_matchers.is_empty()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_types::Label;

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: (*n).to_string(),
                    value: (*v).to_string(),
                })
                .collect(),
        )
        .expect("valid labels")
    }

    #[test]
    fn empty_string_eq_matches_absent_label() {
        let m = LabelMatcher::equal("foo", "");
        assert!(m.is_match(&labels(&[])));
        assert!(!m.is_match(&labels(&[("foo", "bar")])));
    }

    #[test]
    fn dot_star_regex_matches_everything_including_absent() {
        let m = LabelMatcher::regex("foo", ".*").expect("compiles");
        assert!(m.is_match(&labels(&[])));
        assert!(m.is_match(&labels(&[("foo", "bar")])));
        assert!(m.is_match(&labels(&[("foo", "")])));
    }

    #[test]
    fn not_equal_empty_requires_present_and_non_empty() {
        let m = LabelMatcher::not_equal("foo", "");
        assert!(!m.is_match(&labels(&[])));
        assert!(m.is_match(&labels(&[("foo", "bar")])));
    }

    #[test]
    fn regex_empty_matches_only_absent() {
        let m = LabelMatcher::regex("foo", "").expect("compiles");
        assert!(m.is_match(&labels(&[])));
        assert!(!m.is_match(&labels(&[("foo", "bar")])));
        assert!(!m.is_match(&labels(&[("foo", "x")])));
    }

    #[test]
    fn regex_is_fully_anchored_like_prometheus() {
        // `job=~"api"` must not match "api-server": Prometheus anchors
        // every regex matcher as `^(?:pattern)$`.
        let m = LabelMatcher::regex("job", "api").expect("compiles");
        assert!(m.is_match(&labels(&[("job", "api")])));
        assert!(!m.is_match(&labels(&[("job", "api-server")])));
    }

    #[test]
    fn not_regex_negates_the_anchored_match() {
        let m = LabelMatcher::not_regex("job", "api").expect("compiles");
        assert!(!m.is_match(&labels(&[("job", "api")])));
        assert!(m.is_match(&labels(&[("job", "api-server")])));
    }

    #[test]
    fn matches_series_requires_all_matchers() {
        let matchers = vec![
            LabelMatcher::equal("__name__", "up"),
            LabelMatcher::equal("job", "api"),
        ];
        assert!(matches_series(
            &matchers,
            &labels(&[("__name__", "up"), ("job", "api")])
        ));
        assert!(!matches_series(
            &matchers,
            &labels(&[("__name__", "up"), ("job", "web")])
        ));
    }

    #[test]
    fn from_ast_matcher_reuses_anchored_regex() {
        let expr = promql_parser::parser::parse(r#"up{job=~"api"}"#).expect("parses");
        let promql_parser::parser::Expr::VectorSelector(vs) = expr else {
            panic!("expected vector selector");
        };
        let converted: Vec<LabelMatcher> = from_ast_matchers(&vs.matchers);
        assert_eq!(converted.len(), 1);
        assert!(converted[0].is_match(&labels(&[("job", "api")])));
        assert!(!converted[0].is_match(&labels(&[("job", "api-server")])));
    }
}
