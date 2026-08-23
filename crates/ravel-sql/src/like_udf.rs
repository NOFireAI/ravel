//! The SQL `col LIKE 'pattern'` / `col NOT LIKE 'pattern'` predicate for the
//! `logs` table (#479), evaluated by a Ravel-owned scalar UDF instead of
//! DataFusion's built-in `LIKE`.
//!
//! # Why a UDF and a rewrite, not the built-in operator
//!
//! A declared `Str` column reaches the query as `Dictionary(Int32, Utf8)`
//! (ADR-0099 decision 5). DataFusion's built-in `LIKE` coercion casts a
//! dictionary operand back to one value per row before matching, so a `LIKE`
//! over a low-cardinality declared column pays a match per row rather than a
//! match per distinct value, and the dictionary is gone by the time any Ravel
//! code runs. This is the same wall the `has_word` UDF hit ([`crate::logs_udf`]):
//! an exact signature makes DataFusion hydrate the dictionary away.
//!
//! The fix is the same shape as `has_word`: a [`ScalarUDFImpl`] whose
//! [`LikeUdf::coerce_types`] leaves a `Dictionary(Int32, Utf8)` first argument
//! **unchanged** (so no coercing `CAST` hydrates it) while coercing every other
//! first-argument type, and the second argument, to `Utf8` exactly as the
//! built-in accepted. A [`FunctionRewrite`] ([`LikeToUdf`]) rewrites every
//! case-sensitive `Expr::Like` into a call to this UDF. It is registered as a
//! *function rewrite*, not an analyzer rule, so it runs BEFORE type coercion:
//! the rewrite sees the original operands (a dictionary still a dictionary), and
//! coercion then applies [`LikeUdf::coerce_types`] to the UDF call it produced.
//!
//! `ILIKE` (`case_insensitive`) is left to the built-in: Ravel's matcher folds
//! case with ASCII rules only, which would silently diverge from the built-in's
//! full-Unicode `ILIKE` on non-ASCII input, and ClickBench needs only
//! case-sensitive `LIKE`. Leaving `ILIKE` native keeps its existing (correct, if
//! dictionary-hydrating) behavior rather than replacing it with a subtly wrong
//! one.
//!
//! # Two evaluation paths
//!
//! - **Dictionary path** for a declared `Str` column's `Dictionary(Int32, Utf8)`
//!   first argument with a constant pattern: the pattern is matched once per
//!   DISTINCT dictionary value, then results are gathered by key. A dictionary
//!   may retain values no surviving row references; matching them is redundant
//!   but correct, so there is no compaction pass.
//! - **Row-wise path** for a plain `Utf8` first argument (`body` is a fixed
//!   `Utf8` column and stays plain), and for the rare non-constant pattern.
//!
//! Neither path is a fallback for the other; the array type of the first
//! argument selects the path.
//!
//! # Pushdown
//!
//! None. `LIKE` is a substring predicate and the RLOG reader offers only exact
//! `HasWord` (token/phrase) and `Equals` predicates ([`ravel_logseg::Predicate`]),
//! neither of which is a sound superset of SQL substring `LIKE`: `LIKE '%foo%'`
//! matches `"foobar"`, whose only token is `"foobar"`, so a `HasWord{word:"foo"}`
//! prune (even block-level, widen-only) would drop that block and lose the row.
//! `LIKE` is therefore evaluated exactly by DataFusion's mandatory `Inexact`
//! residual over the scanned rows and pushes nothing; [`crate::logs_pushdown`]
//! recognizes neither `Expr::Like` nor this UDF.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, DictionaryArray, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::Transformed;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::{
    ColumnarValue, Expr, Like, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

/// The name of the `like` scalar UDF the `LIKE` rewrite produces.
pub const LIKE_UDF: &str = "like";

/// The default `LIKE` escape character when a statement gives no `ESCAPE`
/// clause, matching DataFusion's built-in `LIKE` (and Postgres): a backslash
/// escapes the next character, so `\%` and `\_` are literal `%`/`_`.
const DEFAULT_ESCAPE: char = '\\';

/// The `col LIKE 'pattern' -> Boolean` (or `NOT LIKE`) scalar UDF.
///
/// One instance is built per `Expr::Like` by [`LikeToUdf`], carrying that
/// predicate's `negated` flag and `ESCAPE` character. The UDF is embedded in the
/// expression tree directly (never registered by name), so the per-predicate
/// configuration travels with the call and the session's scalar allowlist
/// (which gates the *registry*, [`crate::session`]) is unaffected.
#[derive(Debug, PartialEq, Eq, Hash)]
struct LikeUdf {
    signature: Signature,
    /// `true` for `NOT LIKE`: the per-row match is inverted (a NULL stays NULL).
    negated: bool,
    /// The escape character; `None` means [`DEFAULT_ESCAPE`].
    escape_char: Option<char>,
}

impl LikeUdf {
    fn new(negated: bool, escape_char: Option<char>) -> Self {
        LikeUdf {
            signature: Signature::user_defined(Volatility::Immutable),
            negated,
            escape_char,
        }
    }
}

impl ScalarUDFImpl for LikeUdf {
    fn name(&self) -> &str {
        LIKE_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    /// Leave a declared `Str` column's `Dictionary(Int32, Utf8)` first argument
    /// unchanged so no coercing `CAST` hydrates the dictionary away (the whole
    /// point; see the module doc). Every other first-argument type, and the
    /// pattern, coerce to `Utf8` -- exactly what DataFusion's built-in `LIKE`
    /// accepted, so `LargeUtf8`, `Utf8View` (SQL `VARCHAR`), a NULL literal, a
    /// dictionary over any other key/value pair, and any other castable type all
    /// still plan. Coercing the second argument (rather than leaving it verbatim
    /// and failing at execution) keeps a wrong pattern type a typed *planning*
    /// error.
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "like() expects 2 arguments, got {}",
                arg_types.len()
            )));
        }
        let first = match &arg_types[0] {
            DataType::Dictionary(k, v) if **k == DataType::Int32 && **v == DataType::Utf8 => {
                arg_types[0].clone()
            }
            _ => DataType::Utf8,
        };
        Ok(vec![first, DataType::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        like_impl(&args.args, self.negated, self.escape_char)
    }
}

/// A [`FunctionRewrite`] that turns every case-sensitive `Expr::Like` into a
/// call to [`LikeUdf`], so the Ravel evaluator (and its dictionary fast path)
/// runs instead of DataFusion's built-in `LIKE`. Registered per `logs` session
/// ([`crate::session::build_session`]); it runs before type coercion, so it sees
/// the original (un-hydrated) operands. `ILIKE` is left untouched (see the
/// module doc).
#[derive(Debug)]
pub(crate) struct LikeToUdf;

impl FunctionRewrite for LikeToUdf {
    fn name(&self) -> &str {
        "ravel_logs_like_to_udf"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> DFResult<Transformed<Expr>> {
        if let Expr::Like(Like {
            negated,
            expr: inner,
            pattern,
            escape_char,
            case_insensitive: false,
        }) = &expr
        {
            let udf = Arc::new(ScalarUDF::new_from_impl(LikeUdf::new(
                *negated,
                *escape_char,
            )));
            let call = Expr::ScalarFunction(ScalarFunction::new_udf(
                udf,
                vec![(**inner).clone(), (**pattern).clone()],
            ));
            return Ok(Transformed::yes(call));
        }
        Ok(Transformed::no(expr))
    }
}

/// Evaluate `text LIKE pattern` (or `NOT LIKE`) over `args`. After coercion the
/// text is `Utf8` (a `StringArray`) or `Dictionary(Int32, Utf8)`, and the
/// pattern is `Utf8` (scalar or, rarely, an array).
pub(crate) fn like_impl(
    args: &[ColumnarValue],
    negated: bool,
    escape_char: Option<char>,
) -> DFResult<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "like() expects 2 arguments, got {}",
            args.len()
        )));
    }
    let escape = escape_char.unwrap_or(DEFAULT_ESCAPE);
    let text: ArrayRef = match &args[0] {
        ColumnarValue::Array(a) => Arc::clone(a),
        ColumnarValue::Scalar(s) => s.to_array()?,
    };

    match &args[1] {
        // The common case: a constant pattern, compiled once. This is where the
        // dictionary fast path lives.
        ColumnarValue::Scalar(sv) => {
            let pattern = scalar_utf8_opt(sv, "like() pattern")?;
            let matcher = pattern.as_deref().map(|p| LikeMatcher::compile(p, escape));
            eval_constant_pattern(&text, matcher.as_ref(), negated)
        }
        // A per-row (non-constant) pattern: rare, and never a dictionary fast
        // path. Hydrate the text to plain `Utf8` and match row by row.
        ColumnarValue::Array(patterns) => {
            let patterns = patterns
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("like() pattern column must be Utf8".into())
                })?;
            eval_array_pattern(&text, patterns, escape, negated)
        }
    }
}

/// Apply `negated` to a per-row match, preserving SQL NULL (`NOT NULL` is NULL).
#[inline]
fn apply_negation(matched: Option<bool>, negated: bool) -> Option<bool> {
    matched.map(|m| m != negated)
}

/// Match a constant `matcher` (or a NULL pattern, `None`) against every cell of
/// `text`. `text` is either a `StringArray` (row-wise path) or a
/// `Dictionary(Int32, Utf8)` (dictionary path: match once per distinct value).
fn eval_constant_pattern(
    text: &ArrayRef,
    matcher: Option<&LikeMatcher>,
    negated: bool,
) -> DFResult<ColumnarValue> {
    // A NULL pattern makes every comparison NULL (`x LIKE NULL` is NULL), which
    // NOT LIKE does not flip.
    let Some(matcher) = matcher else {
        let out = BooleanArray::from(vec![None; text.len()]);
        return Ok(ColumnarValue::Array(Arc::new(out)));
    };

    if let Some(strings) = text.as_any().downcast_ref::<StringArray>() {
        let out: BooleanArray = (0..strings.len())
            .map(|i| {
                let m = if strings.is_null(i) {
                    None
                } else {
                    Some(matcher.matches(strings.value(i)))
                };
                apply_negation(m, negated)
            })
            .collect();
        return Ok(ColumnarValue::Array(Arc::new(out)));
    }

    if let Some(dict) = text.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("like() dictionary column must have Utf8 values".into())
            })?;
        // Match once per distinct value; a NULL value stays NULL.
        let per_value: Vec<Option<bool>> = (0..values.len())
            .map(|k| {
                if values.is_null(k) {
                    None
                } else {
                    Some(matcher.matches(values.value(k)))
                }
            })
            .collect();
        let keys = dict.keys();
        let mut out: Vec<Option<bool>> = Vec::with_capacity(dict.len());
        for i in 0..dict.len() {
            if dict.is_null(i) {
                out.push(None);
                continue;
            }
            // A key that resolves to no value is a corrupt column, not a
            // non-match; error rather than serve a wrong answer on a read path
            // (mirrors `has_word_impl` and `output.rs`'s `resolve_key`).
            let key = keys.value(i);
            let idx = usize::try_from(key).map_err(|_| {
                DataFusionError::Execution(format!("like() negative dictionary key {key}"))
            })?;
            let matched = *per_value.get(idx).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "like() dictionary key {idx} out of range for {} values",
                    per_value.len()
                ))
            })?;
            out.push(apply_negation(matched, negated));
        }
        return Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out))));
    }

    Err(DataFusionError::Execution(
        "like() first argument must be a Utf8 or Dictionary(Int32, Utf8) column".into(),
    ))
}

/// Match a per-row `patterns` array against `text`, compiling the matcher for
/// each row. The text is hydrated to plain `Utf8` first: a per-row pattern is
/// rare and never worth a dictionary fast path.
fn eval_array_pattern(
    text: &ArrayRef,
    patterns: &StringArray,
    escape: char,
    negated: bool,
) -> DFResult<ColumnarValue> {
    let hydrated: ArrayRef = if text.data_type() == &DataType::Utf8 {
        Arc::clone(text)
    } else {
        cast(text, &DataType::Utf8)?
    };
    let strings = hydrated
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("like() first argument must be castable to Utf8".into())
        })?;
    let rows = strings.len();
    let out: BooleanArray = (0..rows)
        .map(|i| {
            let m = if strings.is_null(i) || patterns.is_null(i) {
                None
            } else {
                let matcher = LikeMatcher::compile(patterns.value(i), escape);
                Some(matcher.matches(strings.value(i)))
            };
            apply_negation(m, negated)
        })
        .collect();
    Ok(ColumnarValue::Array(Arc::new(out)))
}

/// A non-null `Utf8`/`LargeUtf8`/`Utf8View` scalar as an owned string, or `None`
/// for the corresponding NULL scalar. After coercion the pattern is `Utf8`; the
/// other string types are accepted defensively.
fn scalar_utf8_opt(sv: &ScalarValue, what: &str) -> DFResult<Option<String>> {
    match sv {
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
            Ok(v.clone())
        }
        _ => Err(DataFusionError::Execution(format!(
            "{what} must be a Utf8 literal"
        ))),
    }
}

/// One element of a compiled `LIKE` pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Elem {
    /// `%`: matches any sequence of zero or more characters.
    Any,
    /// `_`: matches exactly one character.
    Single,
    /// A literal character (an escaped `%`/`_`/escape, or an ordinary char).
    Lit(char),
}

/// A `LIKE` pattern compiled into a sequence of [`Elem`]s, matched
/// character-wise (Unicode scalar values, not bytes) and case-sensitively.
#[derive(Debug)]
struct LikeMatcher {
    elems: Vec<Elem>,
}

impl LikeMatcher {
    /// Compile `pattern` with `escape` as the escape character. An escape
    /// followed by any character makes that character a literal; a trailing
    /// escape with nothing after it is itself literal.
    fn compile(pattern: &str, escape: char) -> Self {
        let mut elems = Vec::new();
        let mut chars = pattern.chars();
        while let Some(c) = chars.next() {
            if c == escape {
                match chars.next() {
                    Some(n) => elems.push(Elem::Lit(n)),
                    None => elems.push(Elem::Lit(escape)),
                }
            } else if c == '%' {
                elems.push(Elem::Any);
            } else if c == '_' {
                elems.push(Elem::Single);
            } else {
                elems.push(Elem::Lit(c));
            }
        }
        LikeMatcher { elems }
    }

    /// Whether `text` matches the whole pattern. Classic two-pointer wildcard
    /// match with backtracking to the last `%`, over character slices so
    /// multi-byte characters count as one for `_`.
    fn matches(&self, text: &str) -> bool {
        let text: Vec<char> = text.chars().collect();
        let elems = &self.elems;
        let (n, m) = (text.len(), elems.len());
        let mut i = 0usize; // index into text
        let mut j = 0usize; // index into elems
        let mut star_j: Option<usize> = None;
        let mut star_i = 0usize;

        while i < n {
            if j < m {
                match elems[j] {
                    Elem::Single => {
                        i += 1;
                        j += 1;
                        continue;
                    }
                    Elem::Lit(c) => {
                        if text[i] == c {
                            i += 1;
                            j += 1;
                            continue;
                        }
                    }
                    Elem::Any => {
                        star_j = Some(j);
                        star_i = i;
                        j += 1;
                        continue;
                    }
                }
            }
            // A mismatch (or the pattern ran out before the text did): stretch
            // the most recent `%` by one more character, if there was one.
            match star_j {
                Some(sj) => {
                    j = sj + 1;
                    star_i += 1;
                    i = star_i;
                }
                None => return false,
            }
        }
        // The text is consumed; the match succeeds iff every remaining pattern
        // element is a `%` (which can match the empty tail).
        while j < m && elems[j] == Elem::Any {
            j += 1;
        }
        j == m
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn matches(pattern: &str, text: &str) -> bool {
        LikeMatcher::compile(pattern, DEFAULT_ESCAPE).matches(text)
    }

    #[test]
    fn literal_and_substring_patterns() {
        assert!(matches("abc", "abc"));
        assert!(!matches("abc", "abcd"));
        assert!(matches("%abc%", "xxabcyy"));
        assert!(matches("%google%", "http://google.com/"));
        assert!(!matches("%google%", "http://example.com/"));
        // Bare `%` matches anything, including the empty string.
        assert!(matches("%", ""));
        assert!(matches("%", "anything"));
        // Empty pattern matches only the empty string.
        assert!(matches("", ""));
        assert!(!matches("", "x"));
    }

    #[test]
    fn like_is_case_sensitive() {
        // The load-bearing property for ClickBench Q23, which pairs
        // `Title LIKE '%Google%'` with `URL NOT LIKE '%.google.%'`.
        assert!(matches("%Google%", "The Google Homepage"));
        assert!(!matches("%Google%", "the google homepage"));
        assert!(matches("%.google.%", "www.google.com"));
        assert!(!matches("%.Google.%", "www.google.com"));
    }

    #[test]
    fn underscore_matches_exactly_one_character() {
        assert!(matches("a_c", "abc"));
        assert!(!matches("a_c", "ac"));
        assert!(!matches("a_c", "abbc"));
        // One multi-byte character is one `_`, not one per byte.
        assert!(matches("a_c", "aéc"));
    }

    #[test]
    fn escaped_wildcards_are_literal() {
        // `\%` is a literal percent, so this matches only a real `%`.
        assert!(matches("100\\%", "100%"));
        assert!(!matches("100\\%", "1000"));
        // `%\%foo\%%` looks for the literal substring `%foo%`.
        assert!(matches("%\\%foo\\%%", "bar %foo% baz"));
        assert!(!matches("%\\%foo\\%%", "bar foo baz"));
        // `\_` is a literal underscore.
        assert!(matches("a\\_c", "a_c"));
        assert!(!matches("a\\_c", "abc"));
    }

    #[test]
    fn adjacent_and_trailing_wildcards() {
        assert!(matches("%%abc%%", "zzabczz"));
        assert!(matches("abc%", "abcdef"));
        assert!(matches("%abc", "defabc"));
        assert!(matches("a%b%c", "axxbyyc"));
        assert!(!matches("a%b%c", "axxbyy"));
    }
}
