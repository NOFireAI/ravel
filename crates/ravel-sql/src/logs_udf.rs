//! The `has_word(text, 'literal') -> Boolean` scalar UDF for the `logs` table
//! (ADR-0033 "word/phrase text search via a `has_word(col, 'literal')` scalar
//! UDF").
//!
//! Registered into the per-query `SessionContext` the same way the metrics
//! `label`/`label_match` UDFs are (crate::udf); that registration lives in the
//! session builder (the session layer), not here. This module only supplies
//! the UDF and its name so [`crate::logs_pushdown`] can recognize the predicate
//! shape so the session layer can register it.
//!
//! # Why the UDF, not `LIKE`, is the pushdown-sound text predicate
//!
//! The UDF's SQL semantics are defined to be **exactly**
//! [`ravel_logseg::Predicate::HasWord`]'s: `has_word(col, w)` is true for a row
//! iff `w` tokenizes to a run of one or more tokens that occurs, in order and
//! contiguous, in the tokenized cell (the frozen tokenizer,
//! `ravel_logseg::tokenizer::tokens`, docs/log-segment-format.md "Tokenizer").
//! Because the SQL predicate and the pushed `HasWord` arm agree byte-for-byte
//! on which rows match, handing `HasWord` to `RlogReader::scan` (which applies
//! it as an exact per-row filter, not merely a bloom prune) removes exactly the
//! rows the residual `has_word` above the scan would remove, so no row the
//! query needs is ever dropped. This is the same agreement that makes the
//! metrics `label_match` regex pushdown sound (crate::udf): the pushed arm and
//! the re-applied UDF call the identical [`ravel_logseg::reader::phrase_match`],
//! not two implementations kept in sync by hand.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, DictionaryArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

/// The name of the `has_word(text, 'word') -> Boolean` UDF.
pub const HAS_WORD_UDF: &str = "has_word";

/// The `has_word` scalar UDF implementation.
///
/// The text argument is either a fixed `Utf8` column (`body`, `severity_text`)
/// or a declared `Str` column, which arrives `Dictionary(Int32, Utf8)`
/// (ADR-0099 decision 5). An exact `create_udf([Utf8, Utf8])` signature would
/// make DataFusion coerce a dictionary argument to `Utf8`, inserting
/// `CAST(col AS Utf8)` ahead of the call: the dictionary would be hydrated back
/// to one value per row before the UDF ran, the dictionary arm of
/// [`has_word_impl`] would be unreachable, and `has_word` over a declared column
/// would be *slower* than the plain `Utf8` column it replaced. A user-defined
/// signature that passes a `Dictionary(Int32, Utf8)` first argument through
/// unchanged (see [`Self::coerce_types`]) keeps the dictionary intact into the
/// evaluator, so it is matched once per distinct value; #479's LIKE pushdown
/// builds on exactly this shape. Every other first-argument type still coerces
/// to `Utf8`, so the UDF accepts exactly what the old exact signature did.
#[derive(Debug, PartialEq, Eq, Hash)]
struct HasWordUdf {
    signature: Signature,
}

impl HasWordUdf {
    fn new() -> Self {
        HasWordUdf {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HasWordUdf {
    fn name(&self) -> &str {
        HAS_WORD_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    /// Coerce the arguments to what the evaluator consumes while leaving a
    /// declared `Str` column's `Dictionary(Int32, Utf8)` first argument
    /// **unchanged**, so no coercing `CAST` hydrates the dictionary away (the
    /// whole point of the user-defined signature; see the type doc).
    ///
    /// Every other first-argument type is coerced to `Utf8`, exactly what the
    /// original `create_udf([Utf8, Utf8])` exact signature accepted: `Utf8`,
    /// `LargeUtf8`, `Utf8View` (DataFusion types SQL `VARCHAR` as `Utf8View`
    /// via `map_varchar_to_utf8view`), a `Dictionary` over any other key/value
    /// pair, a NULL literal, and any type DataFusion can cast to `Utf8` all plan
    /// and answer as they did before. The second argument coerces to `Utf8`, so
    /// a wrong type there is a typed planning error rather than an execution
    /// failure, consistent with the first. An input DataFusion cannot cast to
    /// `Utf8` fails with its standard typed planning error at cast insertion.
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "has_word() expects 2 arguments, got {}",
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
        has_word_impl(&args.args)
    }
}

/// Build the `has_word` scalar UDF: `has_word(text, word) -> Boolean`.
pub fn has_word_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(HasWordUdf::new())
}

/// Forwards to `ravel_logseg::reader::phrase_match`, the same function
/// `RlogReader::scan` applies as the pushed [`ravel_logseg::Predicate::HasWord`]
/// filter, so the two can no longer drift into disagreement (there is only one
/// implementation). `text` is always valid UTF-8 here (it comes from an Arrow
/// `StringArray`), so the byte conversion cannot itself change the result.
fn phrase_match(text: &str, word: &str) -> bool {
    ravel_logseg::reader::phrase_match(text.as_bytes(), word)
}

pub(crate) fn has_word_impl(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "has_word() expects 2 arguments, got {}",
            args.len()
        )));
    }
    let word = scalar_utf8(&args[1], "has_word() word")?;
    let text = match &args[0] {
        ColumnarValue::Array(a) => Arc::clone(a),
        ColumnarValue::Scalar(s) => s.to_array()?,
    };
    // Fixed Utf8 columns (`body`, `severity_text`) arrive as a `StringArray`.
    if let Some(strings) = text.as_any().downcast_ref::<StringArray>() {
        let out: BooleanArray = (0..strings.len())
            .map(|i| {
                if strings.is_null(i) {
                    // A NULL cell contains no word; matches the reader treating
                    // an absent/undecodable text field as "no match".
                    Some(false)
                } else {
                    Some(phrase_match(strings.value(i), &word))
                }
            })
            .collect();
        return Ok(ColumnarValue::Array(Arc::new(out)));
    }
    // Declared `Str` columns arrive dictionary-encoded (ADR-0099 decision 5).
    // Evaluate `phrase_match` once per distinct dictionary value, then gather
    // by key, so a column with few distinct values costs far less than one
    // match per row (the shape #479's LIKE pushdown builds on).
    if let Some(dict) = text.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "has_word() dictionary column must have Utf8 values".into(),
                )
            })?;
        let per_value: Vec<bool> = (0..values.len())
            .map(|k| !values.is_null(k) && phrase_match(values.value(k), &word))
            .collect();
        let keys = dict.keys();
        let mut out: Vec<Option<bool>> = Vec::with_capacity(dict.len());
        for i in 0..dict.len() {
            if dict.is_null(i) {
                out.push(Some(false));
                continue;
            }
            // A key that does not resolve to a distinct value is a corrupt
            // column, not a non-match: default `false` would serve a
            // silently-wrong answer on a read path (this crate's rule is exact
            // semantics by default). Mirror `output.rs`'s `resolve_key`, which
            // errors on the identical shape.
            let key = keys.value(i);
            let idx = usize::try_from(key).map_err(|_| {
                DataFusionError::Execution(format!("has_word() negative dictionary key {key}"))
            })?;
            let matched = *per_value.get(idx).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "has_word() dictionary key {idx} out of range for {} values",
                    per_value.len()
                ))
            })?;
            out.push(Some(matched));
        }
        return Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out))));
    }
    Err(DataFusionError::Execution(
        "has_word() first argument must be a Utf8 or Dictionary(Int32, Utf8) column".into(),
    ))
}

/// Extract a non-null Utf8 scalar argument, mirroring `crate::udf`'s helper.
fn scalar_utf8(arg: &ColumnarValue, what: &str) -> DFResult<String> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => Ok(s.clone()),
        _ => Err(DataFusionError::Execution(format!(
            "{what} must be a non-null Utf8 literal"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn single_word_is_token_containment_not_substring() {
        // Token match: "time" is not a token of "timeout", so has_word is false
        // even though "time" is a substring. This is exactly why LIKE '%time%'
        // cannot be pushed as HasWord (see the module doc).
        assert!(phrase_match("connection timeout", "timeout"));
        assert!(!phrase_match("connection timeout", "time"));
        // Case-insensitive, split on non-alphanumerics.
        assert!(phrase_match("GET /api/v1 TIMEOUT", "timeout"));
    }

    #[test]
    fn empty_word_matches_every_row() {
        // ravel_logseg::reader::phrase_match checks query.is_empty() before the
        // UTF-8 conversion, so this is the one input where the shim's control
        // flow order actually matters, not just its output.
        assert!(phrase_match("connection timeout", ""));
        assert!(phrase_match("", ""));
    }

    #[test]
    fn multi_word_requires_contiguous_in_order_run() {
        assert!(phrase_match(
            "a connection timeout here",
            "connection timeout"
        ));
        assert!(!phrase_match(
            "timeout of the connection",
            "connection timeout"
        ));
    }

    #[test]
    fn impl_evaluates_over_a_string_column() {
        let col = StringArray::from(vec![Some("connection timeout"), Some("ok"), None]);
        let args = vec![
            ColumnarValue::Array(Arc::new(col)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("timeout".into()))),
        ];
        let out = has_word_impl(&args).expect("eval");
        let arr = match out {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(_) => panic!("expected array"),
        };
        let b = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(b.value(0));
        assert!(!b.value(1));
        assert!(!b.value(2));
    }

    /// A declared `Str` column reaches `has_word` as a `Dictionary(Int32, Utf8)`
    /// (ADR-0099 decision 5), not a `StringArray`. Without the dictionary arm the
    /// downcast fails at runtime; this proves the arm evaluates the same rows the
    /// plain-column path would, including a NULL key and a repeated value.
    #[test]
    fn impl_evaluates_over_a_declared_str_dictionary_column() {
        use datafusion::arrow::array::{DictionaryArray, Int32Array};

        // Distinct values 0="connection timeout", 1="ok"; keys reuse them and a
        // NULL key stands in for an absent/undecodable cell.
        let values = StringArray::from(vec![Some("connection timeout"), Some("ok")]);
        let keys = Int32Array::from(vec![Some(0), Some(1), None, Some(0)]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).expect("dict");
        let args = vec![
            ColumnarValue::Array(Arc::new(dict)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("timeout".into()))),
        ];
        let out = has_word_impl(&args).expect("eval");
        let arr = match out {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(_) => panic!("expected array"),
        };
        let b = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(b.value(0), "row 0 = 'connection timeout' matches 'timeout'");
        assert!(!b.value(1), "row 1 = 'ok' does not match");
        assert!(!b.value(2), "row 2 = NULL key is no match");
        assert!(b.value(3), "row 3 reuses value 0 and matches");
    }
}
