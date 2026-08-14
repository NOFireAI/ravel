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
//! the re-applied UDF are the identical predicate.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use datafusion::scalar::ScalarValue;
use ravel_logseg::tokenizer::tokens;

/// The name of the `has_word(text, 'word') -> Boolean` UDF.
pub const HAS_WORD_UDF: &str = "has_word";

/// Build the `has_word` scalar UDF: `has_word(text, word) -> Boolean`.
pub fn has_word_udf() -> ScalarUDF {
    create_udf(
        HAS_WORD_UDF,
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(has_word_impl),
    )
}

/// True iff `word` tokenizes to an in-order contiguous run of tokens present in
/// the tokenized `text`. Mirrors `RlogReader`'s own `phrase_match` exactly so
/// the pushed [`ravel_logseg::Predicate::HasWord`] and this UDF agree on every
/// row (docs/log-segment-format.md "Tokenizer"). An empty query (a `word` with
/// no tokens) matches every row, matching the reader.
fn phrase_match(text: &str, word: &str) -> bool {
    let query = tokens(word);
    if query.is_empty() {
        return true;
    }
    let toks = tokens(text);
    toks.windows(query.len()).any(|w| w == query.as_slice())
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
    let strings = text.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Execution("has_word() first argument must be a Utf8 column".into())
    })?;
    let out: BooleanArray = (0..strings.len())
        .map(|i| {
            if strings.is_null(i) {
                // A NULL cell contains no word; matches the reader treating an
                // absent/undecodable text field as "no match".
                Some(false)
            } else {
                Some(phrase_match(strings.value(i), &word))
            }
        })
        .collect();
    Ok(ColumnarValue::Array(Arc::new(out)))
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
}
