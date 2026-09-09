//! The `label(labels, 'name') -> Utf8` scalar UDF (plan Table model).
//!
//! Label access in SQL goes through this UDF rather than map-subscript
//! syntax so the endpoint controls the name and (later) pushdown
//! recognition, insulating it from DataFusion's evolving map-function
//! surface. This registers and implements it; pushdown is future work.

use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use datafusion::scalar::ScalarValue;
use ravel_promql::LabelMatcher;

use crate::labels::{eval_matcher, lookup_label};
use crate::schema::labels_type;

/// Build the `label` scalar UDF: `label(labels, name) -> Utf8`.
pub fn label_udf() -> ScalarUDF {
    create_udf(
        "label",
        vec![labels_type(), DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(label_impl),
    )
}

/// Build the `label_match` scalar UDF:
/// `label_match(labels, name, pattern) -> Boolean`.
///
/// The regex is anchored exactly as Prometheus anchors label matchers
/// (`^(?:pattern)$`), and an absent label reads as the empty string, so the
/// UDF and a `LabelMatcher::regex` prune agree byte-for-byte. That agreement is
/// what makes regex pushdown sound: the scan prunes to the matcher's series
/// superset and this UDF re-applies the identical predicate above the scan.
/// SQL's own unanchored `~` operator is deliberately *not* recognized for
/// pushdown, because an anchored matcher would select a subset and lose rows.
pub fn label_match_udf() -> ScalarUDF {
    create_udf(
        "label_match",
        vec![labels_type(), DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(label_match_impl),
    )
}

pub(crate) fn label_match_impl(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if args.len() != 3 {
        return Err(DataFusionError::Execution(format!(
            "label_match() expects 3 arguments, got {}",
            args.len()
        )));
    }
    let key = scalar_utf8(&args[1], "label_match() name")?;
    let pattern = scalar_utf8(&args[2], "label_match() pattern")?;
    let matcher = LabelMatcher::regex(&key, &pattern).map_err(|e| {
        DataFusionError::Execution(format!("label_match() invalid regex {pattern:?}: {e}"))
    })?;
    let labels = match &args[0] {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array()?,
    };
    let out: BooleanArray = eval_matcher(&labels, &matcher)?;
    Ok(ColumnarValue::Array(Arc::new(out)))
}

/// Extract a non-null Utf8 scalar argument.
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

pub(crate) fn label_impl(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "label() expects 2 arguments, got {}",
            args.len()
        )));
    }
    let key = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => s.clone(),
        _ => {
            return Err(DataFusionError::Execution(
                "label() second argument must be a non-null Utf8 literal".into(),
            ));
        }
    };
    let labels = match &args[0] {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array()?,
    };
    let out: StringArray = lookup_label(&labels, &key)?;
    Ok(ColumnarValue::Array(Arc::new(out)))
}
