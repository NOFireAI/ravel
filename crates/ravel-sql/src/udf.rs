//! The `label(labels, 'name') -> Utf8` scalar UDF (plan Table model).
//!
//! Label access in SQL goes through this UDF rather than map-subscript
//! syntax so the endpoint controls the name and (later) pushdown
//! recognition, insulating it from DataFusion's evolving map-function
//! surface. B1 registers and implements it; pushdown is a later ticket.

use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use datafusion::scalar::ScalarValue;

use crate::labels::lookup_label;
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
