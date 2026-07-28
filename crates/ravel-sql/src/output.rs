//! Query results and their two wire encodings.
//!
//! [`QueryOutput`] owns the collected `RecordBatch`es and hides every arrow
//! and DataFusion type behind its own surface. That is deliberate: the HTTP
//! endpoint lives in services/ravel-server, which must not link datafusion
//! (ADR-0013 / docs/arrow-datafusion-plan.md section 3 structural
//! isolation). The endpoint picks an encoding from the `Accept` header and
//! gets back bytes or a `serde_json::Value`; it never touches a batch.
//!
//! # JSON float encoding
//!
//! JSON has no NaN or infinity literal. Ravel's exactness posture makes
//! those values significant (NaN payloads and -0.0 are preserved
//! bit-exactly through the storage and dedup path), so they are encoded as
//! the strings `"NaN"`, `"+Inf"`, and `"-Inf"` -- the same spelling
//! Prometheus uses in its own JSON API, so a client that already speaks the
//! PromQL endpoint needs no new rule. Finite values are JSON numbers,
//! including `-0.0`, which `serde_json` renders with its sign preserved.
//!
//! Arrow IPC has no such problem and carries the exact bit patterns; it is
//! the encoding to use when bit-exactness matters to the caller.

use std::fmt::Write as _;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray, MapArray, StringArray,
    StringViewArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Int32Type, SchemaRef, TimeUnit};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::{Map as JsonMap, Value as Json, json};

use crate::error::SqlError;

/// The collected result of one SQL query.
#[derive(Debug, Clone)]
pub struct QueryOutput {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl QueryOutput {
    pub(crate) fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        QueryOutput { schema, batches }
    }

    /// Column names in output order.
    pub fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Total rows across all batches.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Number of batches the plan emitted. Exposed for the memory-accounting
    /// tests, which need to prove more than one batch went through the pool.
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// The collected batches, for in-crate tests and the differential gate.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Encode as an Arrow IPC stream (schema message followed by one record
    /// batch message per batch). Bit-exact for every float.
    pub fn to_arrow_ipc(&self) -> Result<Vec<u8>, SqlError> {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, self.schema.as_ref())
                .map_err(|e| SqlError::Internal(format!("arrow IPC writer: {e}")))?;
            for batch in &self.batches {
                writer
                    .write(batch)
                    .map_err(|e| SqlError::Internal(format!("arrow IPC write: {e}")))?;
            }
            writer
                .finish()
                .map_err(|e| SqlError::Internal(format!("arrow IPC finish: {e}")))?;
        }
        Ok(buf)
    }

    /// Encode as JSON: `{"columns": [...], "rows": [[...], ...]}`.
    ///
    /// Row-major rather than column-major because a SQL result is
    /// heterogeneous by column and clients consume it row-wise; the columnar
    /// answer for clients that want it is Arrow IPC.
    pub fn to_json(&self) -> Result<Json, SqlError> {
        let columns: Vec<Json> = self
            .schema
            .fields()
            .iter()
            .map(|f| json!({ "name": f.name(), "type": f.data_type().to_string() }))
            .collect();

        let mut rows: Vec<Json> = Vec::with_capacity(self.num_rows());
        for batch in &self.batches {
            for row in 0..batch.num_rows() {
                let mut cells = Vec::with_capacity(batch.num_columns());
                for col in batch.columns() {
                    cells.push(cell_to_json(col, row)?);
                }
                rows.push(Json::Array(cells));
            }
        }

        Ok(json!({ "columns": columns, "rows": rows }))
    }
}

/// Encode one cell. Unsupported types are an internal error rather than a
/// lossy stringification: silently rendering a type we do not understand
/// would be a quiet correctness hole in an endpoint whose whole contract is
/// exactness.
fn cell_to_json(array: &ArrayRef, row: usize) -> Result<Json, SqlError> {
    if array.is_null(row) {
        return Ok(Json::Null);
    }
    let value = match array.data_type() {
        DataType::Null => Json::Null,
        DataType::Boolean => Json::Bool(downcast::<BooleanArray>(array, "Boolean")?.value(row)),
        DataType::Int32 => json!(downcast::<Int32Array>(array, "Int32")?.value(row)),
        DataType::Int64 => json!(downcast::<Int64Array>(array, "Int64")?.value(row)),
        DataType::UInt32 => json!(downcast::<UInt32Array>(array, "UInt32")?.value(row)),
        DataType::UInt64 => json!(downcast::<UInt64Array>(array, "UInt64")?.value(row)),
        DataType::Float32 => float_to_json(f64::from(
            downcast::<Float32Array>(array, "Float32")?.value(row),
        )),
        DataType::Float64 => float_to_json(downcast::<Float64Array>(array, "Float64")?.value(row)),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            json!(downcast::<TimestampNanosecondArray>(array, "Timestamp(ns)")?.value(row))
        }
        DataType::Utf8 => json!(downcast::<StringArray>(array, "Utf8")?.value(row)),
        DataType::LargeUtf8 => json!(downcast::<LargeStringArray>(array, "LargeUtf8")?.value(row)),
        DataType::Utf8View => json!(downcast::<StringViewArray>(array, "Utf8View")?.value(row)),
        DataType::FixedSizeBinary(_) => json!(hex(downcast::<FixedSizeBinaryArray>(
            array,
            "FixedSizeBinary"
        )?
        .value(row))),
        DataType::Binary => json!(hex(downcast::<BinaryArray>(array, "Binary")?.value(row))),
        DataType::Dictionary(key, value) if **key == DataType::Int32 => {
            let dict = downcast::<DictionaryArray<Int32Type>>(array, "Dictionary(Int32, _)")?;
            let index = resolve_key(dict.keys().value(row), dict.values().len())?;
            if matches!(**value, DataType::Map(_, _)) {
                map_to_json(dict.values(), index)
            } else {
                cell_to_json(dict.values(), index)
            }?
        }
        DataType::Map(_, _) => map_to_json(array, row)?,
        other => {
            return Err(SqlError::Internal(format!(
                "no JSON encoding for arrow type {other}"
            )));
        }
    };
    Ok(value)
}

/// A `Map(Utf8, Utf8)` cell becomes a JSON object. Ravel's only map column
/// is `labels`, whose keys are non-null Utf8 by construction (crate::schema).
fn map_to_json(array: &ArrayRef, row: usize) -> Result<Json, SqlError> {
    let map = downcast::<MapArray>(array, "Map")?;
    if map.is_null(row) {
        return Ok(Json::Null);
    }
    let entries = map.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SqlError::Internal("map keys are not Utf8".to_string()))?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SqlError::Internal("map values are not Utf8".to_string()))?;

    let mut object = JsonMap::with_capacity(keys.len());
    for i in 0..keys.len() {
        let value = if values.is_null(i) {
            Json::Null
        } else {
            Json::String(values.value(i).to_string())
        };
        object.insert(keys.value(i).to_string(), value);
    }
    Ok(Json::Object(object))
}

/// NaN and the infinities have no JSON literal; encode them the way
/// Prometheus does so a client needs one rule, not two.
fn float_to_json(value: f64) -> Json {
    if value.is_nan() {
        Json::String("NaN".to_string())
    } else if value == f64::INFINITY {
        Json::String("+Inf".to_string())
    } else if value == f64::NEG_INFINITY {
        Json::String("-Inf".to_string())
    } else {
        // `serde_json::Number::from_f64` only returns None for non-finite
        // input, which the branches above already handled.
        json!(value)
    }
}

/// Resolve a dictionary key into a valid index into the `len` distinct
/// values. A negative key (via `usize::try_from`) or an index `>= len` is a
/// corrupt column, so it becomes a typed `SqlError::Internal` rather than an
/// out-of-bounds index panic on `dict.values()`. This mirrors the sibling
/// decoder in `labels.rs`, which rejects the identical condition on the same
/// `Dictionary(Int32, Map)` column, so both decoders fail loudly and the same
/// way. Callers handle a null cell before reaching here.
fn resolve_key(key: i32, len: usize) -> Result<usize, SqlError> {
    let k = usize::try_from(key)
        .map_err(|_| SqlError::Internal(format!("negative dictionary key {key}")))?;
    if k >= len {
        return Err(SqlError::Internal(format!(
            "dictionary key {k} out of range for {len} entries"
        )));
    }
    Ok(k)
}

fn downcast<'a, T: Array + 'static>(array: &'a ArrayRef, what: &str) -> Result<&'a T, SqlError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| SqlError::Internal(format!("expected {what} array")))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String is infallible.
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{Field, Schema};

    use super::*;

    fn float_output(values: Vec<f64>) -> QueryOutput {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(values)) as ArrayRef],
        )
        .expect("batch");
        QueryOutput::new(schema, vec![batch])
    }

    #[test]
    fn json_encodes_nan_and_infinities_as_prometheus_strings() {
        let out = float_output(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5]);
        let json = out.to_json().expect("json");
        let rows = json["rows"].as_array().expect("rows");
        assert_eq!(rows[0][0], Json::String("NaN".to_string()));
        assert_eq!(rows[1][0], Json::String("+Inf".to_string()));
        assert_eq!(rows[2][0], Json::String("-Inf".to_string()));
        assert_eq!(rows[3][0], json!(1.5));
    }

    #[test]
    fn json_preserves_the_sign_of_negative_zero() {
        let out = float_output(vec![-0.0, 0.0]);
        let json = out.to_json().expect("json");
        let text = serde_json::to_string(&json["rows"]).expect("serialize");
        assert!(
            text.contains("-0.0"),
            "-0.0 must keep its sign in JSON: {text}"
        );
    }

    /// Arrow IPC is the bit-exact encoding: a NaN payload must survive a
    /// round trip through it unchanged.
    #[test]
    fn arrow_ipc_round_trips_nan_payloads_bit_exactly() {
        use datafusion::arrow::ipc::reader::StreamReader;

        let payload = f64::from_bits(0x7ff8_0000_0000_00ab);
        let out = float_output(vec![payload, -0.0, f64::NEG_INFINITY]);
        let bytes = out.to_arrow_ipc().expect("ipc");

        let reader = StreamReader::try_new(bytes.as_slice(), None).expect("reader");
        let batches: Vec<RecordBatch> = reader.map(|b| b.expect("batch")).collect();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64 col");
        assert_eq!(col.value(0).to_bits(), payload.to_bits());
        assert_eq!(col.value(1).to_bits(), (-0.0f64).to_bits());
        assert_eq!(col.value(2).to_bits(), f64::NEG_INFINITY.to_bits());
    }

    #[test]
    fn out_of_range_dictionary_key_is_typed_error() {
        // Arrow's safe DictionaryArray constructors validate key bounds, so a
        // corrupt key is unconstructible through safe APIs and cannot be driven
        // end to end without `unsafe`, which is denied workspace-wide.
        // resolve_key is the guard cell_to_json calls before indexing
        // dict.values(); test it directly. A negative key and an out-of-range
        // positive key must each be a typed Internal error, never a panic; a
        // valid key resolves.
        assert!(matches!(resolve_key(-1, 4), Err(SqlError::Internal(_))));
        assert!(matches!(resolve_key(4, 4), Err(SqlError::Internal(_))));
        assert!(matches!(resolve_key(3, 4), Ok(3)));
    }

    #[test]
    fn fixed_size_binary_is_hex_encoded() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "series_id",
            DataType::FixedSizeBinary(4),
            false,
        )]));
        let array =
            FixedSizeBinaryArray::try_from_iter(vec![[0xde_u8, 0xad, 0xbe, 0xef]].into_iter())
                .expect("array");
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("batch");
        let json = QueryOutput::new(schema, vec![batch])
            .to_json()
            .expect("json");
        assert_eq!(json["rows"][0][0], json!("deadbeef"));
    }
}
