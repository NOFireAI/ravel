//! Prometheus-compatible JSON response envelopes.

use std::collections::HashMap;

use ravel_promql::{Value, ms_to_ns};
use ravel_types::{LabelSet, SeriesId};
use serde::{Serialize, Serializer};

use crate::http::error::ApiError;

#[derive(Serialize)]
#[serde(tag = "status")]
pub enum ApiResponse<T> {
    #[serde(rename = "success")]
    Success { data: T },
    #[serde(rename = "error")]
    Error {
        #[serde(rename = "errorType")]
        error_type: &'static str,
        error: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "resultType", content = "result")]
pub enum QueryData {
    #[serde(rename = "vector")]
    Vector(Vec<VectorResult>),
    #[serde(rename = "matrix")]
    Matrix(Vec<MatrixResult>),
    #[serde(rename = "scalar")]
    Scalar((Timestamp, String)),
    #[serde(rename = "string")]
    String((Timestamp, String)),
}

#[derive(Debug, Serialize)]
pub struct VectorResult {
    pub metric: HashMap<String, String>,
    pub value: (Timestamp, String),
}

#[derive(Debug, Serialize)]
pub struct MatrixResult {
    pub metric: HashMap<String, String>,
    pub values: Vec<(Timestamp, String)>,
}

/// A query result timestamp, rendered in JSON as Prometheus' Go encoder
/// renders it: a whole-second value is a bare integer (`1700000150`), not
/// serde_json's default `f64` encoding, which always keeps a fractional
/// part (`1700000150.0`) to disambiguate an `f64` from a JSON integer on
/// deserialize. Matching Prometheus' actual wire text, not just its
/// numeric value, is the bit-exact HTTP-API parity ADR-0021 requires.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timestamp(pub f64);

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_finite() && self.0.fract() == 0.0 && self.0.abs() < 2f64.powi(53) {
            serializer.serialize_i64(self.0 as i64)
        } else {
            serializer.serialize_f64(self.0)
        }
    }
}

/// Prometheus renders a sample value as a JSON string, not a number, so
/// full f64 precision survives round-tripping through any JSON library.
pub fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        format!("{v}")
    }
}

fn labels_to_map(labels: &LabelSet) -> HashMap<String, String> {
    labels
        .iter()
        .map(|l| (l.name.clone(), l.value.clone()))
        .collect()
}

/// Reported sample timestamps from the evaluator are always exact
/// multiples of 1ms in nanoseconds (docs/query-engine.md, `ravel_promql`
/// eval module doc), so this floor-division never loses precision.
fn ts_ns_to_seconds(ts_ns: i64) -> Timestamp {
    Timestamp((ts_ns as f64) / 1_000_000_000.0)
}

/// Render an [`Evaluator::eval_instant`] result as a Prometheus-shaped
/// `/api/v1/query` payload. A bare top-level range vector is rejected here
/// rather than in `ravel-promql`: `eval_instant`'s own doc contract says a
/// top-level matrix selector is a type error, but the evaluator itself does
/// not enforce that (it always returns whatever the AST resolves to), so
/// the HTTP layer is what actually applies Prometheus' instant-query type
/// check (a real, reachable case, not a dead branch: flagged separately as
/// a `ravel-promql` doc/code mismatch, out of scope for this ticket).
pub fn instant_value_to_json(value: Value, time_ms: i64) -> Result<QueryData, ApiError> {
    match value {
        Value::Vector(vector) => Ok(QueryData::Vector(
            vector
                .into_iter()
                .map(|s| VectorResult {
                    metric: labels_to_map(&s.labels),
                    value: (ts_ns_to_seconds(s.ts_ns), format_value(s.value)),
                })
                .collect(),
        )),
        Value::Scalar(v) => Ok(QueryData::Scalar((
            ms_to_seconds(time_ms)?,
            format_value(v),
        ))),
        Value::String(s) => Ok(QueryData::String((ms_to_seconds(time_ms)?, s))),
        Value::Matrix(_) => Err(ApiError::BadData(
            ravel_promql::Error::WrongType {
                expected: "scalar, string, or instant vector",
                got: "range vector",
            }
            .to_string(),
        )),
    }
}

/// Render an [`Evaluator::eval_range`] result as a Prometheus-shaped
/// `/api/v1/query_range` payload. A scalar or string top-level result is
/// constant across the whole grid (`eval_range`'s own doc comment), so
/// Prometheus renders it as a `matrix` with one synthetic empty-labeled
/// series repeating that value at every evaluated step; this materializes
/// that repetition, which `eval_range` deliberately defers to the wire
/// format layer.
pub fn range_value_to_json(
    value: Value,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<QueryData, ApiError> {
    match value {
        Value::Matrix(matrix) => Ok(QueryData::Matrix(
            matrix
                .into_iter()
                .map(|(labels, samples)| MatrixResult {
                    metric: labels_to_map(&labels),
                    values: samples
                        .into_iter()
                        .map(|s| (ts_ns_to_seconds(s.ts_ns), format_value(s.value)))
                        .collect(),
                })
                .collect(),
        )),
        Value::Scalar(v) => Ok(QueryData::Matrix(vec![repeated_matrix_result(
            start_ms,
            end_ms,
            step_ms,
            format_value(v),
        )?])),
        Value::String(s) => Ok(QueryData::Matrix(vec![repeated_matrix_result(
            start_ms, end_ms, step_ms, s,
        )?])),
        // eval_range's own contract (crates/ravel-promql/src/eval.rs) never
        // produces an instant vector from a range query; handled explicitly
        // rather than left to panic if that contract ever changes.
        Value::Vector(_) => Err(ApiError::BadData(
            ravel_promql::Error::WrongType {
                expected: "scalar, string, or range vector",
                got: "instant vector",
            }
            .to_string(),
        )),
    }
}

fn ms_to_seconds(ms: i64) -> Result<Timestamp, ApiError> {
    Ok(ts_ns_to_seconds(
        ms_to_ns(ms).map_err(|e| ApiError::BadData(e.to_string()))?,
    ))
}

/// Builds the repeated-value series for a scalar/string range result, over
/// the exact same evaluation grid `eval_range` used internally: `start`,
/// `start + step`, ..., stopping at the last instant `<= end`.
fn repeated_matrix_result(
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    value: String,
) -> Result<MatrixResult, ApiError> {
    let start_ns = ms_to_ns(start_ms).map_err(|e| ApiError::BadData(e.to_string()))?;
    let end_ns = ms_to_ns(end_ms).map_err(|e| ApiError::BadData(e.to_string()))?;
    let step_ns = ms_to_ns(step_ms).map_err(|e| ApiError::BadData(e.to_string()))?;

    let mut values = Vec::new();
    let mut t = start_ns;
    while t <= end_ns {
        values.push((ts_ns_to_seconds(t), value.clone()));
        t += step_ns;
    }
    Ok(MatrixResult {
        metric: HashMap::new(),
        values,
    })
}

pub fn series_to_json(series: Vec<(SeriesId, LabelSet)>) -> Vec<HashMap<String, String>> {
    series
        .into_iter()
        .map(|(_, labels)| labels_to_map(&labels))
        .collect()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn matrix_result(data: QueryData) -> Vec<MatrixResult> {
        match data {
            QueryData::Matrix(m) => m,
            _ => panic!("expected matrix result, got a differently-shaped QueryData"),
        }
    }

    #[test]
    fn instant_scalar_renders_as_scalar_tuple_at_the_query_time() {
        let data =
            instant_value_to_json(Value::Scalar(42.5), 1_700_000_000_000).expect("scalar renders");
        match data {
            QueryData::Scalar((ts, value)) => {
                assert_eq!(ts, Timestamp(1_700_000_000.0));
                assert_eq!(value, "42.5");
            }
            _ => panic!("expected scalar result, got a differently-shaped QueryData"),
        }
    }

    #[test]
    fn instant_string_renders_as_string_tuple_at_the_query_time() {
        let data = instant_value_to_json(Value::String("up".to_string()), 1_700_000_000_000)
            .expect("string renders");
        match data {
            QueryData::String((ts, value)) => {
                assert_eq!(ts, Timestamp(1_700_000_000.0));
                assert_eq!(value, "up");
            }
            _ => panic!("expected string result, got a differently-shaped QueryData"),
        }
    }

    #[test]
    fn instant_matrix_is_rejected_as_wrong_type() {
        // The evaluator's own contract says a bare top-level matrix selector
        // never survives to this layer, but the HTTP layer enforces it
        // itself (a real, reachable defensive check; see the ravel-promql
        // doc/code mismatch flagged in this ticket's final report), so a
        // Value::Matrix reaching here must still be rejected, not panic or
        // silently render.
        let err = instant_value_to_json(Value::Matrix(vec![]), 1_700_000_000_000)
            .expect_err("matrix must be rejected at instant type-check");
        match err {
            ApiError::BadData(msg) => assert!(msg.contains("range vector")),
            other => panic!("expected BadData, got a different ApiError: {other:?}"),
        }
    }

    #[test]
    fn range_scalar_repeats_the_value_across_the_whole_grid() {
        let data = range_value_to_json(
            Value::Scalar(3.0),
            1_700_000_000_000,
            1_700_000_002_000,
            1_000,
        )
        .expect("scalar range renders");
        let matrix = matrix_result(data);
        assert_eq!(matrix.len(), 1, "one synthetic series");
        assert!(
            matrix[0].metric.is_empty(),
            "synthetic series has no labels"
        );
        let values: Vec<&str> = matrix[0].values.iter().map(|(_, v)| v.as_str()).collect();
        assert_eq!(values, vec!["3", "3", "3"], "one repeat per grid step");
    }

    #[test]
    fn range_string_repeats_the_value_across_the_whole_grid() {
        let data = range_value_to_json(
            Value::String("idle".to_string()),
            1_700_000_000_000,
            1_700_000_001_000,
            1_000,
        )
        .expect("string range renders");
        let matrix = matrix_result(data);
        assert_eq!(matrix.len(), 1);
        let values: Vec<&str> = matrix[0].values.iter().map(|(_, v)| v.as_str()).collect();
        assert_eq!(values, vec!["idle", "idle"]);
    }

    #[test]
    fn whole_second_timestamp_renders_without_a_fractional_part() {
        // Prometheus' Go encoder emits a whole-second timestamp as a bare
        // JSON integer; serde_json's default f64 encoding would emit
        // `1700000150.0` instead, which is a real value this differential
        // test corpus caught against a pinned Prometheus binary.
        let json = serde_json::to_string(&Timestamp(1_700_000_150.0)).expect("serializes");
        assert_eq!(json, "1700000150");
    }

    #[test]
    fn fractional_timestamp_still_renders_its_fractional_part() {
        let json = serde_json::to_string(&Timestamp(1_700_000_150.5)).expect("serializes");
        assert_eq!(json, "1700000150.5");
    }

    #[test]
    fn range_vector_is_rejected_as_wrong_type() {
        // eval_range's contract never produces a bare instant vector, but
        // this defensive arm must still reject it rather than panic.
        let err = range_value_to_json(Value::Vector(vec![]), 0, 1_000, 1_000)
            .expect_err("vector must be rejected at range type-check");
        match err {
            ApiError::BadData(msg) => assert!(msg.contains("instant vector")),
            other => panic!("expected BadData, got a different ApiError: {other:?}"),
        }
    }
}
