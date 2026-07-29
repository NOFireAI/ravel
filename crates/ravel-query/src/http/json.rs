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
    Success {
        data: T,
        /// Prometheus' top-level `warnings` array: non-fatal diagnostics that
        /// very likely indicate a result the caller did not intend. Omitted
        /// when empty (Prometheus' own `omitempty`), which most responses
        /// are; the labels/series/label-values endpoints always pass empty.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<String>,
        /// Prometheus' top-level `infos` array: non-fatal diagnostics worth
        /// surfacing but usually benign. Kept separate from `warnings` (the
        /// two are distinct fields in Prometheus and in [`ravel_promql`]'s
        /// `Annotations`). Omitted when empty.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        infos: Vec<String>,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(rename = "errorType")]
        error_type: &'static str,
        error: String,
    },
}

impl<T> ApiResponse<T> {
    /// A success envelope with no annotations, for endpoints that never
    /// produce warnings or infos (labels, series, label-values).
    pub fn success(data: T) -> Self {
        ApiResponse::Success {
            data,
            warnings: Vec::new(),
            infos: Vec::new(),
        }
    }

    /// A success envelope carrying the query's evaluation annotations.
    pub fn success_with_annotations(data: T, warnings: Vec<String>, infos: Vec<String>) -> Self {
        ApiResponse::Success {
            data,
            warnings,
            infos,
        }
    }
}

/// Segment counters for the query that produced a response
/// (docs/metric-index-plan.md P5b), rendered under the Prometheus response
/// envelope's `data` object alongside `resultType`/`result`. Prometheus'
/// own API has no standardized shape for this (ravel-query previously
/// carried no query-level stats at all; see this ticket's final report),
/// so the field names here are ravel's own.
#[derive(Debug, Serialize)]
pub struct QueryStatsJson {
    #[serde(rename = "segmentsFetched")]
    pub segments_fetched: u64,
    #[serde(rename = "segmentsPruned")]
    pub segments_pruned: u64,
}

impl From<crate::QueryStats> for QueryStatsJson {
    fn from(stats: crate::QueryStats) -> Self {
        QueryStatsJson {
            segments_fetched: stats.segments_fetched,
            segments_pruned: stats.segments_pruned,
        }
    }
}

/// `/api/v1/query` and `/api/v1/query_range` response `data` object: the
/// Prometheus-shaped result, flattened alongside this query's segment
/// stats.
#[derive(Debug, Serialize)]
pub struct QueryResponseData {
    #[serde(flatten)]
    pub result: QueryData,
    pub stats: QueryStatsJson,
}

pub fn with_stats(result: QueryData, stats: crate::QueryStats) -> QueryResponseData {
    QueryResponseData {
        result,
        stats: stats.into(),
    }
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
/// `/api/v1/query` payload. A top-level range vector is a valid instant
/// result: Prometheus renders `resultType: matrix` for an instant query
/// whose top-level expression is itself a range vector (e.g. a bare
/// subquery, or `max_over_time(rate(...)[5m:1m])[10m:2m]`), and Ravel now
/// matches that. Unlike the scalar/string case in `range_value_to_json`,
/// which repeats a constant across the query-range grid, a matrix from an
/// instant query already carries its own per-series timestamps from the
/// evaluator, so no grid bounds are needed to render it.
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
    fn instant_matrix_renders_as_matrix_with_its_own_timestamps() {
        // An instant query whose top-level expression is itself a range
        // vector (e.g. a bare subquery, or a range function nested inside an
        // outer subquery) evaluates to a Value::Matrix. Prometheus renders
        // that as resultType: matrix for /api/v1/query, using the per-series
        // timestamps the evaluator already produced, not the single query
        // time. Ravel matches that here.
        use ravel_types::{Label, LabelSet, Sample};

        let labels = LabelSet::new(vec![Label {
            name: "job".to_string(),
            value: "api".to_string(),
        }])
        .expect("valid label set");
        let matrix = vec![(
            labels,
            vec![
                Sample {
                    ts_ns: 1_700_000_000_000_000_000,
                    value: 1.0,
                },
                Sample {
                    ts_ns: 1_700_000_120_000_000_000,
                    value: 2.5,
                },
            ],
        )];

        // time_ms is deliberately unrelated to the sample timestamps: the
        // matrix arm must ignore it and use each sample's own ts_ns.
        let data = instant_value_to_json(Value::Matrix(matrix), 9_999_999_999_000)
            .expect("matrix renders");
        let result = matrix_result(data);
        assert_eq!(result.len(), 1, "one series");
        assert_eq!(result[0].metric.get("job").map(String::as_str), Some("api"));
        assert_eq!(
            result[0].values,
            vec![
                (Timestamp(1_700_000_000.0), "1".to_string()),
                (Timestamp(1_700_000_120.0), "2.5".to_string()),
            ],
        );
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
    fn success_envelope_carries_warnings_and_infos() {
        // Both annotation channels render as top-level arrays, matching
        // Prometheus' `warnings`/`infos` fields (issue #178).
        let resp = ApiResponse::success_with_annotations(
            "data-goes-here",
            vec!["quantile value should be between 0 and 1, got 1.5".to_string()],
            vec!["needed to be fixed for monotonicity".to_string()],
        );
        let json = serde_json::to_value(&resp).expect("serializes");
        assert_eq!(json["status"], "success");
        assert_eq!(
            json["warnings"],
            serde_json::json!(["quantile value should be between 0 and 1, got 1.5"])
        );
        assert_eq!(
            json["infos"],
            serde_json::json!(["needed to be fixed for monotonicity"])
        );
    }

    #[test]
    fn success_envelope_omits_empty_annotation_arrays() {
        // Prometheus' `omitempty`: a query with no annotations carries
        // neither field, so the comparator's presence check reads them as
        // absent rather than present-but-empty.
        let resp = ApiResponse::success("data-goes-here");
        let json = serde_json::to_value(&resp).expect("serializes");
        assert!(json.get("warnings").is_none(), "empty warnings omitted");
        assert!(json.get("infos").is_none(), "empty infos omitted");
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
