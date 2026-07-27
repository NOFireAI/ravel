//! Prometheus-compatible JSON response envelopes.

use std::collections::HashMap;

use ravel_promql::{InstantSample, RangeMatrix};
use ravel_types::{LabelSet, SeriesId};
use serde::Serialize;

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

#[derive(Serialize)]
#[serde(tag = "resultType", content = "result")]
pub enum QueryData {
    #[serde(rename = "vector")]
    Vector(Vec<VectorResult>),
    #[serde(rename = "matrix")]
    Matrix(Vec<MatrixResult>),
}

#[derive(Serialize)]
pub struct VectorResult {
    pub metric: HashMap<String, String>,
    pub value: (f64, String),
}

#[derive(Serialize)]
pub struct MatrixResult {
    pub metric: HashMap<String, String>,
    pub values: Vec<(f64, String)>,
}

/// Prometheus renders a sample value as a JSON string, not a number, so
/// full f64 precision survives round-tripping through any JSON library.
pub fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() }
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
fn ts_ns_to_seconds(ts_ns: i64) -> f64 {
    (ts_ns as f64) / 1_000_000_000.0
}

pub fn instant_vector_to_json(vector: Vec<InstantSample>) -> QueryData {
    QueryData::Vector(
        vector
            .into_iter()
            .map(|s| VectorResult {
                metric: labels_to_map(&s.labels),
                value: (ts_ns_to_seconds(s.ts_ns), format_value(s.value)),
            })
            .collect(),
    )
}

pub fn range_matrix_to_json(matrix: RangeMatrix) -> QueryData {
    QueryData::Matrix(
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
    )
}

pub fn series_to_json(series: Vec<(SeriesId, LabelSet)>) -> Vec<HashMap<String, String>> {
    series.into_iter().map(|(_, labels)| labels_to_map(&labels)).collect()
}
