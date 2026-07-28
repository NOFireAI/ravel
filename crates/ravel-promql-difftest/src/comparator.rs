//! Compares a Prometheus JSON response against a Ravel JSON response for
//! bit-exact equivalence (docs/promql-evaluator-plan.md section 5.4,
//! ADR-0021 decision 3).
//!
//! Rules: sample values compare by `f64::to_bits`, except any two NaN bit
//! patterns compare equal as a class (the stale marker and an ordinary NaN
//! both round-trip through the JSON `"NaN"` literal, so this also makes the
//! comparator agnostic to that internal bit pattern); `-0.0` is otherwise
//! bit-significant. Result vectors compare label-set-sorted unless the
//! corpus entry is marked order-sensitive. Errors compare by `errorType`
//! class string equality. Warnings compare by presence only.

use serde_json::Value as Json;

use crate::corpus::ComparisonMode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Match,
    Mismatch(String),
}

/// Parses a Prometheus/Ravel sample value string (`"NaN"`, `"+Inf"`,
/// `"-Inf"`, or a plain decimal) into its `f64`.
fn parse_sample_value(s: &str) -> Result<f64, String> {
    match s {
        "NaN" => Ok(f64::NAN),
        "+Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        other => other
            .parse::<f64>()
            .map_err(|e| format!("cannot parse sample value '{other}': {e}")),
    }
}

fn values_equal(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    a.to_bits() == b.to_bits()
}

fn sample_pair_equal(a: &Json, b: &Json) -> Result<bool, String> {
    let (a_ts, a_val) = sample_pair(a)?;
    let (b_ts, b_val) = sample_pair(b)?;
    if a_ts != b_ts {
        return Ok(false);
    }
    Ok(values_equal(
        parse_sample_value(&a_val)?,
        parse_sample_value(&b_val)?,
    ))
}

fn sample_pair(v: &Json) -> Result<(f64, String), String> {
    let arr = v.as_array().ok_or("sample is not a 2-element array")?;
    if arr.len() != 2 {
        return Err(format!(
            "sample array has {} elements, expected 2",
            arr.len()
        ));
    }
    let ts = arr[0].as_f64().ok_or("sample timestamp is not a number")?;
    let val = arr[1]
        .as_str()
        .ok_or("sample value is not a string")?
        .to_string();
    Ok((ts, val))
}

fn label_key(metric: &Json) -> String {
    let mut pairs: Vec<(String, String)> = metric
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn vector_result_equal(a: &Json, b: &Json) -> Result<bool, String> {
    if a["metric"] != b["metric"] {
        return Ok(false);
    }
    sample_pair_equal(&a["value"], &b["value"])
}

fn matrix_result_equal(a: &Json, b: &Json) -> Result<bool, String> {
    if a["metric"] != b["metric"] {
        return Ok(false);
    }
    let a_values = a["values"].as_array().ok_or("matrix values not an array")?;
    let b_values = b["values"].as_array().ok_or("matrix values not an array")?;
    if a_values.len() != b_values.len() {
        return Ok(false);
    }
    for (av, bv) in a_values.iter().zip(b_values.iter()) {
        if !sample_pair_equal(av, bv)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn sort_by_label_key(results: &[Json]) -> Vec<Json> {
    let mut sorted: Vec<Json> = results.to_vec();
    sorted.sort_by_key(|r| label_key(&r["metric"]));
    sorted
}

fn has_warnings(body: &Json) -> bool {
    body.get("warnings")
        .and_then(|w| w.as_array())
        .is_some_and(|arr| !arr.is_empty())
}

fn error_type(body: &Json) -> Option<&str> {
    body.get("errorType").and_then(|v| v.as_str())
}

/// Compares two full `/api/v1/query` or `/api/v1/query_range` JSON envelope
/// bodies (already parsed) per `mode`.
pub fn compare(mode: ComparisonMode, prometheus: &Json, ravel: &Json) -> Verdict {
    match mode {
        ComparisonMode::ExpectError => compare_error(prometheus, ravel),
        ComparisonMode::Unordered | ComparisonMode::Ordered => {
            compare_success(mode, prometheus, ravel)
        }
    }
}

fn compare_error(prometheus: &Json, ravel: &Json) -> Verdict {
    let prom_status = prometheus["status"].as_str().unwrap_or_default();
    let ravel_status = ravel["status"].as_str().unwrap_or_default();
    if prom_status != "error" || ravel_status != "error" {
        return Verdict::Mismatch(format!(
            "expected both sides to error: prometheus status={prom_status:?} ravel status={ravel_status:?}"
        ));
    }
    let prom_type = error_type(prometheus);
    let ravel_type = error_type(ravel);
    if prom_type != ravel_type {
        return Verdict::Mismatch(format!(
            "errorType mismatch: prometheus={prom_type:?} ravel={ravel_type:?}"
        ));
    }
    Verdict::Match
}

fn compare_success(mode: ComparisonMode, prometheus: &Json, ravel: &Json) -> Verdict {
    let prom_status = prometheus["status"].as_str().unwrap_or_default();
    let ravel_status = ravel["status"].as_str().unwrap_or_default();
    if prom_status != "success" || ravel_status != "success" {
        return Verdict::Mismatch(format!(
            "expected both sides to succeed: prometheus status={prom_status:?} ravel status={ravel_status:?} prometheus_error={:?} ravel_error={:?}",
            prometheus.get("error"),
            ravel.get("error"),
        ));
    }

    if has_warnings(prometheus) != has_warnings(ravel) {
        return Verdict::Mismatch(format!(
            "warning presence mismatch: prometheus={} ravel={}",
            has_warnings(prometheus),
            has_warnings(ravel)
        ));
    }

    let prom_type = prometheus["data"]["resultType"]
        .as_str()
        .unwrap_or_default();
    let ravel_type = ravel["data"]["resultType"].as_str().unwrap_or_default();
    if prom_type != ravel_type {
        return Verdict::Mismatch(format!(
            "resultType mismatch: prometheus={prom_type:?} ravel={ravel_type:?}"
        ));
    }

    match prom_type {
        "vector" => compare_vector(mode, prometheus, ravel),
        "matrix" => compare_matrix(mode, prometheus, ravel),
        "scalar" => compare_pair(&prometheus["data"]["result"], &ravel["data"]["result"]),
        "string" => {
            if prometheus["data"]["result"] == ravel["data"]["result"] {
                Verdict::Match
            } else {
                Verdict::Mismatch(format!(
                    "string result mismatch: prometheus={} ravel={}",
                    prometheus["data"]["result"], ravel["data"]["result"]
                ))
            }
        }
        other => Verdict::Mismatch(format!("unhandled resultType '{other}'")),
    }
}

fn compare_pair(prom: &Json, ravel: &Json) -> Verdict {
    match sample_pair_equal(prom, ravel) {
        Ok(true) => Verdict::Match,
        Ok(false) => Verdict::Mismatch(format!("scalar mismatch: prometheus={prom} ravel={ravel}")),
        Err(e) => Verdict::Mismatch(e),
    }
}

fn compare_vector(mode: ComparisonMode, prometheus: &Json, ravel: &Json) -> Verdict {
    let (prom_results, ravel_results) = match (
        prometheus["data"]["result"].as_array(),
        ravel["data"]["result"].as_array(),
    ) {
        (Some(a), Some(b)) => (a.clone(), b.clone()),
        _ => return Verdict::Mismatch("vector result is not an array".to_string()),
    };
    compare_series(mode, prom_results, ravel_results, vector_result_equal)
}

fn compare_matrix(mode: ComparisonMode, prometheus: &Json, ravel: &Json) -> Verdict {
    let (prom_results, ravel_results) = match (
        prometheus["data"]["result"].as_array(),
        ravel["data"]["result"].as_array(),
    ) {
        (Some(a), Some(b)) => (a.clone(), b.clone()),
        _ => return Verdict::Mismatch("matrix result is not an array".to_string()),
    };
    compare_series(mode, prom_results, ravel_results, matrix_result_equal)
}

fn compare_series(
    mode: ComparisonMode,
    prom_results: Vec<Json>,
    ravel_results: Vec<Json>,
    element_equal: impl Fn(&Json, &Json) -> Result<bool, String>,
) -> Verdict {
    if prom_results.len() != ravel_results.len() {
        return Verdict::Mismatch(format!(
            "result length mismatch: prometheus={} ravel={}",
            prom_results.len(),
            ravel_results.len()
        ));
    }
    let (prom_ordered, ravel_ordered) = match mode {
        ComparisonMode::Ordered => (prom_results, ravel_results),
        ComparisonMode::Unordered | ComparisonMode::ExpectError => (
            sort_by_label_key(&prom_results),
            sort_by_label_key(&ravel_results),
        ),
    };
    for (index, (p, r)) in prom_ordered.iter().zip(ravel_ordered.iter()).enumerate() {
        match element_equal(p, r) {
            Ok(true) => {}
            Ok(false) => {
                return Verdict::Mismatch(format!(
                    "result[{index}] differs: prometheus={p} ravel={r}"
                ));
            }
            Err(e) => return Verdict::Mismatch(format!("result[{index}]: {e}")),
        }
    }
    Verdict::Match
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn identical_vector_results_match() {
        let body = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {"__name__": "up"}, "value": [1.0, "1"]}]
            }
        });
        assert_eq!(
            compare(ComparisonMode::Unordered, &body, &body),
            Verdict::Match
        );
    }

    #[test]
    fn out_of_order_but_equal_sets_match_when_unordered() {
        let prom = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"job": "b"}, "value": [1.0, "2"]},
                    {"metric": {"job": "a"}, "value": [1.0, "1"]}
                ]
            }
        });
        let ravel = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"job": "a"}, "value": [1.0, "1"]},
                    {"metric": {"job": "b"}, "value": [1.0, "2"]}
                ]
            }
        });
        assert_eq!(
            compare(ComparisonMode::Unordered, &prom, &ravel),
            Verdict::Match
        );
    }

    #[test]
    fn out_of_order_mismatches_when_ordered() {
        let prom = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"job": "b"}, "value": [1.0, "2"]},
                    {"metric": {"job": "a"}, "value": [1.0, "1"]}
                ]
            }
        });
        let ravel = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"job": "a"}, "value": [1.0, "1"]},
                    {"metric": {"job": "b"}, "value": [1.0, "2"]}
                ]
            }
        });
        assert!(matches!(
            compare(ComparisonMode::Ordered, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn nan_compares_equal_as_a_class() {
        let prom = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "NaN"]}
        });
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "NaN"]}
        });
        assert_eq!(
            compare(ComparisonMode::Unordered, &prom, &ravel),
            Verdict::Match
        );
    }

    #[test]
    fn negative_zero_is_bit_significant() {
        let prom = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "-0"]}
        });
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "0"]}
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn error_class_match_ignores_message_text() {
        let prom =
            json!({"status": "error", "errorType": "bad_data", "error": "prometheus wording"});
        let ravel = json!({"status": "error", "errorType": "bad_data", "error": "ravel wording"});
        assert_eq!(
            compare(ComparisonMode::ExpectError, &prom, &ravel),
            Verdict::Match
        );
    }

    #[test]
    fn error_class_mismatch_is_caught() {
        let prom = json!({"status": "error", "errorType": "bad_data", "error": "x"});
        let ravel = json!({"status": "error", "errorType": "execution", "error": "y"});
        assert!(matches!(
            compare(ComparisonMode::ExpectError, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn one_side_succeeding_when_error_expected_is_a_mismatch() {
        let prom = json!({"status": "error", "errorType": "bad_data", "error": "x"});
        let ravel = json!({"status": "success", "data": {"resultType": "vector", "result": []}});
        assert!(matches!(
            compare(ComparisonMode::ExpectError, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn warning_presence_mismatch_is_caught() {
        let prom = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []},
            "warnings": ["clamped"]
        });
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []}
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }
}
