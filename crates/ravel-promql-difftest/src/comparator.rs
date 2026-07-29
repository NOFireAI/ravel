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
//!
//! A corpus entry may opt into a bounded ULP tolerance (ADR-0025's
//! allowlist: a named, measured, per-entry exception, not a global fuzzy
//! comparison). `-0.0` vs `0.0` never matches under any tolerance: this
//! project's `-0.0` bit-significance rule is a matter of principle, not
//! magnitude, so the zero boundary is always exact-bit-compared.
//!
//! A corpus entry may also opt into the one-sided
//! [`ComparisonMode::RavelErrorPromSuccess`] mode (ADR-0030's allowlist):
//! an accepted, per-entry, by-design divergence where Ravel rejects a query
//! and Prometheus accepts it. Like the ULP tolerance, it is a named
//! exception with a written justification in the corpus file, never a
//! blanket "any disagreement is fine": the comparator still requires Ravel
//! to error and Prometheus to succeed, so a spurious both-error or
//! both-success result is caught.

use serde_json::Value as Json;

use crate::corpus::ComparisonMode;

/// Maps an f64's bit pattern to a `u64` whose ordering matches the float's
/// numeric ordering (for any non-NaN value): the standard "flip the sign
/// bit for non-negative, flip every bit for negative" transform. Adjacent
/// floats map to adjacent integers, so the difference between two keys is
/// exactly their ULP distance.
fn ordered_key(x: f64) -> u64 {
    let bits = x.to_bits();
    if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits | (1u64 << 63)
    }
}

/// True if `a` and `b` are within `max_ulps` representable f64 steps of
/// each other. Deliberately exact-bit only (never "close") across the zero
/// boundary, across a sign change, or when either side is non-finite: a
/// tolerance is for two answers that are the same real number to within a
/// few representable steps, not a general closeness measure.
fn within_ulps(a: f64, b: f64, max_ulps: u32) -> bool {
    if a.to_bits() == b.to_bits() {
        return true;
    }
    if a == 0.0 || b == 0.0 || !a.is_finite() || !b.is_finite() {
        return false;
    }
    if a.is_sign_negative() != b.is_sign_negative() {
        return false;
    }
    ordered_key(a).abs_diff(ordered_key(b)) <= u64::from(max_ulps)
}

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

fn values_equal(a: f64, b: f64, tolerance_ulps: Option<u32>) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    match tolerance_ulps {
        Some(max_ulps) => within_ulps(a, b, max_ulps),
        None => a.to_bits() == b.to_bits(),
    }
}

fn sample_pair_equal(a: &Json, b: &Json, tolerance_ulps: Option<u32>) -> Result<bool, String> {
    let (a_ts, a_val) = sample_pair(a)?;
    let (b_ts, b_val) = sample_pair(b)?;
    if a_ts != b_ts {
        return Ok(false);
    }
    Ok(values_equal(
        parse_sample_value(&a_val)?,
        parse_sample_value(&b_val)?,
        tolerance_ulps,
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

fn vector_result_equal(a: &Json, b: &Json, tolerance_ulps: Option<u32>) -> Result<bool, String> {
    if a["metric"] != b["metric"] {
        return Ok(false);
    }
    sample_pair_equal(&a["value"], &b["value"], tolerance_ulps)
}

fn matrix_result_equal(a: &Json, b: &Json, tolerance_ulps: Option<u32>) -> Result<bool, String> {
    if a["metric"] != b["metric"] {
        return Ok(false);
    }
    let a_values = a["values"].as_array().ok_or("matrix values not an array")?;
    let b_values = b["values"].as_array().ok_or("matrix values not an array")?;
    if a_values.len() != b_values.len() {
        return Ok(false);
    }
    for (av, bv) in a_values.iter().zip(b_values.iter()) {
        if !sample_pair_equal(av, bv, tolerance_ulps)? {
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
/// bodies (already parsed) per `mode`. `tolerance_ulps` is the corpus
/// entry's own ADR-0025 allowlist tolerance, if any; `None` means the
/// default bit-exact comparison.
pub fn compare(
    mode: ComparisonMode,
    tolerance_ulps: Option<u32>,
    prometheus: &Json,
    ravel: &Json,
) -> Verdict {
    match mode {
        ComparisonMode::ExpectError => compare_error(prometheus, ravel),
        ComparisonMode::RavelErrorPromSuccess => {
            compare_ravel_error_prom_success(prometheus, ravel)
        }
        ComparisonMode::Unordered | ComparisonMode::Ordered => {
            compare_success(mode, tolerance_ulps, prometheus, ravel)
        }
    }
}

/// ADR-0030 one-sided divergence: the entry documents a query that Ravel
/// rejects by design (a per-subquery-node point-cap budget with no
/// Prometheus counterpart) while Prometheus accepts it. The mismatch is not
/// a PromQL semantic difference to fix, so there is nothing for the two
/// engines to agree on; the comparator only asserts the divergence has the
/// exact shape ADR-0030 accepts. Anything else (both erroring, both
/// succeeding, or Prometheus erroring) is still a real mismatch: it would
/// mean the divergence has changed and the allowlist entry is now wrong.
fn compare_ravel_error_prom_success(prometheus: &Json, ravel: &Json) -> Verdict {
    let prom_status = prometheus["status"].as_str().unwrap_or_default();
    let ravel_status = ravel["status"].as_str().unwrap_or_default();
    if prom_status != "success" {
        return Verdict::Mismatch(format!(
            "expected prometheus to succeed (ADR-0030 one-sided divergence): status={prom_status:?} error={:?}",
            prometheus.get("error")
        ));
    }
    if ravel_status != "error" {
        return Verdict::Mismatch(format!(
            "expected ravel to error by design (ADR-0030 one-sided divergence): status={ravel_status:?}"
        ));
    }
    Verdict::Match
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

fn compare_success(
    mode: ComparisonMode,
    tolerance_ulps: Option<u32>,
    prometheus: &Json,
    ravel: &Json,
) -> Verdict {
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
        "vector" => compare_vector(mode, tolerance_ulps, prometheus, ravel),
        "matrix" => compare_matrix(mode, tolerance_ulps, prometheus, ravel),
        "scalar" => compare_pair(
            &prometheus["data"]["result"],
            &ravel["data"]["result"],
            tolerance_ulps,
        ),
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

fn compare_pair(prom: &Json, ravel: &Json, tolerance_ulps: Option<u32>) -> Verdict {
    match sample_pair_equal(prom, ravel, tolerance_ulps) {
        Ok(true) => Verdict::Match,
        Ok(false) => Verdict::Mismatch(format!("scalar mismatch: prometheus={prom} ravel={ravel}")),
        Err(e) => Verdict::Mismatch(e),
    }
}

fn compare_vector(
    mode: ComparisonMode,
    tolerance_ulps: Option<u32>,
    prometheus: &Json,
    ravel: &Json,
) -> Verdict {
    let (prom_results, ravel_results) = match (
        prometheus["data"]["result"].as_array(),
        ravel["data"]["result"].as_array(),
    ) {
        (Some(a), Some(b)) => (a.clone(), b.clone()),
        _ => return Verdict::Mismatch("vector result is not an array".to_string()),
    };
    compare_series(mode, prom_results, ravel_results, |a, b| {
        vector_result_equal(a, b, tolerance_ulps)
    })
}

fn compare_matrix(
    mode: ComparisonMode,
    tolerance_ulps: Option<u32>,
    prometheus: &Json,
    ravel: &Json,
) -> Verdict {
    let (prom_results, ravel_results) = match (
        prometheus["data"]["result"].as_array(),
        ravel["data"]["result"].as_array(),
    ) {
        (Some(a), Some(b)) => (a.clone(), b.clone()),
        _ => return Verdict::Mismatch("matrix result is not an array".to_string()),
    };
    compare_series(mode, prom_results, ravel_results, |a, b| {
        matrix_result_equal(a, b, tolerance_ulps)
    })
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
        ComparisonMode::Unordered
        | ComparisonMode::ExpectError
        | ComparisonMode::RavelErrorPromSuccess => (
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
            compare(ComparisonMode::Unordered, None, &body, &body),
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
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
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
            compare(ComparisonMode::Ordered, None, &prom, &ravel),
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
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
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
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn a_value_outside_tolerance_still_mismatches() {
        let prom = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "1.0"]}
        });
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "1.5"]}
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, Some(4), &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn a_value_within_tolerance_matches() {
        let a = 1.0_f64;
        let b = f64::from_bits(a.to_bits() + 3); // three representable steps above `a`
        let prom = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, a.to_string()]}
        });
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, b.to_string()]}
        });
        assert_eq!(
            compare(ComparisonMode::Unordered, Some(4), &prom, &ravel),
            Verdict::Match
        );
        assert!(matches!(
            compare(ComparisonMode::Unordered, Some(2), &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn negative_zero_never_matches_under_any_tolerance() {
        let prom = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "-0"]}
        });
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "0"]}
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, Some(u32::MAX), &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn opposite_signs_never_match_under_tolerance() {
        let prom = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "0.0000001"]}
        });
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "-0.0000001"]}
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, Some(u32::MAX), &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn error_class_match_ignores_message_text() {
        let prom =
            json!({"status": "error", "errorType": "bad_data", "error": "prometheus wording"});
        let ravel = json!({"status": "error", "errorType": "bad_data", "error": "ravel wording"});
        assert_eq!(
            compare(ComparisonMode::ExpectError, None, &prom, &ravel),
            Verdict::Match
        );
    }

    #[test]
    fn error_class_mismatch_is_caught() {
        let prom = json!({"status": "error", "errorType": "bad_data", "error": "x"});
        let ravel = json!({"status": "error", "errorType": "execution", "error": "y"});
        assert!(matches!(
            compare(ComparisonMode::ExpectError, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn one_side_succeeding_when_error_expected_is_a_mismatch() {
        let prom = json!({"status": "error", "errorType": "bad_data", "error": "x"});
        let ravel = json!({"status": "success", "data": {"resultType": "vector", "result": []}});
        assert!(matches!(
            compare(ComparisonMode::ExpectError, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn ravel_error_prom_success_matches_the_accepted_one_sided_divergence() {
        let prom = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1.0, "1"]}]}
        });
        let ravel =
            json!({"status": "error", "errorType": "execution", "error": "too many points"});
        assert_eq!(
            compare(ComparisonMode::RavelErrorPromSuccess, None, &prom, &ravel),
            Verdict::Match
        );
    }

    #[test]
    fn ravel_error_prom_success_rejects_both_erroring() {
        let prom = json!({"status": "error", "errorType": "bad_data", "error": "x"});
        let ravel = json!({"status": "error", "errorType": "execution", "error": "y"});
        assert!(matches!(
            compare(ComparisonMode::RavelErrorPromSuccess, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn ravel_error_prom_success_rejects_both_succeeding() {
        let body = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []}
        });
        assert!(matches!(
            compare(ComparisonMode::RavelErrorPromSuccess, None, &body, &body),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn ravel_error_prom_success_rejects_prometheus_erroring() {
        let prom = json!({"status": "error", "errorType": "bad_data", "error": "x"});
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []}
        });
        assert!(matches!(
            compare(ComparisonMode::RavelErrorPromSuccess, None, &prom, &ravel),
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
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }
}
