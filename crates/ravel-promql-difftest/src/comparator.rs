//! Compares a Prometheus JSON response against a Ravel JSON response for
//! bit-exact equivalence (ADR-0021 decision 3).
//!
//! Rules: sample values compare by `f64::to_bits`, except any two NaN bit
//! patterns compare equal as a class (the stale marker and an ordinary NaN
//! both round-trip through the JSON `"NaN"` literal, so this also makes the
//! comparator agnostic to that internal bit pattern); `-0.0` is otherwise
//! bit-significant. Result vectors compare label-set-sorted unless the
//! corpus entry is marked order-sensitive. Errors compare by `errorType`
//! class string equality. The two annotation channels `warnings` and
//! `infos` each compare by presence only, but independently: they are
//! distinct Prometheus fields, so a query where one engine
//! emits an info and the other a warning (or nothing) is a mismatch.
//!
//! A corpus entry may opt into a bounded ULP tolerance (ADR-0025's
//! allowlist: a named, measured, per-entry exception, not a global fuzzy
//! comparison). `-0.0` vs `0.0` never matches under any tolerance: this
//! project's `-0.0` bit-significance rule is a matter of principle, not
//! magnitude, so the zero boundary is always exact-bit-compared.
//!
//! Native-histogram elements (ADR-0108 decision 10) compare by a canonical
//! semantic form, never a byte layout: two engines can encode the same
//! histogram with different internal span layouts, so the wire form is what is
//! compared. A vector element carries either a float `value` or a native
//! `histogram`; a matrix series carries float steps under `values` and
//! histogram steps under `histograms`, either or both across its grid. Which
//! channel an element uses is part of its series identity, so a series
//! Prometheus returns as a histogram and Ravel returns as a float (or the
//! reverse) is a mismatch, not a match. A histogram value's bucket structure
//! (each bucket's interval-openness code and its lower/upper boundaries, in
//! order) and its per-bucket counts compare exactly; the histogram's total
//! `count` and `sum` compare under the same bit-exact-or-ULP value rule as any
//! float, so an entry's `tolerance:` field reaches them because both engines
//! compute extrapolated rates independently. Boundaries and counts are parsed
//! from their Prometheus value strings and bit-compared, so a formatting
//! difference over the same `f64` is never a divergence.
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

/// The channel an element carries: a float value (`value`/`values`), a native
/// histogram (`histogram`/`histograms`), or both across a matrix series' grid.
/// Part of a series' identity: a series Prometheus returns as a histogram and
/// Ravel returns as a float for the same label set is a divergence, so the two
/// must never sort into the same identity slot.
fn element_channel(elem: &Json) -> &'static str {
    let has_float = elem.get("value").is_some_and(|v| !v.is_null())
        || elem
            .get("values")
            .and_then(Json::as_array)
            .is_some_and(|a| !a.is_empty());
    let has_hist = elem.get("histogram").is_some_and(|v| !v.is_null())
        || elem
            .get("histograms")
            .and_then(Json::as_array)
            .is_some_and(|a| !a.is_empty());
    match (has_float, has_hist) {
        (true, true) => "float+histogram",
        (false, true) => "histogram",
        (true, false) => "float",
        (false, false) => "empty",
    }
}

/// A result element's identity for order-insensitive comparison: its full
/// label set plus the channel it uses. The `\u{1f}` unit separator keeps the
/// two parts from colliding (no label name or value contains it).
fn series_identity(elem: &Json) -> String {
    format!(
        "{}\u{1f}{}",
        label_key(&elem["metric"]),
        element_channel(elem)
    )
}

/// A vector element's native `histogram` field, if present and non-null.
fn histogram_field(elem: &Json) -> Option<&Json> {
    elem.get("histogram").filter(|v| !v.is_null())
}

/// A named array field, or an empty slice when the field is absent (Prometheus
/// omits `values`/`histograms` when a series has no steps of that channel).
fn array_field<'a>(elem: &'a Json, field: &str) -> &'a [Json] {
    elem.get(field)
        .and_then(Json::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// One `SampleHistogram` bucket's field `i` as a Prometheus value string.
fn bucket_str(arr: &[Json], i: usize) -> Result<&str, String> {
    arr[i]
        .as_str()
        .ok_or_else(|| format!("histogram bucket field {i} is not a string"))
}

/// One histogram scalar field (`count`/`sum`) parsed to its `f64`.
fn histogram_scalar(h: &Json, field: &str) -> Result<f64, String> {
    let s = h
        .get(field)
        .and_then(Json::as_str)
        .ok_or_else(|| format!("histogram missing string field '{field}'"))?;
    parse_sample_value(s)
}

/// Compares one `[boundaries, lower, upper, count]` bucket tuple. The
/// interval-openness code is an exact integer identity; the lower/upper
/// boundaries and the per-bucket count are parsed from their value strings and
/// bit-compared exactly. Bucket structure and counts do not take the entry's
/// ULP tolerance: that applies only to the histogram's total `count`/`sum`.
fn bucket_equal(a: &Json, b: &Json) -> Result<bool, String> {
    let a_arr = a.as_array().ok_or("histogram bucket is not an array")?;
    let b_arr = b.as_array().ok_or("histogram bucket is not an array")?;
    if a_arr.len() != 4 || b_arr.len() != 4 {
        return Err(format!(
            "histogram bucket has {} / {} elements, expected 4",
            a_arr.len(),
            b_arr.len()
        ));
    }
    if a_arr[0] != b_arr[0] {
        return Ok(false);
    }
    for i in [1usize, 2, 3] {
        let a_v = parse_sample_value(bucket_str(a_arr, i)?)?;
        let b_v = parse_sample_value(bucket_str(b_arr, i)?)?;
        if !values_equal(a_v, b_v, None) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Compares two Prometheus `SampleHistogram` JSON objects
/// (`{count, sum, buckets}`) for semantic equality: the bucket structure and
/// per-bucket counts exact, the total `count`/`sum` under the entry's
/// bit-exact-or-ULP value rule.
fn histogram_json_equal(a: &Json, b: &Json, tolerance_ulps: Option<u32>) -> Result<bool, String> {
    if !values_equal(
        histogram_scalar(a, "count")?,
        histogram_scalar(b, "count")?,
        tolerance_ulps,
    ) {
        return Ok(false);
    }
    if !values_equal(
        histogram_scalar(a, "sum")?,
        histogram_scalar(b, "sum")?,
        tolerance_ulps,
    ) {
        return Ok(false);
    }
    let a_buckets = array_field(a, "buckets");
    let b_buckets = array_field(b, "buckets");
    if a_buckets.len() != b_buckets.len() {
        return Ok(false);
    }
    for (ab, bb) in a_buckets.iter().zip(b_buckets.iter()) {
        if !bucket_equal(ab, bb)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Compares one `[<ts>, <histogram>]` sample pair: the timestamp exact, the
/// histogram object by [`histogram_json_equal`].
fn histogram_pair_equal(a: &Json, b: &Json, tolerance_ulps: Option<u32>) -> Result<bool, String> {
    let a_arr = a
        .as_array()
        .ok_or("histogram sample is not a 2-element array")?;
    let b_arr = b
        .as_array()
        .ok_or("histogram sample is not a 2-element array")?;
    if a_arr.len() != 2 || b_arr.len() != 2 {
        return Err(format!(
            "histogram sample has {} / {} elements, expected 2",
            a_arr.len(),
            b_arr.len()
        ));
    }
    let a_ts = a_arr[0]
        .as_f64()
        .ok_or("histogram sample timestamp is not a number")?;
    let b_ts = b_arr[0]
        .as_f64()
        .ok_or("histogram sample timestamp is not a number")?;
    if a_ts != b_ts {
        return Ok(false);
    }
    histogram_json_equal(&a_arr[1], &b_arr[1], tolerance_ulps)
}

fn vector_result_equal(a: &Json, b: &Json, tolerance_ulps: Option<u32>) -> Result<bool, String> {
    if a["metric"] != b["metric"] {
        return Ok(false);
    }
    match (histogram_field(a), histogram_field(b)) {
        (Some(ah), Some(bh)) => histogram_pair_equal(ah, bh, tolerance_ulps),
        (None, None) => sample_pair_equal(&a["value"], &b["value"], tolerance_ulps),
        // Channel divergence: one engine returns a float element, the other a
        // native histogram, for the same series. Never a match.
        _ => Ok(false),
    }
}

fn matrix_result_equal(a: &Json, b: &Json, tolerance_ulps: Option<u32>) -> Result<bool, String> {
    if a["metric"] != b["metric"] {
        return Ok(false);
    }
    let a_values = array_field(a, "values");
    let b_values = array_field(b, "values");
    if a_values.len() != b_values.len() {
        return Ok(false);
    }
    for (av, bv) in a_values.iter().zip(b_values.iter()) {
        if !sample_pair_equal(av, bv, tolerance_ulps)? {
            return Ok(false);
        }
    }
    let a_hist = array_field(a, "histograms");
    let b_hist = array_field(b, "histograms");
    if a_hist.len() != b_hist.len() {
        return Ok(false);
    }
    for (av, bv) in a_hist.iter().zip(b_hist.iter()) {
        if !histogram_pair_equal(av, bv, tolerance_ulps)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn sort_by_series_identity(results: &[Json]) -> Vec<Json> {
    let mut sorted: Vec<Json> = results.to_vec();
    sorted.sort_by_key(series_identity);
    sorted
}

fn has_warnings(body: &Json) -> bool {
    has_nonempty_array(body, "warnings")
}

fn has_infos(body: &Json) -> bool {
    has_nonempty_array(body, "infos")
}

fn has_nonempty_array(body: &Json, field: &str) -> bool {
    body.get(field)
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

    // `infos` is a distinct Prometheus field from `warnings`:
    // an out-of-range `quantile` clamp warns, a forced-monotonicity fixup
    // informs. Before Ravel had any annotation channel the comparator could
    // only skip `infos`; now it checks its presence too, so a query where
    // one engine emits an info and the other does not is caught.
    if has_infos(prometheus) != has_infos(ravel) {
        return Verdict::Mismatch(format!(
            "info presence mismatch: prometheus={} ravel={}",
            has_infos(prometheus),
            has_infos(ravel)
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
            sort_by_series_identity(&prom_results),
            sort_by_series_identity(&ravel_results),
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

    #[test]
    fn info_presence_mismatch_is_caught_independently_of_warnings() {
        // Both sides agree on the (empty) warnings channel and on the result,
        // but only Prometheus carries an `infos` entry. Before the comparator
        // checked `infos` it skipped the field entirely and this pair compared
        // equal;
        // now the info-presence check must catch it, proving `infos` is
        // compared and is a channel distinct from `warnings`.
        let prom = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []},
            "infos": ["input to histogram_quantile needed to be fixed for monotonicity"]
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

    #[test]
    fn matching_infos_on_both_sides_still_match() {
        // Presence, not content: both sides carry some info, so they agree.
        let prom = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []},
            "infos": ["prometheus wording for the info"]
        });
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []},
            "infos": ["ravel wording for the info"]
        });
        assert_eq!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Match
        );
    }

    // ---- Native-histogram elements (ADR-0108 decision 10) ----

    /// A vector `/api/v1/query` envelope wrapping one native-histogram element.
    fn histogram_vector(hist: Json) -> Json {
        json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {"__name__": "h"}, "histogram": [1.0, hist]}]
            }
        })
    }

    /// A matrix `/api/v1/query_range` envelope wrapping one native-histogram
    /// series with a single step.
    fn histogram_matrix(hist: Json) -> Json {
        json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{"metric": {"__name__": "h"}, "histograms": [[1.0, hist]]}]
            }
        })
    }

    /// A three-bucket schema-0 native histogram in Prometheus' JSON shape.
    fn sample_histogram() -> Json {
        json!({
            "count": "6",
            "sum": "16",
            "buckets": [[0, "1", "2", "2"], [0, "2", "4", "3"], [0, "4", "8", "1"]]
        })
    }

    #[test]
    fn identical_histogram_vector_elements_match() {
        let body = histogram_vector(sample_histogram());
        assert_eq!(
            compare(ComparisonMode::Unordered, None, &body, &body),
            Verdict::Match
        );
    }

    #[test]
    fn identical_histogram_matrix_series_match() {
        let body = histogram_matrix(sample_histogram());
        assert_eq!(
            compare(ComparisonMode::Unordered, None, &body, &body),
            Verdict::Match
        );
    }

    /// The silent-zeros shape (ADR-0108 context 1): the grid collapse dropped
    /// the histogram and emitted a placeholder float. Prometheus returns the
    /// histogram; pre-fix Ravel returns `value: "0"` for the same series. The
    /// channel divergence is a mismatch. This is what a range
    /// `sum(rate(h[5m]))` entry would have flipped red pre-fix.
    #[test]
    fn a_float_zero_where_prometheus_returns_a_histogram_is_caught() {
        let prom = histogram_vector(sample_histogram());
        let ravel = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {"__name__": "h"}, "value": [1.0, "0"]}]
            }
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    /// The silent-zeros shape on the range endpoint: a matrix step Prometheus
    /// returns as a histogram, pre-fix Ravel as an all-zero float step.
    #[test]
    fn a_float_zero_matrix_step_where_prometheus_returns_a_histogram_is_caught() {
        let prom = histogram_matrix(sample_histogram());
        let ravel = json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{"metric": {"__name__": "h"}, "values": [[1.0, "0"]]}]
            }
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    /// The silent-empties shape (ADR-0108 context 2): a bare histogram
    /// selector or a histogram `rate` whose range arm dropped the histogram
    /// input, so pre-fix Ravel returns an empty result where Prometheus
    /// returns a histogram series. Caught as a result-length mismatch.
    #[test]
    fn a_dropped_histogram_series_where_prometheus_returns_one_is_caught() {
        let prom = histogram_matrix(sample_histogram());
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "matrix", "result": []}
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    /// The false-absence shape (ADR-0108 context 3): `absent_over_time(h[15m])`
    /// with histogram data flowing. Prometheus sees the data and returns empty;
    /// pre-fix Ravel's float-only fetch sees nothing and returns 1. Caught as a
    /// result-length mismatch.
    #[test]
    fn a_false_absence_marker_where_prometheus_returns_empty_is_caught() {
        let prom = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []}
        });
        let ravel = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {}, "value": [1.0, "1"]}]
            }
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    /// Two histograms differing only in one bucket's count are a mismatch even
    /// under a generous total-count/sum tolerance: bucket counts are exact, so
    /// a wrong bucket cannot hide behind a tolerance meant for the aggregate
    /// floats.
    #[test]
    fn a_bucket_count_divergence_is_caught_even_under_tolerance() {
        let prom = histogram_vector(sample_histogram());
        let mut other = sample_histogram();
        other["buckets"][1][3] = json!("4");
        let ravel = histogram_vector(other);
        assert!(matches!(
            compare(ComparisonMode::Unordered, Some(u32::MAX), &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    /// A bucket boundary difference (a different schema/resolution) is a
    /// mismatch: the bucket structure is compared exactly.
    #[test]
    fn a_bucket_boundary_divergence_is_caught() {
        let prom = histogram_vector(sample_histogram());
        let mut other = sample_histogram();
        other["buckets"][2][2] = json!("16");
        let ravel = histogram_vector(other);
        assert!(matches!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    /// The histogram's total `count`/`sum` take the entry's ULP tolerance,
    /// because both engines compute extrapolated rates independently: a
    /// few-ULP drift in `sum` matches under tolerance and mismatches without.
    #[test]
    fn a_histogram_sum_within_tolerance_matches_and_without_it_does_not() {
        let base = 16.0_f64;
        let drifted = f64::from_bits(base.to_bits() + 3);
        let prom = histogram_vector(sample_histogram());
        let mut other = sample_histogram();
        other["sum"] = json!(drifted.to_string());
        let ravel = histogram_vector(other);
        assert_eq!(
            compare(ComparisonMode::Unordered, Some(4), &prom, &ravel),
            Verdict::Match
        );
        assert!(matches!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    /// A histogram whose buckets are listed in a different order is not the
    /// same histogram: bucket order is cumulative-ascending and structural.
    #[test]
    fn a_reordered_bucket_list_is_a_mismatch() {
        let prom = histogram_vector(sample_histogram());
        let reordered = json!({
            "count": "6",
            "sum": "16",
            "buckets": [[0, "2", "4", "3"], [0, "1", "2", "2"], [0, "4", "8", "1"]]
        });
        let ravel = histogram_vector(reordered);
        assert!(matches!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    /// Two equal histograms whose boundary strings are formatted differently
    /// for the same numeric value still match: boundaries are parsed and
    /// bit-compared, not string-compared.
    #[test]
    fn equal_boundaries_formatted_differently_still_match() {
        let prom = histogram_vector(sample_histogram());
        let reformatted = json!({
            "count": "6",
            "sum": "16",
            "buckets": [[0, "1.0", "2", "2"], [0, "2", "4", "3"], [0, "4", "8", "1"]]
        });
        let ravel = histogram_vector(reformatted);
        assert_eq!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Match
        );
    }

    /// A histogram element and a float element for the same label set are a
    /// channel divergence, caught even when both sides carry one result.
    #[test]
    fn a_histogram_vs_float_channel_divergence_is_caught() {
        let prom = histogram_vector(sample_histogram());
        let ravel = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {"__name__": "h"}, "value": [1.0, "6"]}]
            }
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }

    #[test]
    fn a_warning_on_one_side_and_an_info_on_the_other_is_a_mismatch() {
        // The two channels are severity-distinct: an engine emitting a
        // warning where the other emits only an info is a real divergence,
        // caught because the warnings channel disagrees.
        let prom = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []},
            "warnings": ["quantile value should be between 0 and 1"]
        });
        let ravel = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []},
            "infos": ["some info instead"]
        });
        assert!(matches!(
            compare(ComparisonMode::Unordered, None, &prom, &ravel),
            Verdict::Mismatch(_)
        ));
    }
}
