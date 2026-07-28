//! Go-`strconv.FormatFloat`-compatible float formatting for `le` and
//! `quantile` label values.
//!
//! Prometheus classic histogram/summary series identity depends on this
//! formatting matching the collector's `prometheusremotewrite` exporter
//! byte-for-byte (ADR-0016): the same histogram ingested OTLP-exploded and
//! RW-classic must produce the same `SeriesId`. Go's
//! `strconv.FormatFloat(v, 'f', -1, 64)` (shortest round-tripping decimal, no
//! exponent notation) and Rust's default `f64` `Display` produce identical
//! output for every finite value, verified empirically across representative
//! magnitudes including subnormals, `1e308`, and precision-boundary values
//! near 2^53. They differ only in how infinities are spelled.

/// Formats `v` to match Go's `strconv.FormatFloat(v, 'f', -1, 64)`.
pub fn format_float(v: f64) -> String {
    if v.is_infinite() {
        if v.is_sign_negative() {
            "-Inf".to_string()
        } else {
            "+Inf".to_string()
        }
    } else {
        v.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::approx_constant)]
    fn golden_vectors_match_go_strconv_format_float() {
        let cases: &[(f64, &str)] = &[
            (0.1, "0.1"),
            (1.0, "1"),
            (10.0, "10"),
            (100.0, "100"),
            (1000.0, "1000"),
            (0.001, "0.001"),
            (0.0001, "0.0001"),
            (1e-10, "0.0000000001"),
            (1e10, "10000000000"),
            (1e20, "100000000000000000000"),
            (1e21, "1000000000000000000000"),
            (1e-20, "0.00000000000000000001"),
            (1e-21, "0.000000000000000000001"),
            (3.14159265358979, "3.14159265358979"),
            (2.5, "2.5"),
            (-2.5, "-2.5"),
            (0.0, "0"),
            (-0.0, "-0"),
            (123456789.123456, "123456789.123456"),
            (0.0000001, "0.0000001"),
            (9999999999999999.0, "10000000000000000"),
            (0.9999999999999999, "0.9999999999999999"),
            (1.0000000000000002, "1.0000000000000002"),
            (100000000000000000000.0, "100000000000000000000"),
            (0.00000000000001, "0.00000000000001"),
            (
                1.7976931348623157e308,
                "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            ),
            (
                4.9e-324,
                "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005",
            ),
        ];
        for (v, expected) in cases {
            assert_eq!(&format_float(*v), expected, "input {v:e}");
        }
    }

    #[test]
    fn infinities_use_go_spelling() {
        assert_eq!(format_float(f64::INFINITY), "+Inf");
        assert_eq!(format_float(f64::NEG_INFINITY), "-Inf");
    }
}
