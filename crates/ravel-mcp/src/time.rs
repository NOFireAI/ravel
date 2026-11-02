//! Time arguments shared by every tool that takes a `time_range`.
//!
//! A `time_range` bound is a string: either an RFC 3339 timestamp in UTC
//! (`2026-09-12T10:00:00Z`, fractional seconds allowed) or a decimal
//! nanosecond epoch (`1789207200000000000`). Anything else, and any instant
//! outside what an `i64` nanosecond epoch can hold, is an `invalid_argument`
//! failure rather than a clamp, so a caller never queries a window it did not
//! write.

use std::time::UNIX_EPOCH;

use crate::envelope::{Failure, FailureClass, TimeRange};

/// Parse one `time_range` bound into a nanosecond epoch. `name` is the
/// argument path the failure message names (`time_range.start_ns`).
pub fn to_ns(name: &str, raw: &str) -> Result<i64, Failure> {
    if let Ok(ns) = raw.parse::<i64>() {
        return Ok(ns);
    }
    let unusable = || {
        invalid(format!(
            "{name} {raw:?} is neither an RFC 3339 timestamp nor an integer nanosecond epoch"
        ))
    };
    let instant = humantime::parse_rfc3339(raw).map_err(|_| unusable())?;
    let ns = match instant.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_nanos()).ok(),
        Err(before) => i64::try_from(before.duration().as_nanos())
            .ok()
            .and_then(i64::checked_neg),
    };
    ns.ok_or_else(|| {
        invalid(format!(
            "{name} {raw:?} is outside the nanosecond epoch range an i64 can hold"
        ))
    })
}

/// Parse a `time_range` into the half-open window `[start_ns, end_ns)`. A
/// start after the end is refused; an empty window (`start == end`) is not.
pub fn window(range: &TimeRange) -> Result<ravel_types::TimeRange, Failure> {
    let start_ns = to_ns("time_range.start_ns", &range.start_ns)?;
    let end_ns = to_ns("time_range.end_ns", &range.end_ns)?;
    if start_ns > end_ns {
        return Err(invalid(
            "time_range.start_ns must not be after time_range.end_ns".to_string(),
        ));
    }
    Ok(ravel_types::TimeRange { start_ns, end_ns })
}

fn invalid(message: String) -> Failure {
    Failure {
        class: FailureClass::InvalidArgument,
        message,
        counter: None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn range(start: &str, end: &str) -> TimeRange {
        TimeRange {
            start_ns: start.to_string(),
            end_ns: end.to_string(),
        }
    }

    /// FLIP: a parser that read RFC 3339 as local time, dropped the
    /// fractional seconds, or returned seconds rather than nanoseconds gives a
    /// different number from the decimal spelling of the same instant.
    #[test]
    fn rfc3339_equals_its_decimal_nanosecond_spelling() {
        assert_eq!(
            to_ns("t", "2026-09-12T10:00:00Z").expect("rfc3339"),
            to_ns("t", "1789207200000000000").expect("decimal"),
        );
        assert_eq!(
            to_ns("t", "2026-09-12T10:00:00.000000123Z").expect("rfc3339"),
            1_789_207_200_000_000_123,
        );
        assert_eq!(to_ns("t", "1970-01-01T00:00:00Z").expect("epoch"), 0);
        assert_eq!(to_ns("t", "-5").expect("negative decimal"), -5);
    }

    /// FLIP: a window that swapped the bounds, or clamped a reversed range to
    /// empty, would answer a query the caller did not write.
    #[test]
    fn a_start_after_the_end_is_invalid_argument() {
        let failure =
            window(&range("2026-09-12T10:00:01Z", "2026-09-12T10:00:00Z")).expect_err("reversed");
        assert_eq!(failure.class, FailureClass::InvalidArgument);
        assert_eq!(
            failure.message,
            "time_range.start_ns must not be after time_range.end_ns"
        );
        let empty = window(&range("7", "7")).expect("an empty window is a window");
        assert_eq!(
            empty,
            ravel_types::TimeRange {
                start_ns: 7,
                end_ns: 7
            }
        );
        let mixed = window(&range("1970-01-01T00:00:00Z", "9")).expect("mixed spellings");
        assert_eq!(
            mixed,
            ravel_types::TimeRange {
                start_ns: 0,
                end_ns: 9
            }
        );
    }

    /// FLIP: a parser that fell back to zero, to "now", or to a lenient
    /// format on text it does not recognise would run a query over a window
    /// nobody asked for.
    #[test]
    fn nonsense_and_overflow_are_invalid_argument() {
        for raw in [
            "soon",
            "",
            "1.5",
            "2026-09-12",
            "2026-09-12 10:00:00",
            "9223372036854775808",
            "9999-12-31T23:59:59Z",
        ] {
            let failure = to_ns("time_range.end_ns", raw).expect_err(raw);
            assert_eq!(failure.class, FailureClass::InvalidArgument, "{raw:?}");
            assert!(
                failure.message.starts_with("time_range.end_ns "),
                "{}",
                failure.message
            );
        }
    }
}
