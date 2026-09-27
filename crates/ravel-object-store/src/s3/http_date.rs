//! IMF-fixdate parsing for the store-clock observation (ADR-1685 decision 1).
//!
//! Every S3 response carries a `Date` header, and RFC 7231 section 7.1.1.2
//! requires it in the IMF-fixdate form (`Sun, 06 Nov 1994 08:49:37 GMT`): a
//! fixed-width, GMT-only, ASCII spelling. The two obsolete forms (RFC 850 and
//! asctime) are deliberately not accepted, because a sender that emits one is
//! not the S3-compatible endpoint this observation is about, and a wrong
//! reading here moves a writer's flush decision.
//!
//! No workspace dependency parses HTTP dates: `reqwest` and `hyper` expose no
//! date parser, and neither `chrono` nor `time` is in
//! `[workspace.dependencies]`. Fixed-width IMF-fixdate is small enough to parse
//! here rather than add one.

/// Unix nanoseconds for an IMF-fixdate `Date` header value, or `None` for
/// anything this does not accept: a different date format, a malformed field, a
/// calendar-invalid day, or a year so far out that the result does not fit an
/// `i64` of nanoseconds.
///
/// The weekday is checked for spelling but its value is ignored: a sender whose
/// weekday disagrees with its own date has a broken clock either way, and the
/// day-month-year fields are what the timestamp is read from.
pub(crate) fn parse_imf_fixdate_ns(value: &str) -> Option<i64> {
    // Exactly `Sun, 06 Nov 1994 08:49:37 GMT`: 29 bytes, every field at a fixed
    // offset. Bounds-checking the length once lets every field below index
    // directly.
    let bytes = value.as_bytes();
    if bytes.len() != 29 {
        return None;
    }
    if bytes[3] != b',' || bytes[4] != b' ' || bytes[7] != b' ' || bytes[11] != b' ' {
        return None;
    }
    if bytes[16] != b' ' || bytes[19] != b':' || bytes[22] != b':' {
        return None;
    }
    if &bytes[25..29] != b" GMT" {
        return None;
    }
    const WEEKDAYS: [&[u8]; 7] = [b"Mon", b"Tue", b"Wed", b"Thu", b"Fri", b"Sat", b"Sun"];
    if !WEEKDAYS.contains(&&bytes[0..3]) {
        return None;
    }
    let day = two_digits(&bytes[5..7])?;
    let month = month_of(&bytes[8..11])?;
    let year = four_digits(&bytes[12..16])?;
    let hour = two_digits(&bytes[17..19])?;
    let minute = two_digits(&bytes[20..22])?;
    // RFC 7231 allows 60 for a leap second. Read as written rather than
    // clamped: one second of overshoot in a value that is used as a lower bound
    // is harmless, and silently rewriting a field is not.
    let second = two_digits(&bytes[23..25])?;
    if day < 1 || day > days_in_month(year, month) || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour) * 3600 + i64::from(minute) * 60 + i64::from(second))?;
    seconds.checked_mul(1_000_000_000)
}

fn two_digits(bytes: &[u8]) -> Option<u32> {
    let mut value = 0;
    for byte in bytes {
        value = value * 10 + u32::from(byte.checked_sub(b'0').filter(|d| *d < 10)?);
    }
    Some(value)
}

fn four_digits(bytes: &[u8]) -> Option<i64> {
    two_digits(bytes).map(i64::from)
}

fn month_of(name: &[u8]) -> Option<u32> {
    const MONTHS: [&[u8]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];
    MONTHS
        .iter()
        .position(|month| *month == name)
        .map(|index| index as u32 + 1)
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        _ => 28,
    }
}

/// Days from the unix epoch to `year-month-day` (Howard Hinnant's
/// `days_from_civil`, shifting the year to start in March so the leap day is
/// the last day of the shifted year). `month` is 1-based and `day` is a valid
/// day of that month; both are checked by the caller.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(if month > 2 { month - 3 } else { month + 9 });
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// The RFC 7231 example itself, against the value an independent
    /// implementation gives for it (784_111_777 unix seconds).
    #[test]
    fn the_rfc_example_parses_to_its_unix_nanoseconds() {
        assert_eq!(
            parse_imf_fixdate_ns("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784_111_777_000_000_000)
        );
    }

    #[test]
    fn the_epoch_itself_is_zero() {
        assert_eq!(
            parse_imf_fixdate_ns("Thu, 01 Jan 1970 00:00:00 GMT"),
            Some(0)
        );
    }

    /// A century year that is a leap year (2000) and one that is not (2100):
    /// the two branches `is_leap_year`'s `% 400` rule distinguishes.
    #[test]
    fn leap_days_land_on_the_right_second() {
        assert_eq!(
            parse_imf_fixdate_ns("Tue, 29 Feb 2000 00:00:00 GMT"),
            Some(951_782_400_000_000_000)
        );
        assert_eq!(
            parse_imf_fixdate_ns("Mon, 01 Mar 2100 00:00:00 GMT"),
            Some(4_107_542_400_000_000_000)
        );
    }

    /// 2026-02-29 does not exist. Accepting it would report a time one day
    /// later than the sender meant.
    #[test]
    fn a_day_the_month_does_not_have_is_refused() {
        assert_eq!(parse_imf_fixdate_ns("Sun, 29 Feb 2026 00:00:00 GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Wed, 31 Apr 2026 00:00:00 GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Wed, 00 Apr 2026 00:00:00 GMT"), None);
    }

    #[test]
    fn out_of_range_time_fields_are_refused() {
        assert_eq!(parse_imf_fixdate_ns("Wed, 01 Apr 2026 24:00:00 GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Wed, 01 Apr 2026 00:60:00 GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Wed, 01 Apr 2026 00:00:61 GMT"), None);
    }

    /// The leap second RFC 7231 permits in the seconds field.
    #[test]
    fn a_leap_second_is_read_as_written() {
        assert_eq!(
            parse_imf_fixdate_ns("Sat, 31 Dec 2016 23:59:60 GMT"),
            Some(1_483_228_800_000_000_000)
        );
    }

    #[test]
    fn garbage_is_refused() {
        assert_eq!(parse_imf_fixdate_ns(""), None);
        assert_eq!(parse_imf_fixdate_ns("not-a-valid-date"), None);
        assert_eq!(parse_imf_fixdate_ns("Sun, 06 Nov 1994 08:49:37"), None);
        assert_eq!(parse_imf_fixdate_ns("Sun, 06 Nov 1994 08:49:3x GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Sun; 06 Nov 1994 08:49:37 GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Sun, 06 Nov 1994 08-49-37 GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Xxx, 06 Nov 1994 08:49:37 GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Sun, 06 Xxx 1994 08:49:37 GMT"), None);
        // Trailing and leading space change the fixed offsets, so neither is a
        // near miss this accepts.
        assert_eq!(parse_imf_fixdate_ns(" Sun, 06 Nov 1994 08:49:37 GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Sun, 06 Nov 1994 08:49:37 GMT "), None);
    }

    /// The two obsolete HTTP-date formats RFC 7231 still requires recipients to
    /// tolerate. Refused here on purpose: an S3-compatible endpoint sends
    /// IMF-fixdate, and no observation is better than a mis-parsed one.
    #[test]
    fn the_obsolete_http_date_formats_are_refused() {
        assert_eq!(parse_imf_fixdate_ns("Sunday, 06-Nov-94 08:49:37 GMT"), None);
        assert_eq!(parse_imf_fixdate_ns("Sun Nov  6 08:49:37 1994"), None);
    }

    /// A year whose nanoseconds do not fit an `i64` (the representable range
    /// ends in 2262). The parse refuses rather than wrapping into a negative
    /// time, which would read as a store clock decades in the past.
    #[test]
    fn a_year_past_the_representable_range_is_refused() {
        assert_eq!(parse_imf_fixdate_ns("Fri, 31 Dec 9999 23:59:59 GMT"), None);
    }
}
