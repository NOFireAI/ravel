//! Library surface behind the `ravel-cli` binary, split out so integration
//! tests can drive the catalog subcommands in-process against a shared
//! `MemoryStore` (a subprocess per invocation, as `tests/segment_inspect.rs`
//! uses, would give each `ravel-cli catalog ...` call its own empty
//! in-memory store, making a chained fold -> inspect -> verify scenario
//! impossible to construct without a persistent S3 backend).

pub mod catalog;
pub mod cli_profiling;
pub mod erase;
pub mod export;
pub mod gc_config;
pub mod hold;
pub mod idem;
pub mod load;
pub mod maintain;
pub mod provision;
pub mod qualify;
pub mod reconstruct;
pub mod store;
pub mod tenancy;
pub mod tenant_token;
pub mod typed_attr_column;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ns() -> anyhow::Result<i64> {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH)?;
    i64::try_from(dur.as_nanos()).map_err(|_| anyhow::anyhow!("system clock too far in the future"))
}

/// Parse a `--max-flush-lifetime` value into nanoseconds.
///
/// Shared by every subcommand that offers the override: `maintain
/// compact-bucket`, `maintain compact-tenant`, and `catalog fold`. It lives at
/// the crate root rather than in one of them because a third copy of the same
/// three lines is how the three flags start disagreeing about their grammar.
///
/// Same grammar as ravel-server's `--gc-max-flush-lifetime`: a humantime
/// duration string (`1h`, `30m`, `1h5m`), converted to `i64` nanoseconds. The
/// server's parser is `parse_gc_duration_ns` in
/// services/ravel-server/src/config.rs; it is private to a crate ravel-cli does
/// not depend on at build time, so the grammar is copied here rather than
/// shared. One deliberate difference: zero is accepted, because `0s` is the
/// point of this override on a quiescent tenant. Negative values are
/// unrepresentable in humantime, so the only rejections are an unparseable
/// spelling and a value too large for `i64` nanoseconds.
pub fn parse_max_flush_lifetime_ns(s: &str) -> Result<i64, String> {
    let dur = humantime::parse_duration(s)
        .map_err(|e| format!("invalid --max-flush-lifetime '{s}': {e}"))?;
    i64::try_from(dur.as_nanos()).map_err(|_| format!("--max-flush-lifetime '{s}' is too large"))
}

/// Parse a `load --max-flush-delay` value into a [`std::time::Duration`].
///
/// Same humantime grammar as [`parse_max_flush_lifetime_ns`] (`2s`, `10m`,
/// `1h5m`); the loader plumbs the result straight into
/// `IngestConfig::max_flush_delay`, which is a `Duration`, so this returns one
/// rather than an `i64` of nanoseconds. Zero is accepted, mirroring the
/// `--max-flush-lifetime` flags: on the loader's path `0s` means the age
/// trigger fires on the next flush tick for any non-empty buffer (its oldest
/// point is always at least `0s` old), so every buffer flushes by age almost
/// immediately regardless of `--target-bytes`. That holds because every loader
/// write is Strict and therefore leaves a waiter on the buffer it merged into,
/// which is what makes the shard compare the buffer's age against
/// `max_flush_delay` rather than against the slower `max_flush_delay_idle`
/// (`age_threshold_ns`, crates/ravel-ingest/src/log_shard.rs).
///
/// The `i64`-nanosecond ceiling is the same rejection the
/// `--max-flush-lifetime` flags apply, and it is load-bearing here rather than
/// merely tidy: the shard's age check casts `max_flush_delay.as_nanos() as
/// i64`, so a `Duration` past 292 years wraps to a negative threshold that
/// every buffer's age clears on the very next flush tick. The value furthest
/// from "never age out" would otherwise parse as its exact opposite. The
/// ceiling leaves room for the strict-visibility reserve on top, so the
/// derived budget stays strictly greater than the delay instead of saturating
/// equal to it. Negative values are unrepresentable in humantime, so the only
/// rejections are an unparseable spelling and a value too large for the
/// `i64`-nanosecond budget arithmetic.
pub fn parse_max_flush_delay(s: &str) -> Result<std::time::Duration, String> {
    let dur = humantime::parse_duration(s)
        .map_err(|e| format!("invalid --max-flush-delay '{s}': {e}"))?;
    let ceiling = i64::MAX - ravel_ingest::STRICT_VISIBILITY_RESERVE_NS;
    match i64::try_from(dur.as_nanos()) {
        Ok(ns) if ns <= ceiling => Ok(dur),
        _ => Err(format!("--max-flush-delay '{s}' is too large")),
    }
}

/// Parse an `export --max-ingest-lag` value into nanoseconds.
///
/// Same humantime grammar as [`parse_max_flush_lifetime_ns`], naming the flag
/// it belongs to in its errors. It mirrors ravel-server's `--max-ingest-lag`,
/// whose parser lives in a crate ravel-cli does not depend on at build time,
/// so the grammar is matched here rather than shared. Zero is refused, as the
/// server refuses it: no deployment runs with a zero lag, so a zero here could
/// only resolve a window no server's own queries resolve. Negative
/// values are unrepresentable in humantime, so the other rejections are an
/// unparseable spelling and a value too large for `i64` nanoseconds.
pub fn parse_max_ingest_lag_ns(s: &str) -> Result<i64, String> {
    let dur =
        humantime::parse_duration(s).map_err(|e| format!("invalid --max-ingest-lag '{s}': {e}"))?;
    if dur.is_zero() {
        return Err(format!(
            "--max-ingest-lag '{s}' must be a positive duration: ravel-server refuses a zero \
             lag, so no deployment's queries resolve the window a zero lag would"
        ));
    }
    i64::try_from(dur.as_nanos()).map_err(|_| format!("--max-ingest-lag '{s}' is too large"))
}

/// Parse an `export --start`/`--end` value: an RFC 3339 timestamp with any
/// offset, to nanoseconds since the Unix epoch.
///
/// `chrono` is not a workspace dependency, and `humantime::parse_rfc3339`
/// accepts only a UTC designator (`Z` or `+00:00`). So the offset is split off
/// here, the local date-time is parsed by humantime as if it were UTC, and the
/// offset is subtracted: `2026-01-02T05:04:05+02:00` is
/// `2026-01-02T03:04:05Z`. Fractional seconds are kept. An instant before the
/// Unix epoch is rejected, since `export`'s window has no use for one.
pub fn parse_rfc3339_ns(s: &str) -> Result<i64, String> {
    let invalid = |why: &dyn std::fmt::Display| format!("invalid RFC 3339 timestamp '{s}': {why}");
    let (local, offset_secs) = split_rfc3339_offset(s)
        .ok_or_else(|| invalid(&"expected a trailing `Z` or a `+HH:MM`/`-HH:MM` offset"))?;
    let system_time = humantime::parse_rfc3339(&format!("{local}Z")).map_err(|e| invalid(&e))?;
    let too_far = || format!("timestamp '{s}' is too far in the future");
    let local_ns = match system_time.duration_since(UNIX_EPOCH) {
        Ok(after) => i128::try_from(after.as_nanos()).map_err(|_| too_far())?,
        Err(before) => -i128::try_from(before.duration().as_nanos()).map_err(|_| too_far())?,
    };
    let utc_ns = local_ns - i128::from(offset_secs) * 1_000_000_000;
    if utc_ns < 0 {
        return Err(format!("timestamp '{s}' is before the Unix epoch"));
    }
    i64::try_from(utc_ns).map_err(|_| too_far())
}

/// Split an RFC 3339 timestamp into its local date-time and its offset east
/// of UTC in seconds. `Z`/`z` is offset zero; otherwise the last six
/// characters must be `+HH:MM` or `-HH:MM` with `HH` at most 23 and `MM` at
/// most 59. `None` for anything else.
fn split_rfc3339_offset(s: &str) -> Option<(&str, i64)> {
    if let Some(local) = s.strip_suffix(['Z', 'z']) {
        return Some((local, 0));
    }
    let at = s.len().checked_sub(6)?;
    let (local, offset) = (s.get(..at)?, s.get(at..)?);
    let &[sign, h1, h2, b':', m1, m2] = offset.as_bytes() else {
        return None;
    };
    let digit = |b: u8| b.is_ascii_digit().then(|| i64::from(b - b'0'));
    let hours = digit(h1)? * 10 + digit(h2)?;
    let minutes = digit(m1)? * 10 + digit(m2)?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    let magnitude = hours * 3600 + minutes * 60;
    match sign {
        b'+' => Some((local, magnitude)),
        b'-' => Some((local, -magnitude)),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// 2026-01-02T03:04:05Z in nanoseconds since the Unix epoch.
    const T_UTC_NS: i64 = 1_767_323_045_000_000_000;

    /// A UTC timestamp parses exactly, in each UTC spelling RFC 3339 allows.
    #[test]
    fn parse_rfc3339_ns_reads_utc_exactly() {
        for utc in [
            "2026-01-02T03:04:05Z",
            "2026-01-02T03:04:05z",
            "2026-01-02T03:04:05+00:00",
            "2026-01-02T03:04:05-00:00",
        ] {
            assert_eq!(parse_rfc3339_ns(utc), Ok(T_UTC_NS), "{utc}");
        }
        assert_eq!(
            parse_rfc3339_ns("2026-01-02T03:04:05.000000123Z"),
            Ok(T_UTC_NS + 123)
        );
    }

    /// A numeric offset is converted to UTC: `05:04:05+02:00` and
    /// `22:34:05-04:30` of the previous day are both `03:04:05Z`, and the
    /// fractional seconds survive the conversion.
    #[test]
    fn parse_rfc3339_ns_converts_a_numeric_offset_to_utc() {
        assert_eq!(parse_rfc3339_ns("2026-01-02T05:04:05+02:00"), Ok(T_UTC_NS));
        assert_eq!(parse_rfc3339_ns("2026-01-01T22:34:05-04:30"), Ok(T_UTC_NS));
        assert_eq!(
            parse_rfc3339_ns("2026-01-02T05:04:05.5+02:00"),
            Ok(T_UTC_NS + 500_000_000)
        );
    }

    /// A missing or malformed offset, an out-of-range offset, and an instant
    /// that lands before the Unix epoch once converted are each a typed `Err`.
    #[test]
    fn parse_rfc3339_ns_rejects_bad_offsets_and_pre_epoch_instants() {
        for bad in [
            "2026-01-02T03:04:05",
            "2026-01-02T03:04:05+0200",
            "2026-01-02T03:04:05+24:00",
            "2026-01-02T03:04:05+02:60",
            "2026-01-02T03:04:05*02:00",
            "2026-01-02T03:04:05+2:00",
            "",
            "Z",
        ] {
            let err = parse_rfc3339_ns(bad).expect_err(bad);
            assert!(err.contains("invalid RFC 3339"), "{bad}: {err}");
        }
        let err = parse_rfc3339_ns("1970-01-01T00:30:00+01:00").expect_err("pre-epoch");
        assert!(err.contains("before the Unix epoch"), "{err}");
    }

    /// `--max-flush-delay` parses humantime and returns an exact `Duration`
    /// (issue #801, deliverable 2's parse half).
    #[test]
    fn parse_max_flush_delay_reads_humantime_exactly() {
        assert_eq!(
            parse_max_flush_delay("10m").expect("10m"),
            Duration::from_secs(600)
        );
        assert_eq!(
            parse_max_flush_delay("2s").expect("2s"),
            Duration::from_secs(2)
        );
        assert_eq!(
            parse_max_flush_delay("1h5m").expect("1h5m"),
            Duration::from_secs(3600 + 300)
        );
    }

    /// Zero is accepted with a defined meaning, mirroring the sibling
    /// `--max-flush-lifetime` flags rather than being refused (issue #801,
    /// deliverable 4). Negative values are unrepresentable in humantime, so the
    /// grammar cannot express one; a garbage spelling is a typed `Err`, never a
    /// panic.
    #[test]
    fn parse_max_flush_delay_accepts_zero_and_rejects_garbage() {
        assert_eq!(parse_max_flush_delay("0s").expect("0s"), Duration::ZERO);
        let err = parse_max_flush_delay("later").expect_err("garbage is rejected");
        assert!(
            err.contains("--max-flush-delay"),
            "the error names the flag: {err}"
        );
    }

    /// A duration past the `i64`-nanosecond ceiling is rejected, exactly as
    /// [`parse_max_flush_lifetime_ns`] rejects it. The shard's age check casts
    /// `max_flush_delay.as_nanos() as i64`, so an accepted 1000-year delay
    /// would reach it as a negative threshold and age-flush every buffer on
    /// every tick: the parse rejection is what keeps the largest expressible
    /// value from behaving as the smallest.
    ///
    /// Prove-the-test: drop the `i64::try_from` line from
    /// `parse_max_flush_delay` and this fails at `expect_err` with
    /// `Ok(31557600000s)`.
    #[test]
    fn parse_max_flush_delay_rejects_a_value_past_the_i64_nanosecond_ceiling() {
        // 1000 humantime years is 3.15e19 ns, past i64::MAX (9.22e18), and
        // well inside what `Duration` itself can hold.
        let err = parse_max_flush_delay("1000years").expect_err("1000 years is rejected");
        assert!(
            err.contains("--max-flush-delay") && err.contains("too large"),
            "the error names the flag and the reason: {err}"
        );
        // The sibling flag rejects the same spelling for the same reason.
        assert!(parse_max_flush_lifetime_ns("1000years").is_err());
        // The largest value that still fits is accepted, so the guard rejects
        // only what the cast would corrupt.
        let ok = parse_max_flush_delay("292years").expect("292 years still fits in i64 ns");
        assert!(i64::try_from(ok.as_nanos()).is_ok());
        // Boundary: the ceiling leaves room for the strict-visibility reserve,
        // so the derived budget is strictly greater than any accepted delay
        // rather than saturating equal to it at the edge.
        let ceiling = i64::MAX - ravel_ingest::STRICT_VISIBILITY_RESERVE_NS;
        let at = std::time::Duration::from_nanos(ceiling as u64);
        assert!(
            parse_max_flush_delay(&format!("{}ns", at.as_nanos())).is_ok(),
            "the largest accepted delay sits exactly at i64::MAX minus the reserve"
        );
        assert!(
            parse_max_flush_delay(&format!("{}ns", at.as_nanos() + 1)).is_err(),
            "one nanosecond past the ceiling refuses"
        );
    }
}
