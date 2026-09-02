//! Library surface behind the `ravel-cli` binary, split out so integration
//! tests can drive the catalog subcommands in-process against a shared
//! `MemoryStore` (a subprocess per invocation, as `tests/segment_inspect.rs`
//! uses, would give each `ravel-cli catalog ...` call its own empty
//! in-memory store, making a chained fold -> inspect -> verify scenario
//! impossible to construct without a persistent S3/MinIO backend).

pub mod catalog;
pub mod cli_profiling;
pub mod erase;
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
/// from "never age out" would otherwise parse as its exact opposite. Negative
/// values are unrepresentable in humantime, so the only rejections are an
/// unparseable spelling and a value too large for `i64` nanoseconds.
pub fn parse_max_flush_delay(s: &str) -> Result<std::time::Duration, String> {
    let dur = humantime::parse_duration(s)
        .map_err(|e| format!("invalid --max-flush-delay '{s}': {e}"))?;
    i64::try_from(dur.as_nanos()).map_err(|_| format!("--max-flush-delay '{s}' is too large"))?;
    Ok(dur)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::time::Duration;

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
    }
}
