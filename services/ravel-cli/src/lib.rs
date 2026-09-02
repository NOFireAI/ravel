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
/// `--max-flush-lifetime` flags: `0s` means the age trigger fires on the next
/// flush tick for any non-empty buffer (its oldest point is always at least
/// `0s` old), so every buffer flushes by age almost immediately regardless of
/// `--target-bytes`. Negative values are unrepresentable in humantime, so the
/// only rejections are an unparseable spelling and a value too large for
/// `Duration`.
pub fn parse_max_flush_delay(s: &str) -> Result<std::time::Duration, String> {
    humantime::parse_duration(s).map_err(|e| format!("invalid --max-flush-delay '{s}': {e}"))
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
}
