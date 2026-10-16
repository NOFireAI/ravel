//! The wall-clock implementation of `ravel_pqtable::clock::Clock`.
//!
//! `ravel-pqtable` stays clock-free by design (its own `clock` module doc:
//! "the wall-clock implementation lives in the service layer"); `ravel-sql`
//! is that service layer for `SqlExecutor::execute_ddl`, the only caller that
//! threads a `Clock` into `ravel_pqtable::writer::apply`.

use ravel_pqtable::clock::Clock;

/// Production clock backed by the OS wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_reads_a_plausible_unix_time() {
        // Bounded sanity check, not a pin: `now_ns` is real wall-clock time,
        // so the exact value is never asserted anywhere. 2026-01-01 and
        // 2100-01-01 in unix nanoseconds bound every date this test will ever
        // run on.
        let now = SystemClock.now_ns();
        assert!(now > 1_767_225_600_000_000_000);
        assert!(now < 4_102_444_800_000_000_000);
    }
}
