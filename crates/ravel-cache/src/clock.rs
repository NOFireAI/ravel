//! Injected wall clock for disk-tier entry ageing (ADR-0064, issue #753).
//!
//! The disk tier stamps each entry's write time into its on-disk header and
//! refuses to serve an entry older than the configured max-age. That decision
//! needs the current wall time, but library logic must never call
//! `SystemTime::now()` directly (repo testing rule: time is injected so tests
//! are deterministic). A [`DiskCache`](crate::DiskCache) therefore reads the
//! clock only through this trait, and a test drives ageing across the max-age
//! boundary with a clock it advances by hand.

use std::time::{SystemTime, UNIX_EPOCH};

/// A source of wall-clock time in nanoseconds since the Unix epoch.
pub trait Clock: Send + Sync {
    /// Nanoseconds since the Unix epoch. Monotonicity is not required: the
    /// disk tier only ever compares two stamps by subtraction and treats a
    /// backward jump as "not yet expired", never as a panic.
    fn now_ns(&self) -> u64;
}

/// Production clock: wall time read from the operating system. This is the
/// single place `SystemTime::now()` is called in this crate's non-test code;
/// everything else takes a `&dyn Clock` so it can be driven deterministically.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> u64 {
        // A clock before the Unix epoch, or one so far in the future that the
        // nanosecond count overflows u64 (year 2554), both saturate rather
        // than error: the disk tier's only use of this value is an age
        // comparison, and saturating is a safe, bounded answer for both ends.
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(elapsed) => u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Err(_) => 0,
        }
    }
}
