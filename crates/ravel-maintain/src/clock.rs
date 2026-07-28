//! Injected time source. No library logic in this crate reads the system
//! clock directly (repo-wide rule); every timestamp used for the seal rule,
//! the abandonment deadline, and record `created_unix_ns` comes through a
//! [`Clock`] the caller supplies, so tests are deterministic.

/// A source of the current unix time in nanoseconds.
pub trait Clock: Send + Sync {
    fn now_ns(&self) -> i64;
}

/// The real wall clock, for production callers (service wiring, out of
/// scope for this crate). Never used by this crate's own logic.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        let elapsed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct FakeClock(std::sync::Arc<std::sync::atomic::AtomicI64>);

#[cfg(test)]
impl FakeClock {
    pub fn new(now_ns: i64) -> Self {
        FakeClock(std::sync::Arc::new(std::sync::atomic::AtomicI64::new(
            now_ns,
        )))
    }

    pub fn advance(&self, delta_ns: i64) {
        self.0
            .fetch_add(delta_ns, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn set(&self, now_ns: i64) {
        self.0.store(now_ns, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
impl Clock for FakeClock {
    fn now_ns(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_clock_starts_at_given_value() {
        let clock = FakeClock::new(42);
        assert_eq!(clock.now_ns(), 42);
    }

    #[test]
    fn fake_clock_advances() {
        let clock = FakeClock::new(100);
        clock.advance(50);
        assert_eq!(clock.now_ns(), 150);
        clock.advance(-30);
        assert_eq!(clock.now_ns(), 120);
    }

    #[test]
    fn fake_clock_set_overrides_value() {
        let clock = FakeClock::new(0);
        clock.set(999);
        assert_eq!(clock.now_ns(), 999);
    }

    #[test]
    fn fake_clock_clone_shares_state() {
        let clock = FakeClock::new(0);
        let handle = clock.clone();
        handle.advance(10);
        assert_eq!(clock.now_ns(), 10);
    }
}
