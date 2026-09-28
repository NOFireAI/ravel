//! Injected wall clock. Library logic never reads `SystemTime::now()`
//! (CLAUDE.md testing patterns): the timestamp a manifest version records and
//! the resolve-to-put deadline [`crate::writer::apply`] enforces both come
//! through a [`Clock`], so tests drive them exactly. The wall-clock
//! implementation lives in the service layer, keeping this crate clock-free.

/// A source of unix-epoch nanoseconds. Used for a manifest's
/// `created_unix_ns`, a grant's, and the elapsed time between a writer's
/// resolve and its put. Never used to order versions: that is the manifest
/// version number's job.
pub trait Clock: Send + Sync {
    fn now_ns(&self) -> i64;
}

impl<C: Clock + ?Sized> Clock for &C {
    fn now_ns(&self) -> i64 {
        (**self).now_ns()
    }
}

/// A fixed clock for tests: `now_ns` returns a caller-set constant.
#[derive(Debug, Clone)]
pub struct FixedClock {
    now_ns: std::sync::Arc<std::sync::atomic::AtomicI64>,
}

impl FixedClock {
    pub fn new(now_ns: i64) -> Self {
        FixedClock {
            now_ns: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(now_ns)),
        }
    }

    /// Move the clock, so one test can advance time between a writer's
    /// resolve and its put.
    pub fn set(&self, now_ns: i64) {
        self.now_ns
            .store(now_ns, std::sync::atomic::Ordering::SeqCst);
    }

    /// Move the clock forward by `delta_ns`, returning the new value.
    pub fn advance(&self, delta_ns: i64) -> i64 {
        self.now_ns
            .fetch_add(delta_ns, std::sync::atomic::Ordering::SeqCst)
            .saturating_add(delta_ns)
    }
}

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.load(std::sync::atomic::Ordering::SeqCst)
    }
}
