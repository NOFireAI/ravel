//! Injected wall clock. Library logic never reads `SystemTime::now()`
//! (CLAUDE.md testing patterns): every timestamp the compactor stamps or
//! compares comes through a [`Clock`], so tests are fully deterministic. The
//! wall-clock implementation lives in the service layer (phase 8), keeping
//! this crate clock-free.

/// A monotone-enough source of unix-epoch nanoseconds. Only used for the
/// compaction record's `created_unix_ns` (the supersession-horizon anchor,
/// plan §5) and the `max_compaction_lifetime` abandonment deadline
/// (plan §3.4 point 4). Never used to order commits (that is the pinned
/// flush identity's job).
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

    /// Move the clock; lets one test drive the abandonment deadline without
    /// rebuilding the compactor.
    pub fn set(&self, now_ns: i64) {
        self.now_ns
            .store(now_ns, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.load(std::sync::atomic::Ordering::SeqCst)
    }
}
