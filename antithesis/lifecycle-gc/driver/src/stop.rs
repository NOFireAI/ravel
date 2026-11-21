//! The flags that end a timeline. Once faults stop, no rule may return: a
//! rule that returned would let the hegel engine schedule rounds with
//! nothing to do and record every no-op step until the driver runs out of
//! memory. So every rule parks its thread for good once `stopping` is set,
//! and the process ends through the eventually's exit. The forced
//! eventually has no thread of its own: the first rule that finds its
//! deadline passed runs it, inside its hegel worker.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub struct Stop {
    stopping: AtomicBool,
    started: AtomicBool,
    force_at: Option<Instant>,
}

impl Stop {
    pub fn new(force_after: Option<Duration>) -> Stop {
        Stop {
            stopping: AtomicBool::new(false),
            started: AtomicBool::new(false),
            force_at: force_after.map(|d| Instant::now() + d),
        }
    }

    pub fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    pub fn force_due(&self) -> bool {
        self.force_at.is_some_and(|t| Instant::now() >= t)
    }

    /// Sets `stopping` and claims the eventually: exactly one caller gets
    /// `true`.
    pub fn begin(&self) -> bool {
        self.stopping.store(true, Ordering::SeqCst);
        self.started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

/// Parks the calling thread forever.
pub fn halt() -> ! {
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn exactly_one_caller_begins_the_eventually() {
        let stop = Arc::new(Stop::new(None));
        let barrier = Arc::new(Barrier::new(8));
        let winners: usize = (0..8)
            .map(|_| {
                let (s, b) = (Arc::clone(&stop), Arc::clone(&barrier));
                std::thread::spawn(move || {
                    b.wait();
                    s.begin()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| usize::from(h.join().expect("join")))
            .sum();
        assert_eq!(winners, 1);
        assert!(stop.stopping());
    }

    #[test]
    fn the_force_deadline_is_relative_to_creation() {
        assert!(!Stop::new(None).force_due());
        assert!(!Stop::new(Some(Duration::from_secs(3600))).force_due());
        assert!(Stop::new(Some(Duration::ZERO)).force_due());
    }

    /// Every rule in main.rs starts by halting once faults stopped, so no
    /// rule returns after the eventually started.
    #[test]
    fn every_rule_starts_with_the_halt_check() {
        let src = include_str!("main.rs");
        let mut rules = 0;
        let mut lines = src.lines();
        while let Some(line) = lines.next() {
            if !line.trim_start().starts_with("#[rule(") {
                continue;
            }
            rules += 1;
            let sig = lines.next().expect("rule fn");
            let first = lines.next().expect("rule body");
            assert!(
                first.trim() == "self.0.halt_if_stopping();",
                "rule {sig} starts with {first:?}"
            );
        }
        assert!(rules >= 8, "found {rules} rules");
    }
}
