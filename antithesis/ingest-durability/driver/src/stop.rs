//! The flags that end a timeline: faults stopping, the forced eventually's
//! request, and the one eventually allowed to start (#4502).
//!
//! Once faults stop, no rule may return: a returning rule lets the Hegel
//! engine schedule rounds with nothing to do, recording every no-op step
//! until the driver runs out of memory. Every rule therefore halts its
//! thread for good, and the process ends through the eventually's exit.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct Stop {
    stopping: AtomicBool,
    force: AtomicBool,
    started: AtomicBool,
}

impl Stop {
    pub fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Parks the calling thread for good once faults stopped; returns at
    /// once otherwise.
    pub fn halt_if_stopping(&self) {
        if self.stopping() {
            halt();
        }
    }

    /// Asks the next rule to run the forced eventually.
    pub fn request_forced(&self) {
        self.force.store(true, Ordering::SeqCst);
    }

    pub fn forced(&self) -> bool {
        self.force.load(Ordering::SeqCst)
    }

    /// Stops faults and claims the eventually. Exactly one caller gets
    /// `true`; every other one sees `stopping` already set when it halts.
    pub fn begin_eventually(&self) -> bool {
        self.stopping.store(true, Ordering::SeqCst);
        self.started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Whether some thread started an eventually within `limit`, polled every
    /// `every`.
    pub fn wait_started(&self, limit: Duration, every: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while !self.started() {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(every);
        }
        true
    }
}

/// Parks the calling thread forever.
pub fn halt() -> ! {
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn halt_if_stopping_never_returns_once_stopping() {
        let stop = Arc::new(Stop::default());
        stop.stopping.store(true, Ordering::SeqCst);
        let s = Arc::clone(&stop);
        let worker = std::thread::spawn(move || s.halt_if_stopping());
        std::thread::sleep(Duration::from_millis(500));
        // A spurious unpark must not let it out either.
        worker.thread().unpark();
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !worker.is_finished(),
            "a rule returned after faults stopped"
        );
    }

    #[test]
    fn halt_if_stopping_returns_at_once_when_unset() {
        let stop = Arc::new(Stop::default());
        let s = Arc::clone(&stop);
        let worker = std::thread::spawn(move || s.halt_if_stopping());
        let deadline = Instant::now() + Duration::from_secs(5);
        while !worker.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(worker.is_finished());
        assert!(!stop.stopping());
    }

    #[test]
    fn exactly_one_of_two_concurrent_eventuallies_starts() {
        for _ in 0..200 {
            let stop = Arc::new(Stop::default());
            stop.request_forced();
            let barrier = Arc::new(Barrier::new(2));
            let callers: Vec<_> = (0..2)
                .map(|_| {
                    let s = Arc::clone(&stop);
                    let b = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        b.wait();
                        assert!(s.forced());
                        let won = s.begin_eventually();
                        assert!(s.stopping());
                        won
                    })
                })
                .collect();
            let won: Vec<bool> = callers
                .into_iter()
                .map(|c| c.join().unwrap_or(false))
                .collect();
            assert_eq!(won.iter().filter(|w| **w).count(), 1, "{won:?}");
            assert!(stop.started());
        }
    }

    #[test]
    fn forced_flag_starts_nothing_by_itself() {
        let stop = Stop::default();
        assert!(!stop.forced());
        stop.request_forced();
        assert!(stop.forced());
        assert!(!stop.stopping());
        assert!(!stop.started());
        assert!(!stop.wait_started(Duration::from_millis(50), Duration::from_millis(10)));
        assert!(stop.begin_eventually());
        assert!(stop.wait_started(Duration::from_millis(50), Duration::from_millis(10)));
        assert!(!stop.begin_eventually());
    }

    /// Every rule of both machines starts with the helper, so none returns
    /// once faults stopped.
    #[test]
    fn every_rule_starts_with_the_helper() {
        let src = include_str!("main.rs");
        let rules = rule_first_statements(src);
        assert!(rules.len() >= 11, "found only {} rules", rules.len());
        for (name, first) in &rules {
            assert_eq!(
                first, "self.0.halt_if_stopping();",
                "rule {name} does not start with the helper"
            );
        }
    }

    #[test]
    fn the_source_check_catches_a_rule_without_the_helper() {
        let src = "impl W {\n    #[rule(groups = [\"main\"])]\n    fn a(&self, tc: TestCase) {\n        self.0.halt_if_stopping();\n    }\n\n    #[rule(groups = [\"main\"], weight = 2)]\n    fn b(&self, tc: TestCase) {\n        self.0.write_logs(&tc);\n    }\n}\n";
        let rules = rule_first_statements(src);
        assert_eq!(
            rules,
            vec![
                ("a".to_string(), "self.0.halt_if_stopping();".to_string()),
                ("b".to_string(), "self.0.write_logs(&tc);".to_string()),
            ]
        );
    }

    /// Each `#[rule(` attribute's function name and the first line of its
    /// body.
    fn rule_first_statements(src: &str) -> Vec<(String, String)> {
        let lines: Vec<&str> = src.lines().map(str::trim).collect();
        let mut out = Vec::new();
        for (i, line) in lines.iter().enumerate() {
            if !line.starts_with("#[rule(") {
                continue;
            }
            let sig = lines.get(i + 1).copied().unwrap_or("");
            let name = sig
                .strip_prefix("fn ")
                .and_then(|r| r.split('(').next())
                .unwrap_or("?")
                .to_string();
            assert!(sig.ends_with('{'), "rule signature on one line: {sig}");
            let first = lines[i + 2..]
                .iter()
                .find(|l| !l.starts_with("//"))
                .copied()
                .unwrap_or("")
                .to_string();
            out.push((name, first));
        }
        out
    }
}
