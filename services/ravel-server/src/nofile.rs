//! The process's open-file limit, raised at startup (ADR-3565, bucket spill
//! item 1).
//!
//! A bucketed spill retry holds two descriptors per bucket file, and the
//! bucket spill derivation returns no tuning when that need exceeds a share of
//! the soft `RLIMIT_NOFILE`. Service managers and container runtimes commonly
//! leave the soft limit at 1,024 under a far higher hard limit, so the server
//! raises its soft limit to the hard limit before anything reads it, and logs
//! both. After the raise the soft limit is 1,024 only where the hard limit is
//! too.

use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

/// The soft and hard `RLIMIT_NOFILE` after the startup raise. `None` is
/// `RLIM_INFINITY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NofileLimit {
    pub soft: Option<u64>,
    pub hard: Option<u64>,
    /// `raised` when the raise moved the soft limit, `inherited` when it was
    /// already at the hard limit or the hard limit is unbounded, and
    /// `raise-refused` when the kernel refused the raise.
    pub source: &'static str,
}

/// The soft limit to ask for: the hard limit when it is finite and above the
/// current soft limit. An unbounded hard limit is left alone, since Linux
/// refuses an unbounded soft `RLIMIT_NOFILE` past `fs.nr_open`.
fn raised_soft(soft: Option<u64>, hard: Option<u64>) -> Option<u64> {
    match (soft, hard) {
        (Some(soft), Some(hard)) if hard > soft => Some(hard),
        _ => None,
    }
}

/// Raise the soft `RLIMIT_NOFILE` to the hard limit and return the limits in
/// force afterwards. Called once, at startup, before anything reads the
/// limit.
pub fn raise_nofile_soft_limit() -> NofileLimit {
    let before = getrlimit(Resource::Nofile);
    let Some(target) = raised_soft(before.current, before.maximum) else {
        return NofileLimit {
            soft: before.current,
            hard: before.maximum,
            source: "inherited",
        };
    };
    let raised = Rlimit {
        current: Some(target),
        maximum: before.maximum,
    };
    match setrlimit(Resource::Nofile, raised) {
        Ok(()) => NofileLimit {
            soft: Some(target),
            hard: before.maximum,
            source: "raised",
        },
        Err(_) => NofileLimit {
            soft: before.current,
            hard: before.maximum,
            source: "raise-refused",
        },
    }
}

impl NofileLimit {
    /// The soft limit as the bucket spill derivation reads it, `u64::MAX`
    /// for unbounded.
    pub fn soft_or_max(&self) -> u64 {
        self.soft.unwrap_or(u64::MAX)
    }

    /// `(setting, value, source)` for each logged line: the soft limit with
    /// how it was reached, and the hard limit as inherited.
    fn lines(&self) -> [(&'static str, String, &'static str); 2] {
        let render =
            |limit: Option<u64>| limit.map_or_else(|| "unlimited".to_string(), |v| v.to_string());
        [
            ("nofile_soft", render(self.soft), self.source),
            ("nofile_hard", render(self.hard), "inherited"),
        ]
    }

    /// Log both limits, one line each, in the startup resolution form.
    pub fn emit(&self) {
        for (setting, value, source) in self.lines() {
            tracing::info!(setting, value, source, "open file limit resolved");
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn the_soft_limit_rises_to_a_finite_hard_limit_only() {
        assert_eq!(raised_soft(Some(1024), Some(524_288)), Some(524_288));
        assert_eq!(raised_soft(Some(524_288), Some(524_288)), None);
        assert_eq!(raised_soft(Some(1024), None), None);
        assert_eq!(raised_soft(None, None), None);
    }

    /// The raise leaves this process at its hard limit whenever that limit is
    /// finite, and reports what it did.
    #[test]
    fn startup_raises_the_soft_nofile_limit_to_the_hard_limit() {
        let limit = raise_nofile_soft_limit();
        let now = getrlimit(Resource::Nofile);
        assert_eq!(limit.soft, now.current, "{limit:?}");
        assert_eq!(limit.hard, now.maximum, "{limit:?}");
        if now.maximum.is_some() {
            assert_eq!(now.current, now.maximum, "{limit:?}");
        }
        assert!(["raised", "inherited"].contains(&limit.source), "{limit:?}");
        let again = raise_nofile_soft_limit();
        assert_eq!(again.source, "inherited", "a second raise moved {again:?}");
    }

    #[test]
    fn the_limit_logs_both_values_with_their_sources() {
        let limit = NofileLimit {
            soft: Some(524_288),
            hard: Some(524_288),
            source: "raised",
        };
        assert_eq!(
            limit.lines(),
            [
                ("nofile_soft", "524288".to_string(), "raised"),
                ("nofile_hard", "524288".to_string(), "inherited"),
            ]
        );
        let unbounded = NofileLimit {
            soft: Some(1024),
            hard: None,
            source: "inherited",
        };
        assert_eq!(unbounded.lines()[1].1, "unlimited");
        assert_eq!(unbounded.soft_or_max(), 1024);
    }
}
