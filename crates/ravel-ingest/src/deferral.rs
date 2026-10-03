//! The flush deferral cap state one shard actor shares with the router that
//! routes to it (ADR-1642 deferral cap amendment).
//!
//! The actor owns the deferral bookkeeping, but only the router sees a
//! buffered-mode write before it is acknowledged, so the actor publishes its
//! oldest deferral here and the router refuses a write to a shard at the cap
//! before enqueue, in both modes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use ravel_types::Signal;

/// Whether a deferral that began at `since_ns` has lasted at least `cap_ns`
/// at `now_ns`. The one comparison the router, the actor's append check and
/// its flush-open check all share, so the three cannot disagree on the
/// boundary.
pub(crate) fn deferral_cap_reached(since_ns: i64, now_ns: i64, cap_ns: i64) -> bool {
    now_ns.saturating_sub(since_ns) >= cap_ns
}

/// The `Abandoned` message a strict waiter gets when its buffer's deferral
/// reached the cap before the flush opened. The rows are still written, by the
/// flush that opens past the cap, so the message says the outcome is unknown
/// rather than that nothing was stored.
pub(crate) const DEFERRAL_CAP_ABANDONED: &str = "strict ack withheld: the flush stayed deferred \
     past the flush deferral cap; the rows are still written by the flush that opens after it";

/// Stored in place of a deferral start while no buffer on the shard is
/// deferred.
const NOT_DEFERRED: i64 = i64::MAX;

/// One shard's at-cap flag. The actor sets it to the start of its oldest
/// deferral whenever that changes and clears it once every deferred buffer has
/// opened; [`Self::reached`] reads it against the cap with the reader's own
/// clock reading, so the router sees the cap the instant it is reached rather
/// than at the actor's next tick.
#[derive(Debug, Clone)]
pub(crate) struct DeferralCapFlag {
    inner: Arc<FlagInner>,
}

#[derive(Debug)]
struct FlagInner {
    cap_ns: i64,
    oldest_deferral_ns: AtomicI64,
    /// Set by the first refusal of a cap episode, so that refusal alone is
    /// logged; cleared when the actor publishes a deferral that is no longer
    /// at the cap.
    episode_logged: AtomicBool,
}

impl DeferralCapFlag {
    pub(crate) fn new(cap_ns: i64) -> Self {
        DeferralCapFlag {
            inner: Arc::new(FlagInner {
                cap_ns,
                oldest_deferral_ns: AtomicI64::new(NOT_DEFERRED),
                episode_logged: AtomicBool::new(false),
            }),
        }
    }

    /// Publishes the actor's oldest deferral start, or clears the flag on
    /// `None`. A value that is not at the cap at `now_ns` ends the episode.
    pub(crate) fn publish(&self, oldest_deferral_ns: Option<i64>, now_ns: i64) {
        self.inner.oldest_deferral_ns.store(
            oldest_deferral_ns.unwrap_or(NOT_DEFERRED),
            Ordering::Release,
        );
        let at_cap = oldest_deferral_ns
            .is_some_and(|since| deferral_cap_reached(since, now_ns, self.inner.cap_ns));
        if !at_cap {
            self.inner.episode_logged.store(false, Ordering::Relaxed);
        }
    }

    /// A guard that clears the flag when dropped. The actor holds one for its
    /// whole run, so a return or a panic that ends it cannot leave its last
    /// deferral keeping the shard at the cap.
    pub(crate) fn clear_on_exit(&self) -> ClearOnExit {
        ClearOnExit(self.clone())
    }

    /// Whether the shard's oldest deferral has reached the cap at `now_ns`.
    pub(crate) fn reached(&self, now_ns: i64) -> bool {
        let since = self.inner.oldest_deferral_ns.load(Ordering::Acquire);
        since != NOT_DEFERRED && deferral_cap_reached(since, now_ns, self.inner.cap_ns)
    }

    /// Logs the first refusal of a cap episode at WARN; later refusals in the
    /// same episode are left to the refusal counter.
    pub(crate) fn note_refusal(&self, signal: Signal, shard: u32) {
        if !self.inner.episode_logged.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                signal = ?signal,
                shard,
                deferral_cap_ns = self.inner.cap_ns,
                "ingest shard reached the flush deferral cap; refusing new writes \
                 until its deferred flushes open"
            );
        }
    }
}

/// See [`DeferralCapFlag::clear_on_exit`].
pub(crate) struct ClearOnExit(DeferralCapFlag);

impl Drop for ClearOnExit {
    fn drop(&mut self) {
        self.0.publish(None, i64::MIN);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The flag is at the cap from exactly `since + cap`, one nanosecond
    /// earlier is not, and clearing it un-caps the shard whatever the clock.
    #[test]
    fn reached_is_inclusive_at_the_cap_and_cleared_by_none() {
        let flag = DeferralCapFlag::new(1_000);
        assert!(!flag.reached(i64::MAX - 1));
        flag.publish(Some(5_000), 5_000);
        assert!(!flag.reached(5_999));
        assert!(flag.reached(6_000));
        flag.publish(None, 6_000);
        assert!(!flag.reached(6_000));
    }

    /// Dropping the exit guard clears a deferral at the cap.
    #[test]
    fn the_exit_guard_clears_the_flag() {
        let flag = DeferralCapFlag::new(1_000);
        let guard = flag.clear_on_exit();
        flag.publish(Some(0), 0);
        assert!(flag.reached(1_000));
        drop(guard);
        assert!(!flag.reached(1_000));
    }

    /// One log line per episode: a second refusal while still at the cap is
    /// not logged, and a publish below the cap starts a fresh episode.
    #[test]
    fn an_episode_logs_once_and_resets_below_the_cap() {
        let flag = DeferralCapFlag::new(1_000);
        flag.publish(Some(0), 2_000);
        flag.note_refusal(Signal::Logs, 0);
        assert!(flag.inner.episode_logged.load(Ordering::Relaxed));
        flag.publish(Some(0), 3_000);
        assert!(
            flag.inner.episode_logged.load(Ordering::Relaxed),
            "still at the cap: the same episode"
        );
        flag.publish(Some(2_500), 3_000);
        assert!(
            !flag.inner.episode_logged.load(Ordering::Relaxed),
            "the oldest deferral is now below the cap, so the episode ended"
        );
    }
}
