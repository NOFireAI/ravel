//! Coordinator counters, read through [`Coordinator::metrics`].

use std::sync::atomic::{AtomicU64, Ordering};

use ravel_agents_store::counters::CounterSnapshot;

use crate::Coordinator;

/// A point-in-time copy of the coordinator's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentsMetricsSnapshot {
    /// Agents whose root is loaded in memory.
    pub agents_loaded: u64,
    /// Loaded agents whose root records an active turn.
    pub turns_active: u64,
    /// Completions published, including a lost acknowledgement confirmed
    /// through `resolve`. A commit whose acknowledgement was lost and whose
    /// agent was taken over before the settle is answered `OwnershipLost`
    /// and is not counted here, although it landed (see `failure.rs`).
    pub turns_committed: u64,
    /// `FailTurn` publications, retryable or final.
    pub turns_failed: u64,
    /// Inputs dead-lettered after their last attempt.
    pub turns_dead_lettered: u64,
    /// Published `Takeover` commands.
    pub takeovers: u64,
    /// Commits, failures, heartbeats and activations refused because the
    /// caller's ownership moved on.
    pub ownership_lost_refusals: u64,
    /// Polls and commands refused for a handler pin mismatch.
    pub incompatible_handler_refusals: u64,
    /// `OutcomeUnknown` errors settled into this caller's answer: through
    /// `AgentStore::resolve` for an input-scoped command, by re-reading HEAD
    /// for `Takeover` and `BeginTurn`. Counted once the settle succeeds, not
    /// when the error is seen; a settle that ends in a refusal or another
    /// error is not counted.
    pub outcome_unknown_resolved: u64,
    /// Polls that found no work at first and waited.
    pub polls_waited: u64,
    /// The store's per-phase request counters.
    pub store: CounterSnapshot,
}

#[derive(Debug, Default)]
pub(crate) struct Metrics {
    pub(crate) turns_committed: AtomicU64,
    pub(crate) turns_failed: AtomicU64,
    pub(crate) turns_dead_lettered: AtomicU64,
    pub(crate) takeovers: AtomicU64,
    pub(crate) ownership_lost_refusals: AtomicU64,
    pub(crate) incompatible_handler_refusals: AtomicU64,
    pub(crate) outcome_unknown_resolved: AtomicU64,
    pub(crate) polls_waited: AtomicU64,
}

impl Coordinator {
    pub(crate) fn metrics_snapshot(&self) -> AgentsMetricsSnapshot {
        let (agents_loaded, turns_active) = {
            let agents = self.agents.lock();
            let active = agents
                .values()
                .filter(|l| l.head.active_turn.is_some())
                .count();
            (agents.len() as u64, active as u64)
        };
        let m = &self.metrics;
        AgentsMetricsSnapshot {
            agents_loaded,
            turns_active,
            turns_committed: m.turns_committed.load(Ordering::Relaxed),
            turns_failed: m.turns_failed.load(Ordering::Relaxed),
            turns_dead_lettered: m.turns_dead_lettered.load(Ordering::Relaxed),
            takeovers: m.takeovers.load(Ordering::Relaxed),
            ownership_lost_refusals: m.ownership_lost_refusals.load(Ordering::Relaxed),
            incompatible_handler_refusals: m.incompatible_handler_refusals.load(Ordering::Relaxed),
            outcome_unknown_resolved: m.outcome_unknown_resolved.load(Ordering::Relaxed),
            polls_waited: m.polls_waited.load(Ordering::Relaxed),
            store: self.store.counters(),
        }
    }
}
