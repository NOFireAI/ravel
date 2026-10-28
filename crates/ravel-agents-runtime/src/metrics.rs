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
    /// for `Takeover`, `BeginTurn`, `ScheduleTimer` and `CancelTimer`.
    /// Counted once the settle succeeds, not
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
    pub(crate) timers_fired: AtomicU64,
    pub(crate) sweep_passes: AtomicU64,
    pub(crate) sweep_wake_list_pages: AtomicU64,
    pub(crate) sweep_dir_list_pages: AtomicU64,
    pub(crate) sweep_head_gets: AtomicU64,
    pub(crate) sweep_hints_read: AtomicU64,
    pub(crate) sweep_hints_deleted: AtomicU64,
    pub(crate) sweep_agents_loaded: AtomicU64,
    pub(crate) sweep_dir_entries_repaired: AtomicU64,
    pub(crate) sweep_directory_cycles: AtomicU64,
    pub(crate) sweep_last_directory_cycle_ms: AtomicU64,
}

/// The timer and sweep counters, summed over every pass since the
/// coordinator started: the [`crate::SweepReport`] fields plus the pass
/// count and the directory cycles. `ravel-server` renders them on
/// `/metrics`, the LISTs under a `phase` label of `wake` or `dir`. Kept
/// apart from [`AgentsMetricsSnapshot`], which `ravel-server` builds field
/// by field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepMetricsSnapshot {
    /// Passes run, from polls and from `sweep_once`, including failed ones.
    pub passes: u64,
    /// LISTs the passes issued over `_wake/`.
    pub wake_list_pages: u64,
    /// LISTs the passes issued over `_dir/`.
    pub dir_list_pages: u64,
    /// HEAD reads the passes issued themselves.
    pub head_gets: u64,
    /// Due hints acted on.
    pub hints_read: u64,
    /// Hints deleted after HEAD showed their timer settled.
    pub hints_deleted: u64,
    /// `TimerFired` replies published by a `FireTimer` CAS.
    pub timers_fired: u64,
    /// Agents with work a pass added to the dispatch set.
    pub agents_loaded: u64,
    /// Missing `_dir` entries a pass wrote back.
    pub dir_entries_repaired: u64,
    /// Directory cycles completed: the directory half walked `_dir/` from
    /// its first entry to the end of the listing, reading every agent's
    /// HEAD once, over one pass or several.
    pub directory_cycles: u64,
    /// Milliseconds the last completed directory cycle took on the
    /// coordinator's clock, from the start of the pass that began it to the
    /// end of the listing; 0 before the first.
    pub last_directory_cycle_ms: u64,
}

impl Coordinator {
    /// The timer and sweep counters.
    pub fn sweep_metrics(&self) -> SweepMetricsSnapshot {
        let m = &self.metrics;
        SweepMetricsSnapshot {
            passes: m.sweep_passes.load(Ordering::Relaxed),
            wake_list_pages: m.sweep_wake_list_pages.load(Ordering::Relaxed),
            dir_list_pages: m.sweep_dir_list_pages.load(Ordering::Relaxed),
            head_gets: m.sweep_head_gets.load(Ordering::Relaxed),
            hints_read: m.sweep_hints_read.load(Ordering::Relaxed),
            hints_deleted: m.sweep_hints_deleted.load(Ordering::Relaxed),
            timers_fired: m.timers_fired.load(Ordering::Relaxed),
            agents_loaded: m.sweep_agents_loaded.load(Ordering::Relaxed),
            dir_entries_repaired: m.sweep_dir_entries_repaired.load(Ordering::Relaxed),
            directory_cycles: m.sweep_directory_cycles.load(Ordering::Relaxed),
            last_directory_cycle_ms: m.sweep_last_directory_cycle_ms.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_directory_cycle(&self, duration_ms: u64) {
        let m = &self.metrics;
        m.sweep_last_directory_cycle_ms
            .store(duration_ms, Ordering::Relaxed);
        m.sweep_directory_cycles.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_sweep(&self, report: &crate::SweepReport) {
        let m = &self.metrics;
        m.sweep_passes.fetch_add(1, Ordering::Relaxed);
        m.sweep_wake_list_pages
            .fetch_add(report.wake_list_pages, Ordering::Relaxed);
        m.sweep_dir_list_pages
            .fetch_add(report.dir_list_pages, Ordering::Relaxed);
        m.sweep_head_gets
            .fetch_add(report.head_gets, Ordering::Relaxed);
        m.sweep_hints_read
            .fetch_add(report.hints_read, Ordering::Relaxed);
        m.sweep_hints_deleted
            .fetch_add(report.hints_deleted, Ordering::Relaxed);
        m.sweep_agents_loaded
            .fetch_add(report.agents_loaded, Ordering::Relaxed);
        m.sweep_dir_entries_repaired
            .fetch_add(report.dir_entries_repaired, Ordering::Relaxed);
    }

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
