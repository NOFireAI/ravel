//! Idle unloading (ADR-3331 decision 6).
//!
//! Unloading drops an agent's loaded root and lease-table entry and nothing
//! else: no object is written and HEAD is untouched
//! (`tests::idle_unload_writes_nothing_and_send_reloads_from_head`). The next
//! send loads the agent from HEAD; the next poll rescans the agent directory
//! and loads every agent that has work.

use std::sync::atomic::Ordering;

use crate::Coordinator;
use crate::ownership::LeaseTable;

impl Coordinator {
    /// Drop every loaded agent with no waiting input and no active turn that
    /// has been untouched for `idle_unload_after_ms`. Returns how many were
    /// dropped.
    pub fn unload_idle(&self) -> usize {
        let now = self.now();
        let idle_ns =
            i64::try_from(self.config.idle_unload_after_ms.saturating_mul(1_000_000))
                .unwrap_or(i64::MAX);
        let dropped: Vec<_> = {
            let mut agents = self.agents.lock();
            let idle: Vec<_> = agents
                .iter()
                .filter(|(_, l)| {
                    !l.dispatching
                        && l.head.active_turn.is_none()
                        && l.head.inbox.is_empty()
                        && now.saturating_sub(l.touched_unix_ns) >= idle_ns
                })
                .map(|(a, _)| *a)
                .collect();
            for agent in &idle {
                agents.remove(agent);
            }
            idle
        };
        forget_all(&self.leases, &dropped);
        if !dropped.is_empty() {
            self.discovered.store(false, Ordering::Release);
        }
        dropped.len()
    }
}

fn forget_all(leases: &LeaseTable, agents: &[ravel_agents_core::identity::AgentId]) {
    for agent in agents {
        leases.forget(agent);
    }
}
