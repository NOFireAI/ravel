//! Idle unloading (ADR-3331 decision 6).
//!
//! Unloading drops an agent's loaded root and lease-table entry and nothing
//! else: no object is written and HEAD is untouched
//! (`tests::idle_unload_writes_nothing_and_send_reloads_from_head`). The next
//! send loads the agent from HEAD. Only an agent with no work is dropped, so
//! nothing needs rediscovering; work that reaches it later through another
//! coordinator is found by the sweep. A root whose lifecycle is not `Active`
//! is dropped whatever its inbox and active turn hold: a destroy abandoned
//! them and no turn can begin on them
//! (`tests::a_coordinator_that_did_not_serve_the_destroy_stops_offering_the_agent_after_one_refused_dispatch`).

use ravel_agents_core::head::Lifecycle;

use crate::Coordinator;
use crate::ownership::LeaseTable;

impl Coordinator {
    /// Drop every loaded agent with no waiting input and no active turn, or
    /// whose lifecycle is not `Active`, that has been untouched for
    /// `idle_unload_after_ms`. Returns how many were dropped.
    pub fn unload_idle(&self) -> usize {
        let now = self.now();
        let idle_ns = i64::try_from(self.config.idle_unload_after_ms.saturating_mul(1_000_000))
            .unwrap_or(i64::MAX);
        let dropped: Vec<_> = {
            let mut agents = self.agents.lock();
            let idle: Vec<_> = agents
                .iter()
                .filter(|(_, l)| {
                    !l.dispatching
                        && (l.head.lifecycle != Lifecycle::Active
                            || (l.head.active_turn.is_none() && !l.head.has_inbox()))
                        && now.saturating_sub(l.touched_unix_ns) >= idle_ns
                })
                .map(|(a, _)| *a)
                .collect();
            for agent in &idle {
                agents.remove(agent);
            }
            idle
        };
        if !dropped.is_empty() {
            self.open_attempts
                .lock()
                .retain(|(a, _), _| !dropped.contains(a));
        }
        forget_all(&self.leases, &dropped);
        dropped.len()
    }
}

fn forget_all(leases: &LeaseTable, agents: &[ravel_agents_core::identity::AgentId]) {
    for agent in agents {
        leases.forget(agent);
    }
}
