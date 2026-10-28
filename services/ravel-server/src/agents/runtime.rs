//! The per-process agents runtime the routes act on: one
//! [`Coordinator`] per tenant over a tenant-scoped [`AgentStore`], built on
//! first use, plus the router's own admission permits.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use ravel_agents_core::kernel::Limits;
use ravel_agents_runtime::{AgentsMetricsSnapshot, Coordinator, RuntimeConfig, SystemRng};
use ravel_agents_store::counters::{CounterSnapshot, PhaseCounts};
use ravel_agents_store::{AgentStore, StoreLimits, SystemClock};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::{AgentsReadiness, AgentsSettings};

/// The long-poll cap of ADR-3331 decision 9, applied to `worker/poll`,
/// `result` and `call` whatever `wait_ms` a caller asks for.
pub const LONG_POLL_CAP_MS: u64 = ravel_agents_runtime::DEFAULT_POLL_WAIT_CAP_MS;

/// One tenant's coordinator and the store it publishes through.
pub struct TenantAgents {
    pub store: Arc<AgentStore>,
    pub coordinator: Coordinator,
    /// Fired after a commit or a cancel through this process, so a waiting
    /// `result` re-reads HEAD at once instead of at its next recheck.
    pub changed: Notify,
}

/// Which admission pool a route draws from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// `worker/poll`, `result` and `call`: requests that may hold a
    /// connection for up to the long-poll cap.
    Poll,
    /// Every other route.
    Activation,
}

/// The agents surface of one process.
pub struct AgentsRuntime {
    pub settings: AgentsSettings,
    readiness: Arc<AgentsReadiness>,
    object_store: Arc<dyn ObjectStoreBackend>,
    tenants: Mutex<HashMap<TenantHash, Arc<TenantAgents>>>,
    poll_permits: Arc<Semaphore>,
    activation_permits: Arc<Semaphore>,
}

impl AgentsRuntime {
    pub fn new(
        settings: AgentsSettings,
        readiness: Arc<AgentsReadiness>,
        object_store: Arc<dyn ObjectStoreBackend>,
    ) -> Self {
        AgentsRuntime {
            settings,
            readiness,
            object_store,
            tenants: Mutex::new(HashMap::new()),
            poll_permits: Arc::new(Semaphore::new(settings.poll_concurrency)),
            activation_permits: Arc::new(Semaphore::new(settings.activation_concurrency)),
        }
    }

    pub fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig {
            lease_ms: self.settings.lease_ms,
            heartbeat_grace_ms: self.settings.heartbeat_grace_ms,
            poll_wait_cap_ms: LONG_POLL_CAP_MS,
            ..RuntimeConfig::default()
        }
    }

    fn store_limits(&self) -> StoreLimits {
        StoreLimits {
            kernel: Limits {
                max_mailbox_depth: self.settings.max_mailbox,
                max_receipts: self.settings.max_receipts,
                max_timers: self.settings.max_timers,
                ..Limits::default()
            },
            ..StoreLimits::default()
        }
    }

    /// The tenant's coordinator, built on first use and kept for the life of
    /// the process.
    pub fn tenant(&self, tenant: TenantHash) -> Arc<TenantAgents> {
        let mut tenants = self.tenants.lock();
        tenants
            .entry(tenant)
            .or_insert_with(|| {
                let store = Arc::new(AgentStore::with_system_clock(
                    self.object_store.clone(),
                    tenant,
                    self.store_limits(),
                ));
                let coordinator = Coordinator::new(
                    store.clone(),
                    self.runtime_config(),
                    Arc::new(SystemClock),
                    Arc::new(SystemRng::new()),
                );
                Arc::new(TenantAgents {
                    store,
                    coordinator,
                    changed: Notify::new(),
                })
            })
            .clone()
    }

    /// A permit from the lane's pool, or `None` when the pool is exhausted.
    /// Never waits, and never draws from the query admission controller.
    pub fn admit(&self, lane: Lane) -> Option<OwnedSemaphorePermit> {
        let pool = match lane {
            Lane::Poll => &self.poll_permits,
            Lane::Activation => &self.activation_permits,
        };
        pool.clone().try_acquire_owned().ok()
    }

    /// True once the agents router is mounted in this process.
    pub fn enabled(&self) -> bool {
        self.readiness.is_mounted()
    }

    /// Every tenant coordinator's counters, summed.
    pub fn metrics(&self) -> AgentsMetricsSnapshot {
        let coordinators: Vec<Arc<TenantAgents>> = self.tenants.lock().values().cloned().collect();
        coordinators
            .iter()
            .map(|t| t.coordinator.metrics())
            .fold(empty_snapshot(), add_snapshots)
    }
}

fn empty_snapshot() -> AgentsMetricsSnapshot {
    AgentsMetricsSnapshot {
        agents_loaded: 0,
        turns_active: 0,
        turns_committed: 0,
        turns_failed: 0,
        turns_dead_lettered: 0,
        takeovers: 0,
        ownership_lost_refusals: 0,
        incompatible_handler_refusals: 0,
        outcome_unknown_resolved: 0,
        polls_waited: 0,
        store: CounterSnapshot::default(),
    }
}

fn add_snapshots(a: AgentsMetricsSnapshot, b: AgentsMetricsSnapshot) -> AgentsMetricsSnapshot {
    AgentsMetricsSnapshot {
        agents_loaded: a.agents_loaded + b.agents_loaded,
        turns_active: a.turns_active + b.turns_active,
        turns_committed: a.turns_committed + b.turns_committed,
        turns_failed: a.turns_failed + b.turns_failed,
        turns_dead_lettered: a.turns_dead_lettered + b.turns_dead_lettered,
        takeovers: a.takeovers + b.takeovers,
        ownership_lost_refusals: a.ownership_lost_refusals + b.ownership_lost_refusals,
        incompatible_handler_refusals: a.incompatible_handler_refusals
            + b.incompatible_handler_refusals,
        outcome_unknown_resolved: a.outcome_unknown_resolved + b.outcome_unknown_resolved,
        polls_waited: a.polls_waited + b.polls_waited,
        store: CounterSnapshot {
            resolve: add_phase(a.store.resolve, b.store.resolve),
            build: add_phase(a.store.build, b.store.build),
            store: add_phase(a.store.store, b.store.store),
            publish: add_phase(a.store.publish, b.store.publish),
        },
    }
}

fn add_phase(a: PhaseCounts, b: PhaseCounts) -> PhaseCounts {
    PhaseCounts {
        get: a.get + b.get,
        put: a.put + b.put,
        cas: a.cas + b.cas,
        list: a.list + b.list,
        conflict: a.conflict + b.conflict,
        bytes_in: a.bytes_in + b.bytes_in,
        bytes_out: a.bytes_out + b.bytes_out,
    }
}
