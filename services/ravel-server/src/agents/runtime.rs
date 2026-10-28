//! The per-process agents runtime the routes act on: one
//! [`Coordinator`] per tenant over a tenant-scoped [`AgentStore`], built on
//! first use, plus the router's own admission permits.
//!
//! The coordinator is handed a [`WorkerDispatcher`]: the worker that asked
//! for a step runs the tool itself, so dispatching an attempt here executes
//! nothing, and the route answers with the attempt `run_step` returns.
//!
//! Each tenant built while a tokio runtime is current also gets an interval
//! sweep every `sweep_interval_ms`, which stops once the [`AgentsRuntime`]
//! or the tenant is dropped. It is the authority that fires due timers; the
//! `_wake` hints only make it cheap (ADR-3331 decision 8).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use ravel_agents_core::head::{EffectStatus, Head, ObjectRef};
use ravel_agents_core::identity::{AgentId, Id128, effect_id};
use ravel_agents_core::kernel::{DerivedIds, Limits};
use ravel_agents_runtime::{
    AgentsMetricsSnapshot, Coordinator, Dispatch, EffectDispatcher, RuntimeConfig, SweepConfig,
    SweepMetricsSnapshot, SystemRng,
};
use ravel_agents_store::counters::{CounterSnapshot, PhaseCounts};
use ravel_agents_store::{AgentStore, Clock, StoreLimits, SystemClock};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, watch};

use super::{AgentsReadiness, AgentsSettings};
use crate::config::AgentsClock;

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
    /// Late results reported through this process, by effect.
    pub evidence: EvidenceIndex,
}

/// Most effects whose late results one tenant's [`EvidenceIndex`] names,
/// unless `agents.max_evidence_effects` says otherwise.
pub const MAX_EVIDENCE_EFFECTS: usize = crate::config::DEFAULT_AGENTS_MAX_EVIDENCE_EFFECTS;
/// Most late results the index names for one effect.
pub const MAX_EVIDENCE_PER_EFFECT: usize = 16;

/// The least time between two poll-driven sweep passes when
/// `agents.sweep_interval_ms` is 0, so idle polls cannot sweep on every
/// wake.
pub const POLL_SWEEP_FLOOR_MS: u64 = ravel_agents_runtime::DEFAULT_SWEEP_INTERVAL_MS;

/// Where the late results of each effect were stored, for
/// `GET /api/v1/agents/effects/{effect}`. HEAD never references a late
/// result (ADR-3331 decision 7), so nothing durable lists one: the objects
/// themselves are in object storage, and this index is process-local and
/// bounded. A result reported through another process, or before a restart,
/// is not listed. An effect a HEAD read through [`EvidenceIndex::note`]
/// showed settled stays listed until the index is full, and then gives up
/// its slot to an effect the index does not name yet; once the index is
/// full of effects not known settled, results for new effects are not
/// listed.
#[derive(Debug)]
pub struct EvidenceIndex {
    max_effects: usize,
    entries: Mutex<HashMap<(AgentId, Id128), Evidence>>,
}

#[derive(Debug, Default)]
struct Evidence {
    results: Vec<LateResult>,
    /// No operator has to decide the effect any more: it left HEAD, has a
    /// result, or its input settled.
    settled: bool,
}

/// One late result: the attempt that reported it and where it is stored.
type LateResult = (u32, ObjectRef);

impl Default for EvidenceIndex {
    fn default() -> Self {
        EvidenceIndex::new(MAX_EVIDENCE_EFFECTS)
    }
}

impl EvidenceIndex {
    pub fn new(max_effects: usize) -> Self {
        EvidenceIndex {
            max_effects,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn record(&self, agent: AgentId, effect_id: Id128, attempt: u32, result: ObjectRef) {
        let mut entries = self.entries.lock();
        if !entries.contains_key(&(agent, effect_id)) && entries.len() >= self.max_effects {
            let settled = entries.iter().find(|(_, e)| e.settled).map(|(key, _)| *key);
            match settled {
                Some(key) => {
                    entries.remove(&key);
                }
                None => return,
            }
        }
        let kept = &mut entries.entry((agent, effect_id)).or_default().results;
        if kept.len() < MAX_EVIDENCE_PER_EFFECT && !kept.contains(&(attempt, result)) {
            kept.push((attempt, result));
        }
    }

    pub fn of(&self, agent: AgentId, effect_id: Id128) -> Vec<(u32, ObjectRef)> {
        self.entries
            .lock()
            .get(&(agent, effect_id))
            .map(|e| e.results.clone())
            .unwrap_or_default()
    }

    /// Mark every effect of `agent` the index names that `head` shows
    /// settled, so a full index can give its slot away.
    pub fn note(&self, agent: AgentId, head: &Head) {
        let mut entries = self.entries.lock();
        for ((a, id), evidence) in entries.iter_mut() {
            if *a == agent && !evidence.settled {
                evidence.settled = settled_in(head, *id);
            }
        }
    }

    /// Drop what the index names for an effect that is no longer in doubt,
    /// so its slot goes to an effect an operator still has to decide.
    pub fn forget(&self, agent: AgentId, effect_id: Id128) {
        self.entries.lock().remove(&(agent, effect_id));
    }

    /// Effects the index names.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Whether `head` shows `effect` past any operator decision: gone from
/// HEAD, completed, or journaled for an input that is neither waiting nor
/// the active turn's.
fn settled_in(head: &Head, effect: Id128) -> bool {
    let Some(record) = head.effects.iter().find(|e| e.effect_id == effect) else {
        return true;
    };
    if record.status == EffectStatus::Completed {
        return true;
    }
    let pending = head
        .inbox
        .iter()
        .map(|entry| entry.accept_seq)
        .chain(head.active_turn.as_ref().map(|turn| turn.input_accept_seq));
    !pending
        .into_iter()
        .any(|seq| effect_id(head.incarnation, seq, &record.step_key) == effect)
}

/// [`AgentsClock`] as the store's and coordinator's [`Clock`].
struct RuntimeClock(AgentsClock);

impl Clock for RuntimeClock {
    fn now_unix_ns(&self) -> i64 {
        match &self.0.0 {
            Some(now) => now(),
            None => SystemClock.now_unix_ns(),
        }
    }
}

/// The dispatcher hook the routes hand the coordinator. It executes nothing:
/// the worker that asked for the step runs the tool.
#[derive(Debug, Default)]
pub struct WorkerDispatcher;

impl EffectDispatcher for WorkerDispatcher {
    fn dispatch(&self, _dispatch: &Dispatch) {}
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
    pub result_waits: ResultWaitCounters,
    clock: AgentsClock,
    /// Dropped with the runtime, which ends every tenant's interval sweep.
    stop_sweeps: watch::Sender<()>,
}

/// What the `result` and `call` long polls in this process did while waiting
/// for an operation to finish.
#[derive(Debug, Default)]
pub struct ResultWaitCounters {
    /// Long polls that found the operation not yet terminal and parked.
    pub parked: AtomicU64,
    /// Parked long polls woken by a commit or cancel through this process.
    pub notified: AtomicU64,
    /// Long polls that reached their deadline with the operation not
    /// terminal.
    pub expired: AtomicU64,
}

impl ResultWaitCounters {
    /// `(parked, notified, expired)`.
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.parked.load(Ordering::Relaxed),
            self.notified.load(Ordering::Relaxed),
            self.expired.load(Ordering::Relaxed),
        )
    }
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
            result_waits: ResultWaitCounters::default(),
            clock: AgentsClock::default(),
            stop_sweeps: watch::channel(()).0,
        }
    }

    /// The same runtime reading `clock` instead of the host's wall clock.
    /// Applies to tenants built after the call.
    pub fn with_clock(mut self, clock: AgentsClock) -> Self {
        self.clock = clock;
        self
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
                let clock: Arc<dyn Clock> = Arc::new(RuntimeClock(self.clock.clone()));
                let store = Arc::new(AgentStore::new(
                    self.object_store.clone(),
                    tenant,
                    self.store_limits(),
                    clock.clone(),
                    Arc::new(DerivedIds),
                ));
                let coordinator = Coordinator::new(
                    store.clone(),
                    self.runtime_config(),
                    clock,
                    Arc::new(SystemRng::new()),
                )
                .with_sweep(SweepConfig {
                    interval_ms: self.poll_sweep_interval_ms(),
                    ..SweepConfig::default()
                });
                let agents = Arc::new(TenantAgents {
                    store,
                    coordinator,
                    changed: Notify::new(),
                    evidence: EvidenceIndex::new(self.settings.max_evidence_effects),
                });
                self.spawn_sweep(tenant, &agents);
                agents
            })
            .clone()
    }

    /// The least time between two poll-driven passes: the periodic interval,
    /// or [`POLL_SWEEP_FLOOR_MS`] when that is 0.
    pub fn poll_sweep_interval_ms(&self) -> u64 {
        match self.settings.sweep_interval_ms {
            0 => POLL_SWEEP_FLOOR_MS,
            interval_ms => interval_ms,
        }
    }

    /// Start the tenant's interval sweep, when the interval is not zero and
    /// a tokio runtime is current. The first pass runs one interval after
    /// the tenant is built; a pass that overruns delays the next one rather
    /// than bunching them.
    fn spawn_sweep(&self, tenant: TenantHash, agents: &Arc<TenantAgents>) {
        let interval_ms = self.settings.sweep_interval_ms;
        if interval_ms == 0 {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let agents: Weak<TenantAgents> = Arc::downgrade(agents);
        let mut stop = self.stop_sweeps.subscribe();
        let period = Duration::from_millis(interval_ms);
        handle.spawn(async move {
            let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticks.tick() => {}
                    _ = stop.changed() => return,
                }
                let Some(agents) = agents.upgrade() else {
                    return;
                };
                if let Err(error) = agents.coordinator.sweep_once(&tenant).await {
                    tracing::debug!(%error, "agents interval sweep did not complete");
                }
            }
        });
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

    /// Every tenant coordinator's sweep counters, summed.
    pub fn sweep_metrics(&self) -> SweepMetricsSnapshot {
        let coordinators: Vec<Arc<TenantAgents>> = self.tenants.lock().values().cloned().collect();
        coordinators
            .iter()
            .map(|t| t.coordinator.sweep_metrics())
            .fold(SweepMetricsSnapshot::default(), |a, b| {
                SweepMetricsSnapshot {
                    passes: a.passes + b.passes,
                    wake_list_pages: a.wake_list_pages + b.wake_list_pages,
                    dir_list_pages: a.dir_list_pages + b.dir_list_pages,
                    head_gets: a.head_gets + b.head_gets,
                    hints_read: a.hints_read + b.hints_read,
                    hints_deleted: a.hints_deleted + b.hints_deleted,
                    timers_fired: a.timers_fired + b.timers_fired,
                    agents_loaded: a.agents_loaded + b.agents_loaded,
                    dir_entries_repaired: a.dir_entries_repaired + b.dir_entries_repaired,
                }
            })
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

/// An active agent's HEAD with nothing in it, for unit tests over what HEAD
/// shows.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) fn empty_head() -> Head {
    use ravel_agents_core::head::{Lifecycle, Quota};
    use ravel_agents_core::keys::{AgentKey, AgentKind};
    Head {
        tenant: TenantHash([7; 16]),
        kind: AgentKind::new("order").expect("kind"),
        key: AgentKey::new("o-1").expect("key"),
        incarnation: 1,
        root_revision: 1,
        owner_id: String::new(),
        epoch: 0,
        owner_lease_hint_unix_ns: 0,
        state_version: 0,
        state: None,
        next_accept_seq: 1,
        active_turn: None,
        timer_generation: 0,
        next_event_seq: 1,
        lifecycle: Lifecycle::Active,
        quota: Quota::default(),
        inbox: Vec::new(),
        receipts: Vec::new(),
        results: Vec::new(),
        events: Vec::new(),
        timers: Vec::new(),
        steps: Vec::new(),
        effects: Vec::new(),
        outbox: Vec::new(),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// A full index takes a new effect only by dropping one a HEAD read
    /// showed settled; an effect not known settled keeps its slot.
    #[test]
    fn a_full_index_gives_only_a_settled_effects_slot_away() {
        let index = EvidenceIndex::new(2);
        let result = ObjectRef::of(b"late");
        index.record(agent(0), id(1), 1, result);
        index.record(agent(1), id(2), 1, result);
        index.record(agent(2), id(3), 1, result);
        assert!(index.of(agent(2), id(3)).is_empty(), "no settled slot");

        // Neither effect is in this HEAD any more; only agent 0's is noted.
        index.note(agent(0), &empty_head());
        index.record(agent(2), id(3), 1, result);
        assert_eq!(index.of(agent(2), id(3)), vec![(1, result)]);
        assert!(index.of(agent(0), id(1)).is_empty(), "the settled one went");
        assert_eq!(index.of(agent(1), id(2)), vec![(1, result)]);
        assert_eq!(index.len(), 2);

        index.record(agent(3), id(4), 1, result);
        assert!(index.of(agent(3), id(4)).is_empty(), "no settled slot left");
    }

    fn agent(n: u8) -> AgentId {
        AgentId([n; 16])
    }

    fn id(n: u128) -> Id128 {
        Id128(n.to_be_bytes())
    }

    /// A full index takes no new effect until one it names leaves doubt and
    /// is forgotten; then exactly one more fits.
    #[test]
    fn forgetting_an_effect_frees_its_slot_in_a_full_index() {
        let index = EvidenceIndex::default();
        let result = ObjectRef::of(b"late");
        for n in 0..MAX_EVIDENCE_EFFECTS {
            let id = id(n as u128 + 1);
            index.record(agent(0), id, 1, result);
        }
        assert_eq!(index.len(), MAX_EVIDENCE_EFFECTS);
        let late = id(u128::MAX);
        index.record(agent(1), late, 1, result);
        assert!(
            index.of(agent(1), late).is_empty(),
            "a full index took a new effect"
        );

        index.forget(agent(0), id(1));
        assert!(index.of(agent(0), id(1)).is_empty());
        assert_eq!(index.len(), MAX_EVIDENCE_EFFECTS - 1);
        index.record(agent(1), late, 1, result);
        assert_eq!(index.of(agent(1), late), vec![(1, result)]);
        assert_eq!(index.len(), MAX_EVIDENCE_EFFECTS);
        index.record(agent(1), id(u128::MAX - 1), 1, result);
        assert_eq!(index.len(), MAX_EVIDENCE_EFFECTS);
    }
}
