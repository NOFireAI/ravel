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
//!
//! When projection is on, each tenant's coordinator also carries a
//! [`Projector`] over a store instance of its own, and a task per tenant runs
//! its passes on every committed root the coordinator sees and on a retry
//! interval (ADR-3331 decision 12).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use ravel_agents_core::head::{EffectStatus, Head, ObjectRef};
use ravel_agents_core::identity::{AgentId, Id128, effect_id};
use ravel_agents_core::kernel::{DerivedIds, Limits};
use ravel_agents_runtime::projection::{
    PayloadPolicy, ProjectionLimits, ProjectionMetricsSnapshot, ProjectionSink, Projector,
};
use ravel_agents_runtime::{
    AgentsMetricsSnapshot, Coordinator, Dispatch, EffectDispatcher, RuntimeConfig, SweepConfig,
    SweepMetricsSnapshot, SystemRng,
};
use ravel_agents_store::counters::{CounterSnapshot, PhaseCounts};
use ravel_agents_store::{AgentStore, Clock, StoreLimits, SystemClock};
use ravel_ingest::{LogIngestRouter, SpanIngestRouter};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{TenantHash, TenantId};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, watch};

use super::projection::{InProcessSink, OtlpSink, TappedSink};
use super::{AgentsReadiness, AgentsSettings};
use crate::config::{AgentsClock, AgentsProjectionConfig, ProjectionTap};

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
/// listed. Every result not listed, for that reason or because its effect
/// already names [`MAX_EVIDENCE_PER_EFFECT`], counts in
/// [`EvidenceIndex::dropped`] and is logged at most once per
/// [`DROP_WARNING_INTERVAL_NS`] of the index's clock.
pub struct EvidenceIndex {
    tenant: TenantHash,
    max_effects: usize,
    clock: Arc<dyn Clock>,
    entries: Mutex<HashMap<(AgentId, Id128), Evidence>>,
    dropped: AtomicU64,
    /// When the last drop was logged, in the clock's nanoseconds; 0 before
    /// the first.
    last_drop_warning_unix_ns: AtomicI64,
}

/// The least time between two warnings about late results one tenant's
/// [`EvidenceIndex`] did not list.
pub const DROP_WARNING_INTERVAL_NS: i64 = 60 * 1_000_000_000;

impl std::fmt::Debug for EvidenceIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvidenceIndex")
            .field("tenant", &self.tenant)
            .field("max_effects", &self.max_effects)
            .field("entries", &self.entries)
            .field("dropped", &self.dropped)
            .finish_non_exhaustive()
    }
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

/// Why [`EvidenceIndex::record`] did not list a late result.
#[derive(Debug, Clone, Copy)]
enum Dropped {
    /// Every slot holds an effect not known settled.
    IndexFull,
    /// The effect already names [`MAX_EVIDENCE_PER_EFFECT`] results.
    EffectFull,
}

impl EvidenceIndex {
    /// The index of `tenant`, naming at most `max_effects` effects and
    /// rate-limiting its drop warnings on `clock`.
    pub fn new(tenant: TenantHash, max_effects: usize, clock: Arc<dyn Clock>) -> Self {
        EvidenceIndex {
            tenant,
            max_effects,
            clock,
            entries: Mutex::new(HashMap::new()),
            dropped: AtomicU64::new(0),
            last_drop_warning_unix_ns: AtomicI64::new(0),
        }
    }

    pub fn record(&self, agent: AgentId, effect_id: Id128, attempt: u32, result: ObjectRef) {
        let dropped = {
            let mut entries = self.entries.lock();
            let mut room = true;
            if !entries.contains_key(&(agent, effect_id)) && entries.len() >= self.max_effects {
                let settled = entries.iter().find(|(_, e)| e.settled).map(|(key, _)| *key);
                match settled {
                    Some(key) => {
                        entries.remove(&key);
                    }
                    None => room = false,
                }
            }
            if room {
                let kept = &mut entries.entry((agent, effect_id)).or_default().results;
                if kept.contains(&(attempt, result)) {
                    None
                } else if kept.len() < MAX_EVIDENCE_PER_EFFECT {
                    kept.push((attempt, result));
                    None
                } else {
                    Some(Dropped::EffectFull)
                }
            } else {
                Some(Dropped::IndexFull)
            }
        };
        if let Some(reason) = dropped {
            self.count_drop(effect_id, reason);
        }
    }

    fn count_drop(&self, effect_id: Id128, reason: Dropped) {
        let total = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        // 0 is the never-warned sentinel, so `now` is clamped above it.
        let now = self.clock.now_unix_ns().max(1);
        let last = self.last_drop_warning_unix_ns.load(Ordering::Relaxed);
        let due = last == 0 || now.saturating_sub(last) >= DROP_WARNING_INTERVAL_NS;
        if !due
            || self
                .last_drop_warning_unix_ns
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        match reason {
            Dropped::IndexFull => tracing::warn!(
                tenant = %self.tenant.to_hex(),
                effect = %effect_id.to_hex(),
                cap = self.max_effects,
                dropped = total,
                "agents evidence index is full of effects in doubt; a late effect result is stored but not listed"
            ),
            Dropped::EffectFull => tracing::warn!(
                tenant = %self.tenant.to_hex(),
                effect = %effect_id.to_hex(),
                cap = MAX_EVIDENCE_PER_EFFECT,
                dropped = total,
                "agents evidence index already lists the most late results one effect may name; a late effect result is stored but not listed"
            ),
        }
    }

    /// Late results this index did not list, since the process started.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
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
/// the active turn's, with every inbox page loaded.
fn settled_in(head: &Head, effect: Id128) -> bool {
    let Some(record) = head.effects.iter().find(|e| e.effect_id == effect) else {
        return true;
    };
    if record.status == EffectStatus::Completed {
        return true;
    }
    // A shallow HEAD does not show the inputs on sealed inbox pages, and one
    // of them may be waiting to retry this effect's step.
    if !head.index.inbox.unloaded().is_empty() {
        return false;
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

/// Where this process's projectors deliver, resolved at startup.
pub enum ProjectionSinkPlan {
    /// `--agents-projection off`, or a process without `--agents`.
    Off,
    /// Through this process's own ingest routers.
    InProcess {
        logs: Arc<LogIngestRouter>,
        spans: Arc<SpanIngestRouter>,
    },
    /// OTLP gRPC to a gateway.
    Endpoint(String),
}

impl ProjectionSinkPlan {
    /// The mode name the health route reports.
    pub fn mode(&self) -> &'static str {
        match self {
            ProjectionSinkPlan::Off => "off",
            ProjectionSinkPlan::InProcess { .. } => "in_process",
            ProjectionSinkPlan::Endpoint(_) => "endpoint",
        }
    }

    /// The endpoint, in `Endpoint` mode.
    pub fn endpoint(&self) -> Option<&str> {
        match self {
            ProjectionSinkPlan::Endpoint(endpoint) => Some(endpoint),
            _ => None,
        }
    }
}

/// The projection settings of one process.
pub struct ProjectionPlan {
    pub sink: ProjectionSinkPlan,
    pub backlog: usize,
    pub payload_tenants: Vec<String>,
    pub retry_ms: u64,
    pub tap: ProjectionTap,
}

impl ProjectionPlan {
    /// Projection off.
    pub fn off() -> ProjectionPlan {
        ProjectionPlan::new(ProjectionSinkPlan::Off, &AgentsProjectionConfig::default())
    }

    pub fn new(sink: ProjectionSinkPlan, config: &AgentsProjectionConfig) -> ProjectionPlan {
        ProjectionPlan {
            sink,
            backlog: config.backlog,
            payload_tenants: config.payload_tenants.clone(),
            retry_ms: config.retry_ms,
            tap: config.tap.clone(),
        }
    }

    fn payload_policy(&self, tenant: &TenantId) -> PayloadPolicy {
        if self.payload_tenants.iter().any(|t| t == tenant.as_str()) {
            PayloadPolicy::Carried
        } else {
            PayloadPolicy::Redacted
        }
    }
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
    projection: ProjectionPlan,
    /// Dropped with the runtime, which ends every tenant's interval sweep
    /// and projection task.
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
            projection: ProjectionPlan::off(),
            stop_sweeps: watch::channel(()).0,
        }
    }

    /// The same runtime projecting committed history per `plan`. Applies to
    /// tenants built after the call.
    pub fn with_projection(mut self, plan: ProjectionPlan) -> Self {
        self.projection = plan;
        self
    }

    pub fn projection_plan(&self) -> &ProjectionPlan {
        &self.projection
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
    pub fn tenant(&self, tenant_id: &TenantId) -> Arc<TenantAgents> {
        let tenant = tenant_id.hash();
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
                    clock.clone(),
                    Arc::new(SystemRng::new()),
                )
                .with_sweep(SweepConfig {
                    interval_ms: self.poll_sweep_interval_ms(),
                    ..SweepConfig::default()
                });
                let coordinator = match self.projector(tenant_id, &clock) {
                    Some(projector) => coordinator.with_projection(projector),
                    None => coordinator,
                };
                let agents = Arc::new(TenantAgents {
                    store,
                    coordinator,
                    changed: Notify::new(),
                    evidence: EvidenceIndex::new(tenant, self.settings.max_evidence_effects, clock),
                });
                self.spawn_sweep(tenant, &agents);
                self.spawn_projection(&agents);
                agents
            })
            .clone()
    }

    /// The tenant's projector, over a store instance of its own so its reads
    /// never count against the coordinator's; `None` when projection is off.
    fn projector(&self, tenant_id: &TenantId, clock: &Arc<dyn Clock>) -> Option<Arc<Projector>> {
        let inner: Arc<dyn ProjectionSink> = match &self.projection.sink {
            ProjectionSinkPlan::Off => return None,
            ProjectionSinkPlan::InProcess { logs, spans } => Arc::new(InProcessSink {
                tenant: tenant_id.clone(),
                logs: logs.clone(),
                spans: spans.clone(),
                clock: clock.clone(),
            }),
            ProjectionSinkPlan::Endpoint(endpoint) => {
                Arc::new(OtlpSink::new(tenant_id.clone(), endpoint.clone()))
            }
        };
        let sink: Arc<dyn ProjectionSink> = Arc::new(TappedSink {
            tap: self.projection.tap.clone(),
            inner,
        });
        let tenant = tenant_id.hash();
        let store = Arc::new(AgentStore::new(
            self.object_store.clone(),
            tenant,
            self.store_limits(),
            clock.clone(),
            Arc::new(DerivedIds),
        ));
        Some(Arc::new(Projector::new(
            tenant,
            store,
            sink,
            ProjectionLimits {
                backlog: self.projection.backlog,
                ..ProjectionLimits::default()
            },
            self.projection.payload_policy(tenant_id),
            clock.clone(),
        )))
    }

    /// Start the tenant's projection task, when it has a projector and a
    /// tokio runtime is current. A pass runs on every committed root the
    /// coordinator notes and every retry interval; a pass that stopped at a
    /// full backlog with the sink healthy runs again at once.
    fn spawn_projection(&self, agents: &Arc<TenantAgents>) {
        let Some(projector) = agents.coordinator.projection() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let projector: Weak<Projector> = Arc::downgrade(projector);
        let mut stop = self.stop_sweeps.subscribe();
        let retry = Duration::from_millis(self.projection.retry_ms.max(1));
        handle.spawn(async move {
            loop {
                let Some(projector) = projector.upgrade() else {
                    return;
                };
                let pass = projector.project_once().await;
                if pass.backlog_full && !pass.sink_failed {
                    continue;
                }
                tokio::select! {
                    _ = projector.notified() => {}
                    _ = tokio::time::sleep(retry) => {}
                    _ = stop.changed() => return,
                }
            }
        });
    }

    /// Every tenant projector's counters, merged: lag is the largest.
    pub fn projection_metrics(&self) -> ProjectionMetricsSnapshot {
        self.metrics().projection
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

    /// Late results the evidence index of every tenant did not list, summed.
    pub fn evidence_dropped(&self) -> u64 {
        let tenants: Vec<Arc<TenantAgents>> = self.tenants.lock().values().cloned().collect();
        tenants.iter().map(|t| t.evidence.dropped()).sum()
    }

    /// Every tenant coordinator's sweep counters, summed, except
    /// `last_directory_cycle_ms`, which is the largest of them.
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
                    directory_cycles: a.directory_cycles + b.directory_cycles,
                    last_directory_cycle_ms: a
                        .last_directory_cycle_ms
                        .max(b.last_directory_cycle_ms),
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
        projection: ProjectionMetricsSnapshot::default(),
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
            planner_misses: a.store.planner_misses + b.store.planner_misses,
        },
        projection: a.projection.merge(b.projection),
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
        index: ravel_agents_core::pages::Index::default(),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// A full index takes a new effect only by dropping one a HEAD read
    /// showed settled; an effect not known settled keeps its slot.
    fn index(max_effects: usize) -> EvidenceIndex {
        EvidenceIndex::new(TenantHash([7; 16]), max_effects, Arc::new(SystemClock))
    }

    #[test]
    fn a_full_index_gives_only_a_settled_effects_slot_away() {
        let index = index(2);
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

    /// An effect of an input waiting to retry on a sealed inbox page stays
    /// in doubt under a shallow HEAD, so a full index keeps its slot.
    #[test]
    fn an_effect_of_a_retrying_input_on_a_sealed_page_keeps_its_slot() {
        use ravel_agents_core::head::{EffectRecord, InboxEntry, InputSource, RecoveryPolicy};
        use ravel_agents_core::identity::Hash256;
        use ravel_agents_core::pages::{PageBounds, seal_index};

        let mut head = empty_head();
        for seq in 1..=3 {
            head.inbox.push(InboxEntry {
                accept_seq: seq,
                handler: "place".to_string(),
                source: InputSource::Message {
                    request_id: format!("r-{seq}"),
                    request_hash: Hash256([0; 32]),
                },
                payload: None,
                accepted_unix_ns: 0,
                failed_attempts: u32::from(seq == 1),
                last_failed_turn_id: (seq == 1).then_some(id(7)),
            });
        }
        head.next_accept_seq = 4;
        let effect = effect_id(head.incarnation, 1, "charge");
        head.effects.push(EffectRecord {
            effect_id: effect,
            step_key: "charge".to_string(),
            tool_name: "pay".to_string(),
            tool_version: "1".to_string(),
            input_hash: Hash256([0; 32]),
            policy: RecoveryPolicy::Manual,
            status: EffectStatus::InDoubt,
            attempt: 1,
            result: None,
            result_attempt: 0,
            turn_id: id(7),
        });
        seal_index(
            &mut head,
            PageBounds {
                max_page_entries: 2,
                max_index_pages: 512,
            },
        )
        .expect("seal");
        let shallow = Head::decode(&head.encode()).expect("decode");
        assert!(
            shallow.inbox.iter().all(|e| e.accept_seq != 1),
            "the retrying input is on a sealed page"
        );
        assert_eq!(shallow.index.inbox.unloaded(), vec![0]);

        let index = index(1);
        let result = ObjectRef::of(b"late");
        index.record(agent(0), effect, 1, result);
        index.note(agent(0), &shallow);
        index.record(agent(1), id(9), 1, result);
        assert_eq!(index.of(agent(0), effect), vec![(1, result)]);
        assert!(index.of(agent(1), id(9)).is_empty(), "no slot given away");
    }

    /// A full index takes no new effect until one it names leaves doubt and
    /// is forgotten; then exactly one more fits.
    #[test]
    fn forgetting_an_effect_frees_its_slot_in_a_full_index() {
        let index = index(MAX_EVIDENCE_EFFECTS);
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

    /// A result the index does not list counts once, whether the index is
    /// full of effects in doubt or the effect already names
    /// [`MAX_EVIDENCE_PER_EFFECT`] results; a listed result and a repeat of
    /// one count nothing.
    #[test]
    fn every_late_result_the_index_cannot_list_is_counted() {
        let index = index(1);
        let result = |n: u32| ObjectRef::of(&n.to_be_bytes());
        index.record(agent(0), id(1), 1, result(1));
        index.record(agent(0), id(1), 1, result(1));
        assert_eq!(index.dropped(), 0);

        index.record(agent(1), id(2), 1, result(1));
        assert!(index.of(agent(1), id(2)).is_empty());
        assert_eq!(index.dropped(), 1, "full of an effect in doubt");

        let per_effect = u32::try_from(MAX_EVIDENCE_PER_EFFECT).expect("small");
        for attempt in 2..=per_effect {
            index.record(agent(0), id(1), attempt, result(attempt));
        }
        assert_eq!(index.of(agent(0), id(1)).len(), MAX_EVIDENCE_PER_EFFECT);
        assert_eq!(index.dropped(), 1);
        index.record(agent(0), id(1), per_effect + 1, result(per_effect + 1));
        assert_eq!(index.of(agent(0), id(1)).len(), MAX_EVIDENCE_PER_EFFECT);
        assert_eq!(index.dropped(), 2, "the effect names its most results");
    }
}
