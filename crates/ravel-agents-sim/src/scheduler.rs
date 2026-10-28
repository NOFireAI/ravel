//! A deterministic step scheduler for workers and clients over one store.
//!
//! Each [`Actor`] runs as a tokio task on a `current_thread` runtime with
//! paused time, and talks to the shared store through its own
//! [`FaultStore`], whose hold gates stop every matching call before it
//! reaches the store. The scheduler lets the runtime settle until every live
//! actor is either finished or parked on a held call, then performs one
//! [`ScriptStep`]: release one actor's oldest held call, or crash an actor by
//! aborting its task while it holds a gate. With the default gates, which
//! hold every call, store calls therefore reach the store one at a time, in
//! the order the [`ScheduleScript`] names, and the script is derived from
//! the run's seed so a failure replays from the seed alone.
//!
//! Determinism rests on the runtime: a `current_thread` runtime polls tasks
//! in a fixed order and paused time advances only when every task is idle.
//! An actor that uses an unbiased `tokio::select!` draws from tokio's own
//! generator, which the seed does not control.
//!
//! Every [`ScheduleError`] and every failed [`sim_assert!`] or
//! [`sim_assert_eq!`] message carries the seed, a replay line and the
//! script's `Debug` form.
//!
//! [`sim_assert!`]: crate::sim_assert
//! [`sim_assert_eq!`]: crate::sim_assert_eq

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use rand::RngExt;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_sim::GateScript;
use tokio::task::JoinHandle;

use crate::seeds::AgentSeeds;

/// The store an actor sees: its own fault and gate layer over the shared
/// store.
pub type ActorStore = Arc<FaultStore<Arc<dyn ObjectStoreBackend>>>;

type Body = Box<dyn FnOnce(ActorStore) -> BoxFuture<'static, Result<(), String>> + Send>;

/// Whether an actor stands for a worker process or a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Worker,
    Client,
}

/// One simulated process. Its body returns `Err` for a failure the run
/// should report.
pub struct Actor {
    name: String,
    role: Role,
    gates: Vec<GateScript>,
    body: Body,
}

impl fmt::Debug for Actor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Actor")
            .field("name", &self.name)
            .field("role", &self.role)
            .field("gates", &self.gates)
            .finish_non_exhaustive()
    }
}

impl Actor {
    pub fn worker<F, Fut>(name: impl Into<String>, body: F) -> Actor
    where
        F: FnOnce(ActorStore) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        Actor::new(name, Role::Worker, body)
    }

    pub fn client<F, Fut>(name: impl Into<String>, body: F) -> Actor
    where
        F: FnOnce(ActorStore) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        Actor::new(name, Role::Client, body)
    }

    fn new<F, Fut>(name: impl Into<String>, role: Role, body: F) -> Actor
    where
        F: FnOnce(ActorStore) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        Actor {
            name: name.into(),
            role,
            gates: every_call(),
            body: Box::new(move |store| body(store).boxed()),
        }
    }

    /// Hold only the calls these gates match; the rest pass straight
    /// through. The default holds every call.
    pub fn with_gates(mut self, gates: Vec<GateScript>) -> Actor {
        self.gates = gates;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn role(&self) -> Role {
        self.role
    }
}

fn every_call() -> Vec<GateScript> {
    [Op::Put, Op::Get, Op::Head, Op::List, Op::Delete]
        .into_iter()
        .map(|op| GateScript {
            op,
            key_contains: None,
            occurrence: Occurrence::Always,
        })
        .collect()
}

/// One scheduling decision, naming an actor by its index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptStep {
    /// Release the oldest held call of this actor, or, when it holds none,
    /// of the next actor after it (cyclically) that does.
    Release(usize),
    /// Abort this actor's task while it holds a gate. Skipped when the actor
    /// holds no call at that step.
    Crash(usize),
}

/// The order in which held calls are released and actors crashed. Once the
/// script runs out, the lowest-indexed actor holding a call is released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleScript {
    pub steps: Vec<ScriptStep>,
}

impl ScheduleScript {
    pub fn new(steps: Vec<ScriptStep>) -> ScheduleScript {
        ScheduleScript { steps }
    }

    /// `len` steps over `actors` actors from the seed's schedule domain;
    /// each step is a crash with probability `crash_per_mille / 1000`.
    pub fn from_seed(
        seeds: &AgentSeeds,
        actors: usize,
        len: usize,
        crash_per_mille: u32,
    ) -> ScheduleScript {
        if actors == 0 {
            return ScheduleScript::new(Vec::new());
        }
        let mut rng = seeds.schedule_rng();
        let steps = (0..len)
            .map(|_| {
                let crash = rng.random_range(0..1000u32) < crash_per_mille;
                let actor = rng.random_range(0..actors);
                if crash {
                    ScriptStep::Crash(actor)
                } else {
                    ScriptStep::Release(actor)
                }
            })
            .collect();
        ScheduleScript::new(steps)
    }
}

/// What a scheduling step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Released,
    Crashed,
}

/// One performed step: which actor, and the held call it acted on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEntry {
    pub actor: usize,
    pub name: String,
    pub action: Action,
    pub op: Op,
    pub key: String,
}

/// How an actor's run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorEnd {
    Finished,
    Crashed,
}

/// A run that ended with every actor finished or crashed.
#[derive(Debug, Clone)]
pub struct ScheduleOutcome {
    pub seed: u64,
    pub script: ScheduleScript,
    pub trace: Vec<TraceEntry>,
    pub ends: Vec<ActorEnd>,
    /// One entry per settle that hit the bound: the actor that neither
    /// finished nor reached a gate, and the trace length at that point.
    /// Entries recorded in one settle round share that round's deadline:
    /// the round advanced paused time by one settle bound in total, however
    /// many of its actors timed out
    /// (`tests::timeouts_in_one_settle_round_share_one_deadline`). A lease
    /// or timer deadline may therefore have expired without a step asking
    /// for it.
    pub settle_timeouts: Vec<(String, usize)>,
}

impl ScheduleOutcome {
    /// `(actor, op)` of every performed step, in order.
    pub fn order(&self) -> Vec<(String, Action, Op)> {
        self.trace
            .iter()
            .map(|e| (e.name.clone(), e.action, e.op))
            .collect()
    }
}

/// A run that could not finish, with what is needed to replay it.
#[derive(Debug, Clone)]
pub struct ScheduleError {
    pub seed: u64,
    pub script: ScheduleScript,
    pub trace: Vec<TraceEntry>,
    pub reason: String,
}

impl fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\n{}\ntrace: {:?}",
            self.reason,
            failure_context(self.seed, &self.script),
            self.trace
        )
    }
}

impl std::error::Error for ScheduleError {}

/// The lines every failure message carries: the seed, how to replay it, and
/// the schedule.
pub fn failure_context(seed: u64, script: &ScheduleScript) -> String {
    format!(
        "seed: {seed}\nreplay: RAVEL_SIM_SEED={seed} cargo test -p ravel-agents-sim\nschedule: {script:?}"
    )
}

/// Something that can say which seed and schedule produced it.
pub trait FailureContext {
    fn failure_context(&self) -> String;
}

impl FailureContext for ScheduleOutcome {
    fn failure_context(&self) -> String {
        format!(
            "{}\ntrace: {:?}",
            failure_context(self.seed, &self.script),
            self.trace
        )
    }
}

impl FailureContext for ScheduleError {
    fn failure_context(&self) -> String {
        self.to_string()
    }
}

impl FailureContext for (u64, &ScheduleScript) {
    fn failure_context(&self) -> String {
        failure_context(self.0, self.1)
    }
}

/// `assert!` that appends the seed and schedule of `$ctx` (a
/// [`FailureContext`]) to the panic message.
#[macro_export]
macro_rules! sim_assert {
    ($ctx:expr, $cond:expr $(,)?) => {
        $crate::sim_assert!($ctx, $cond, "assertion failed: {}", stringify!($cond))
    };
    ($ctx:expr, $cond:expr, $($arg:tt)+) => {
        if !$cond {
            panic!(
                "{}\n{}",
                format_args!($($arg)+),
                $crate::scheduler::FailureContext::failure_context(&$ctx)
            );
        }
    };
}

/// `assert_eq!` that appends the seed and schedule of `$ctx` (a
/// [`FailureContext`]) to the panic message.
#[macro_export]
macro_rules! sim_assert_eq {
    ($ctx:expr, $left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left, right) => {
                if !(*left == *right) {
                    panic!(
                        "assertion `left == right` failed\n  left: {:?}\n right: {:?}\n{}",
                        left,
                        right,
                        $crate::scheduler::FailureContext::failure_context(&$ctx)
                    );
                }
            }
        }
    };
}

/// Runs actors under a script. See the module docs.
#[derive(Debug, Clone)]
pub struct Scheduler {
    seeds: AgentSeeds,
    script: ScheduleScript,
    settle_bound: Duration,
    max_steps: usize,
}

enum Phase {
    Live,
    Finished,
    Crashed,
}

struct Running {
    name: String,
    gate: GateHandle,
    join: JoinHandle<Result<(), String>>,
    phase: Phase,
}

impl Scheduler {
    pub fn new(seeds: AgentSeeds, script: ScheduleScript) -> Scheduler {
        Scheduler {
            seeds,
            script,
            settle_bound: Duration::from_secs(3600),
            max_steps: 10_000,
        }
    }

    /// Virtual time the runtime may idle while waiting for live actors to
    /// reach a gate. Paused time skips it instantly once every task idles.
    pub fn with_settle_bound(mut self, bound: Duration) -> Scheduler {
        self.settle_bound = bound;
        self
    }

    /// Steps after which the run fails rather than looping forever.
    pub fn with_max_steps(mut self, max_steps: usize) -> Scheduler {
        self.max_steps = max_steps;
        self
    }

    pub fn script(&self) -> &ScheduleScript {
        &self.script
    }

    /// Run `actors` over `store` until each finishes or crashes.
    pub fn run(
        &self,
        store: Arc<dyn ObjectStoreBackend>,
        actors: Vec<Actor>,
    ) -> Result<ScheduleOutcome, ScheduleError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .map_err(|e| self.error(Vec::new(), format!("could not build runtime: {e}")))?;
        runtime.block_on(self.drive(store, actors))
    }

    fn error(&self, trace: Vec<TraceEntry>, reason: String) -> ScheduleError {
        ScheduleError {
            seed: self.seeds.seed(),
            script: self.script.clone(),
            trace,
            reason,
        }
    }

    async fn drive(
        &self,
        store: Arc<dyn ObjectStoreBackend>,
        actors: Vec<Actor>,
    ) -> Result<ScheduleOutcome, ScheduleError> {
        let mut running: Vec<Running> = Vec::with_capacity(actors.len());
        for actor in actors {
            let fault: ActorStore =
                Arc::new(FaultStore::new(Arc::clone(&store), FaultPlan::empty()));
            let mut gate = None;
            for g in &actor.gates {
                gate = Some(fault.hold(g.op, g.key_contains.clone(), g.occurrence));
            }
            let gate = match gate {
                Some(gate) => gate,
                None => fault.hold(Op::Head, Some(String::from('\u{0}')), Occurrence::Nth(0)),
            };
            let join = tokio::spawn((actor.body)(fault));
            running.push(Running {
                name: actor.name,
                gate,
                join,
                phase: Phase::Live,
            });
        }

        let mut trace = Vec::new();
        let mut settle_timeouts = Vec::new();
        let mut steps = self.script.steps.iter();
        loop {
            self.settle(&mut running, &trace, &mut settle_timeouts)
                .await?;
            let ready: Vec<usize> = running
                .iter()
                .enumerate()
                .filter(|(_, r)| matches!(r.phase, Phase::Live) && r.gate.held_count() > 0)
                .map(|(i, _)| i)
                .collect();
            if running.iter().all(|r| !matches!(r.phase, Phase::Live)) {
                break;
            }
            if ready.is_empty() {
                let stuck: Vec<&str> = running
                    .iter()
                    .filter(|r| matches!(r.phase, Phase::Live))
                    .map(|r| r.name.as_str())
                    .collect();
                return Err(self.error(
                    trace,
                    format!(
                        "stuck: {stuck:?} neither finished nor reached a gate within {:?} of virtual time",
                        self.settle_bound
                    ),
                ));
            }
            if trace.len() >= self.max_steps {
                return Err(self.error(trace, format!("no end after {} steps", self.max_steps)));
            }
            let (index, action) = match steps.next() {
                Some(ScriptStep::Release(i)) => {
                    let n = running.len();
                    let pick = (0..n)
                        .map(|k| (i + k) % n)
                        .find(|j| ready.contains(j))
                        .unwrap_or(ready[0]);
                    (pick, Action::Released)
                }
                Some(ScriptStep::Crash(i)) => {
                    if !ready.contains(i) {
                        continue;
                    }
                    (*i, Action::Crashed)
                }
                None => (ready[0], Action::Released),
            };
            let actor = &mut running[index];
            let held = actor.gate.held_details();
            let Some((id, op, key)) = held.first().cloned() else {
                return Err(self.error(trace, format!("{} lost its held call", actor.name)));
            };
            if action == Action::Crashed {
                actor.join.abort();
                match (&mut actor.join).await {
                    Err(e) if e.is_cancelled() => actor.phase = Phase::Crashed,
                    Err(e) => {
                        return Err(self.error(trace, format!("{} panicked: {e}", actor.name)));
                    }
                    Ok(_) => {
                        return Err(self.error(
                            trace,
                            format!("{} finished while it held a gate", actor.name),
                        ));
                    }
                }
                for (held_id, _, _) in &held {
                    actor.gate.release(*held_id);
                }
            } else {
                actor.gate.release(id);
            }
            trace.push(TraceEntry {
                actor: index,
                name: actor.name.clone(),
                action,
                op,
                key,
            });
        }

        let ends = running
            .iter()
            .map(|r| match r.phase {
                Phase::Crashed => ActorEnd::Crashed,
                Phase::Live | Phase::Finished => ActorEnd::Finished,
            })
            .collect();
        Ok(ScheduleOutcome {
            seed: self.seeds.seed(),
            script: self.script.clone(),
            trace,
            ends,
            settle_timeouts,
        })
    }

    /// Let the runtime run until every live actor has finished or holds a
    /// call, or until the settle bound of idle virtual time passes. An actor
    /// still live at the bound is recorded in `settle_timeouts`
    /// (`tests::an_actor_that_never_reaches_a_gate_is_recorded_when_the_settle_bound_passes`).
    async fn settle(
        &self,
        running: &mut [Running],
        trace: &[TraceEntry],
        settle_timeouts: &mut Vec<(String, usize)>,
    ) -> Result<(), ScheduleError> {
        let deadline = tokio::time::Instant::now() + self.settle_bound;
        for actor in running.iter_mut() {
            if !matches!(actor.phase, Phase::Live) {
                continue;
            }
            let gate = actor.gate.clone();
            let settled = tokio::time::timeout_at(deadline, async {
                tokio::select! {
                    biased;
                    joined = &mut actor.join => Some(joined),
                    () = gate.wait_until_held(1) => None,
                }
            })
            .await;
            match settled {
                Ok(Some(Ok(Ok(())))) => actor.phase = Phase::Finished,
                Ok(Some(Ok(Err(reason)))) => {
                    return Err(
                        self.error(trace.to_vec(), format!("{} failed: {reason}", actor.name))
                    );
                }
                Ok(Some(Err(e))) => {
                    return Err(self.error(trace.to_vec(), format!("{} panicked: {e}", actor.name)));
                }
                Ok(None) => {}
                Err(_) => settle_timeouts.push((actor.name.clone(), trace.len())),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, PutMode, PutOptions, StoreError};
    use std::sync::Mutex;

    const HEAD: &str = "tenants/t/agents/a/head";

    /// Two workers each read the head and try to replace it with a CAS on
    /// the version they read; the log says who won.
    fn cas_race(log: Arc<Mutex<Vec<String>>>) -> Vec<Actor> {
        ["A", "B"]
            .into_iter()
            .map(|name| {
                let log = Arc::clone(&log);
                Actor::worker(name, move |store: ActorStore| async move {
                    let read = store
                        .get(HEAD, GetRange::Full)
                        .await
                        .map_err(|e| format!("get: {e}"))?;
                    let put = store
                        .put(
                            HEAD,
                            Bytes::from(name),
                            PutOptions {
                                mode: PutMode::CasVersion(read.version),
                                ..PutOptions::default()
                            },
                        )
                        .await;
                    let line = match put {
                        Ok(_) => format!("{name} won"),
                        Err(StoreError::PreconditionFailed) => format!("{name} lost"),
                        Err(e) => return Err(format!("put: {e}")),
                    };
                    log.lock().map_err(|e| e.to_string())?.push(line);
                    Ok(())
                })
            })
            .collect()
    }

    fn store_with_head() -> Arc<dyn ObjectStoreBackend> {
        let store = Arc::new(MemoryStore::new());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        runtime
            .block_on(store.put(
                HEAD,
                Bytes::from_static(b"0"),
                PutOptions::create_if_absent(),
            ))
            .expect("seed head");
        store
    }

    fn run(seed: u64, script: ScheduleScript) -> (ScheduleOutcome, Vec<String>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let outcome = Scheduler::new(AgentSeeds::new(seed), script.clone())
            .run(store_with_head(), cas_race(Arc::clone(&log)))
            .unwrap_or_else(|e| panic!("{e}"));
        let log = log.lock().expect("log").clone();
        (outcome, log)
    }

    #[test]
    fn the_script_releases_b_cas_before_a_cas() {
        use ScriptStep::Release;
        let script = ScheduleScript::new(vec![Release(0), Release(1), Release(1), Release(0)]);
        let (outcome, log) = run(1, script);
        sim_assert_eq!(
            outcome,
            outcome.order(),
            vec![
                ("A".to_string(), Action::Released, Op::Get),
                ("B".to_string(), Action::Released, Op::Get),
                ("B".to_string(), Action::Released, Op::Put),
                ("A".to_string(), Action::Released, Op::Put),
            ]
        );
        sim_assert_eq!(outcome, log, vec!["B won", "A lost"]);
        sim_assert_eq!(
            outcome,
            outcome.ends,
            vec![ActorEnd::Finished, ActorEnd::Finished]
        );

        // The opposite release order flips the winner.
        let script = ScheduleScript::new(vec![Release(0), Release(1), Release(0), Release(1)]);
        let (outcome, log) = run(1, script);
        sim_assert_eq!(outcome, log, vec!["A won", "B lost"]);
    }

    #[test]
    fn a_worker_crashed_at_its_gate_does_not_hang_the_runtime() {
        ravel_test_support::run_with_watchdog(
            Duration::from_secs(60),
            || "the scheduler hung after crashing worker A at its CAS gate".to_string(),
            || {
                use ScriptStep::{Crash, Release};
                let script =
                    ScheduleScript::new(vec![Release(0), Release(1), Crash(0), Release(1)]);
                let (outcome, log) = run(2, script);
                sim_assert_eq!(
                    outcome,
                    outcome.order(),
                    vec![
                        ("A".to_string(), Action::Released, Op::Get),
                        ("B".to_string(), Action::Released, Op::Get),
                        ("A".to_string(), Action::Crashed, Op::Put),
                        ("B".to_string(), Action::Released, Op::Put),
                    ]
                );
                sim_assert_eq!(
                    outcome,
                    outcome.ends,
                    vec![ActorEnd::Crashed, ActorEnd::Finished]
                );
                sim_assert_eq!(outcome, log, vec!["B won"]);
            },
        );
    }

    fn seeded(seed: u64) -> (ScheduleOutcome, Vec<String>) {
        run(
            seed,
            ScheduleScript::from_seed(&AgentSeeds::new(seed), 2, 8, 0),
        )
    }

    #[test]
    fn the_same_seed_replays_the_same_script_and_order() {
        let (first, first_log) = seeded(42);
        let (second, second_log) = seeded(42);
        sim_assert_eq!(
            first,
            format!("{:?}", first.script),
            format!("{:?}", second.script)
        );
        sim_assert_eq!(first, first.trace, second.trace);
        sim_assert_eq!(first, first_log, second_log);
    }

    #[test]
    fn a_different_seed_yields_a_different_order() {
        let (base, _) = seeded(0);
        let differs = (1..=20).any(|seed| seeded(seed).0.order() != base.order());
        sim_assert!(base, differs, "20 seeds all replayed seed 0's order");
    }

    #[test]
    fn a_failed_assertion_names_the_seed_and_the_script() {
        let (outcome, _) = seeded(42);
        let caught = std::panic::catch_unwind(|| {
            sim_assert_eq!(outcome, 1, 2);
        })
        .expect_err("the assertion fails");
        let message = caught
            .downcast_ref::<String>()
            .cloned()
            .expect("a formatted panic message");
        assert!(message.contains("seed: 42"), "{message}");
        assert!(message.contains("RAVEL_SIM_SEED=42"), "{message}");
        assert!(message.contains("schedule: ScheduleScript {"), "{message}");
        assert!(
            message.contains(&format!("{:?}", outcome.script)),
            "{message}"
        );

        let caught = std::panic::catch_unwind(|| {
            sim_assert!(outcome, outcome.trace.is_empty());
        })
        .expect_err("the assertion fails");
        let message = caught
            .downcast_ref::<String>()
            .cloned()
            .expect("a formatted panic message");
        assert!(
            message.contains("assertion failed: outcome.trace.is_empty()"),
            "{message}"
        );
        assert!(message.contains("RAVEL_SIM_SEED=42"), "{message}");
        assert!(message.contains("schedule: ScheduleScript {"), "{message}");
    }

    #[test]
    fn a_release_of_a_finished_actor_moves_on_and_a_crash_of_one_is_skipped() {
        use ScriptStep::{Crash, Release};
        let script = ScheduleScript::new(vec![Release(0), Release(0), Crash(0), Release(0)]);
        let (outcome, log) = run(4, script);
        // After A finishes, Crash(0) does nothing, Release(0) moves on to B,
        // and the exhausted script releases B's last call.
        sim_assert_eq!(
            outcome,
            outcome.order(),
            vec![
                ("A".to_string(), Action::Released, Op::Get),
                ("A".to_string(), Action::Released, Op::Put),
                ("B".to_string(), Action::Released, Op::Get),
                ("B".to_string(), Action::Released, Op::Put),
            ]
        );
        sim_assert_eq!(outcome, log, vec!["A won", "B won"]);
        sim_assert_eq!(
            outcome,
            outcome.ends,
            vec![ActorEnd::Finished, ActorEnd::Finished]
        );
    }

    #[test]
    fn calls_no_gate_matches_pass_through() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let actors = cas_race(Arc::clone(&log))
            .into_iter()
            .map(|actor| {
                actor.with_gates(vec![GateScript {
                    op: Op::Put,
                    key_contains: Some("/head".into()),
                    occurrence: Occurrence::Always,
                }])
            })
            .collect();
        let script = ScheduleScript::new(vec![ScriptStep::Release(1), ScriptStep::Release(0)]);
        let outcome = Scheduler::new(AgentSeeds::new(5), script)
            .run(store_with_head(), actors)
            .unwrap_or_else(|e| panic!("{e}"));
        // Both GETs ran ungated before either PUT was released.
        sim_assert_eq!(
            outcome,
            outcome.order(),
            vec![
                ("B".to_string(), Action::Released, Op::Put),
                ("A".to_string(), Action::Released, Op::Put),
            ]
        );
        sim_assert_eq!(
            outcome,
            log.lock().expect("log").clone(),
            vec!["B won", "A lost"]
        );
    }

    #[test]
    fn a_run_past_max_steps_fails() {
        let err = Scheduler::new(AgentSeeds::new(6), ScheduleScript::new(Vec::new()))
            .with_max_steps(5)
            .run(
                store_with_head(),
                vec![Actor::client("C", |store: ActorStore| async move {
                    loop {
                        store
                            .get(HEAD, GetRange::Full)
                            .await
                            .map_err(|e| format!("get: {e}"))?;
                    }
                })],
            )
            .expect_err("never ends");
        assert_eq!(err.reason, "no end after 5 steps", "{err}");
        assert_eq!(err.trace.len(), 5, "{err}");
    }

    #[test]
    fn a_failing_actor_reports_the_seed_and_the_script() {
        let script = ScheduleScript::from_seed(&AgentSeeds::new(9), 1, 4, 0);
        let err = Scheduler::new(AgentSeeds::new(9), script.clone())
            .run(
                store_with_head(),
                vec![Actor::client("C", |store: ActorStore| async move {
                    store
                        .get("missing", GetRange::Full)
                        .await
                        .map(|_| ())
                        .map_err(|e| format!("get: {e}"))
                })],
            )
            .expect_err("the client fails");
        let message = err.to_string();
        assert!(message.contains("C failed: get:"), "{message}");
        assert!(message.contains("seed: 9"), "{message}");
        assert!(
            message.contains("replay: RAVEL_SIM_SEED=9 cargo test -p ravel-agents-sim"),
            "{message}"
        );
        assert!(message.contains(&format!("{script:?}")), "{message}");
    }

    #[test]
    fn an_actor_that_never_reaches_a_gate_is_reported_stuck() {
        let err = Scheduler::new(AgentSeeds::new(3), ScheduleScript::new(Vec::new()))
            .run(
                store_with_head(),
                vec![Actor::client("C", |_store: ActorStore| async move {
                    futures::future::pending::<()>().await;
                    Ok(())
                })],
            )
            .expect_err("stuck");
        assert!(err.reason.starts_with("stuck: [\"C\"]"), "{err}");
    }

    #[test]
    fn an_actor_that_never_reaches_a_gate_is_recorded_when_the_settle_bound_passes() {
        // "P" parks on a timer the scheduler cannot see for longer than the
        // settle bound; "W" does one gated read. The first settle waits the
        // whole bound for P before W's call is released, and the outcome
        // says so; P then finishes inside the second settle.
        let outcome = Scheduler::new(AgentSeeds::new(3), ScheduleScript::new(Vec::new()))
            .with_settle_bound(Duration::from_secs(60))
            .run(
                store_with_head(),
                vec![
                    Actor::client("P", |_store: ActorStore| async move {
                        tokio::time::sleep(Duration::from_secs(100)).await;
                        Ok(())
                    }),
                    Actor::worker("W", |store: ActorStore| async move {
                        store
                            .get(HEAD, GetRange::Full)
                            .await
                            .map_err(|e| format!("get: {e}"))?;
                        Ok(())
                    }),
                ],
            )
            .unwrap_or_else(|e| panic!("{e}"));
        sim_assert_eq!(outcome, outcome.trace.len(), 1);
        sim_assert_eq!(outcome, outcome.settle_timeouts, vec![("P".to_string(), 0)]);
    }

    #[test]
    fn timeouts_in_one_settle_round_share_one_deadline() {
        // P1 and P2 both outlast the bound in the first settle round. W's
        // read is released only after that round, and it sees one bound of
        // virtual time gone, not two.
        let released_at = Arc::new(Mutex::new(None));
        let seen = Arc::clone(&released_at);
        let sleeper = |name: &str| {
            Actor::client(name, |_store: ActorStore| async move {
                tokio::time::sleep(Duration::from_secs(100)).await;
                Ok(())
            })
        };
        let outcome = Scheduler::new(AgentSeeds::new(3), ScheduleScript::new(Vec::new()))
            .with_settle_bound(Duration::from_secs(60))
            .run(
                store_with_head(),
                vec![
                    sleeper("P1"),
                    sleeper("P2"),
                    Actor::worker("W", move |store: ActorStore| async move {
                        let start = tokio::time::Instant::now();
                        store
                            .get(HEAD, GetRange::Full)
                            .await
                            .map_err(|e| format!("get: {e}"))?;
                        *seen.lock().map_err(|e| e.to_string())? = Some(start.elapsed());
                        Ok(())
                    }),
                ],
            )
            .unwrap_or_else(|e| panic!("{e}"));
        sim_assert_eq!(
            outcome,
            outcome.settle_timeouts,
            vec![("P1".to_string(), 0), ("P2".to_string(), 0)]
        );
        sim_assert_eq!(
            outcome,
            *released_at.lock().expect("lock"),
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn the_env_override_does_not_reach_the_seeds_the_multi_seed_tests_compare() {
        // The replay line sets RAVEL_SIM_SEED; a run built from an explicit
        // seed must ignore it, or the seed comparison tests compare one
        // seed with itself.
        let (a, _) = seeded(5);
        let (b, _) = seeded(6);
        sim_assert_eq!(a, a.seed, 5);
        sim_assert_eq!(b, b.seed, 6);
        sim_assert_eq!(a, AgentSeeds::new(5).seed(), 5);
    }
}
