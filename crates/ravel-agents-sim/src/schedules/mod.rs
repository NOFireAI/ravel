//! Named schedules: the production store and runtime driven through the
//! scheduler and checked against the reference machine.

#![allow(clippy::expect_used)]

mod errors;
mod races;
mod senders;
mod workers;

use std::future::Future;
use std::sync::{Arc, OnceLock};

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op};
use ravel_sim::GateScript;
use serde_json::{Value, json};

use crate::drive::{Proc, Rig, Run};
use crate::scheduler::{
    Actor, ActorStore, Role, ScheduleError, ScheduleOutcome, ScheduleScript, Scheduler,
    ScriptStep,
};
use crate::seeds::AgentSeeds;

/// Seeds every batch runs, `BASE..BASE + BATCH`.
pub(crate) const BATCH: u64 = 64;
pub(crate) const BASE: u64 = 0x5eed_0000;

pub(crate) fn seeds() -> impl Iterator<Item = u64> {
    BASE..BASE + BATCH
}

/// A client actor whose body runs against its own process.
pub(crate) fn client<F, Fut>(rig: &Rig, name: &str, id_seed: u64, body: F) -> Actor
where
    F: FnOnce(Proc) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    let rig = rig.clone();
    let owned = name.to_string();
    Actor::client(name, move |store| body(rig.proc(&owned, store, id_seed)))
}

/// A worker actor whose body runs against its own process.
pub(crate) fn worker<F, Fut>(rig: &Rig, name: &str, id_seed: u64, body: F) -> Actor
where
    F: FnOnce(Proc) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    let rig = rig.clone();
    let owned = name.to_string();
    Actor::worker(name, move |store| body(rig.proc(&owned, store, id_seed)))
}

/// The fault layer an actor built from [`faulty`] put under its process.
pub(crate) type Faults = Arc<OnceLock<Arc<FaultStore<Arc<dyn ObjectStoreBackend>>>>>;

/// An actor whose process sees `plan` applied to its own calls only, under
/// its scheduler gates. The returned cell holds that layer once the actor
/// starts, so the schedule can count the faults that fired.
pub(crate) fn faulty<F, Fut>(
    rig: &Rig,
    role: Role,
    name: &str,
    id_seed: u64,
    plan: FaultPlan,
    body: F,
) -> (Actor, Faults)
where
    F: FnOnce(Proc) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    let rig = rig.clone();
    let owned = name.to_string();
    let faults: Faults = Arc::default();
    let cell = Arc::clone(&faults);
    let body = move |store: ActorStore| {
        let layer = Arc::new(FaultStore::new(store as Arc<dyn ObjectStoreBackend>, plan));
        let _ = cell.set(Arc::clone(&layer));
        body(rig.proc(&owned, layer, id_seed))
    };
    let actor = match role {
        Role::Client => Actor::client(name, body),
        Role::Worker => Actor::worker(name, body),
    };
    (actor, faults)
}

/// Every fault an actor's layer injected, by op and kind.
pub(crate) fn all_fired(faults: &Faults) -> Vec<((Op, FaultKind), u64)> {
    let mut all: Vec<_> = faults
        .get()
        .map(|f| f.counters_snapshot().into_iter().filter(|(_, n)| *n > 0).collect())
        .unwrap_or_default();
    all.sort_by_key(|((op, kind), _)| (format!("{op:?}"), format!("{kind:?}")));
    all
}

pub(crate) fn run(
    rig: &Rig,
    seed: u64,
    script: ScheduleScript,
    actors: Vec<Actor>,
) -> Result<ScheduleOutcome, ScheduleError> {
    Scheduler::new(AgentSeeds::new(seed), script).run(rig.shared(), actors)
}

/// A script from `seed` over `actors` actors.
pub(crate) fn seeded(seed: u64, actors: usize, len: usize, crash_per_mille: u32) -> ScheduleScript {
    ScheduleScript::from_seed(&AgentSeeds::new(seed), actors, len, crash_per_mille)
}

/// The same script with every crash of an actor in `clients` turned into a
/// release: only workers die.
pub(crate) fn crash_workers_only(script: ScheduleScript, clients: &[usize]) -> ScheduleScript {
    ScheduleScript::new(
        script
            .steps
            .into_iter()
            .map(|step| match step {
                ScriptStep::Crash(i) if clients.contains(&i) => ScriptStep::Release(i),
                other => other,
            })
            .collect(),
    )
}

/// A gate holding the `n`-th call of `op` whose key contains `key`.
pub(crate) fn gate(op: Op, key: &str, n: u64) -> GateScript {
    GateScript {
        op,
        key_contains: Some(key.to_string()),
        occurrence: Occurrence::Nth(n),
    }
}

/// Create the agent outside any schedule.
pub(crate) fn create(rig: &Rig) {
    rig.setup(|p| async move {
        p.create().await.map_err(|e| format!("create: {e:?}"))?;
        Ok(())
    })
    .expect("setup create");
}

/// Create the agent and send `r1..=rn` outside any schedule.
pub(crate) fn create_with_inputs(rig: &Rig, n: u64) {
    rig.setup(|p| async move {
        p.create().await.map_err(|e| format!("create: {e:?}"))?;
        for i in 1..=n {
            p.send(&format!("r{i}"), &payload(i))
                .await
                .map_err(|e| format!("send r{i}: {e:?}"))?;
        }
        Ok(())
    })
    .expect("setup inputs");
}

/// Name of the replacement worker [`drain`] runs.
pub(crate) const DRAIN: &str = "drain";

/// Put every lease past its grace, then run a fresh worker on its own
/// coordinator until it finds no work.
pub(crate) fn drain(rig: &Rig, seed: u64) -> ScheduleOutcome {
    rig.clock.advance_ms(crate::drive::PAST_LEASE_MS);
    let w = worker(rig, DRAIN, 99, |p| async move {
        p.work(16).await;
        Ok(())
    });
    run(rig, seed, ScheduleScript::new(Vec::new()), vec![w]).expect("drain")
}

pub(crate) fn payload(n: u64) -> Value {
    json!({ "n": n })
}

/// Build the run and check it: nothing unmappable, and the history passes
/// the reference machine.
pub(crate) fn checked(rig: &Rig, ctx: &dyn crate::scheduler::FailureContext) -> Run {
    let run = rig.build().expect("build history");
    if !run.unmappable.is_empty() {
        panic!(
            "unmappable: {:?}\n{}",
            run.unmappable,
            ctx.failure_context()
        );
    }
    if let Err(divergence) = run.check() {
        panic!(
            "history diverged: {divergence}\nhistory: {:?}\n{}",
            run.history,
            ctx.failure_context()
        );
    }
    run
}
